package io.github.riccardomerolla.zio.eclipsestore.schema

import zio.*
import zio.schema.Schema
import zio.schema.codec.JsonCodec
import zio.stream.ZStream

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

/** Type-safe, schema-aware persistence operations on top of EclipseStoreService. */
trait TypedStore:
  /** Stores a typed value under a typed key after schema round-trip validation. */
  def store[K: Schema, V: Schema](key: K, value: V): IO[EclipseStoreError, Unit]

  /** Fetches a typed value by key after schema validation of the key. */
  def fetch[K: Schema, V: Schema](key: K): IO[EclipseStoreError, Option[V]]

  /** Removes a value by typed key after schema validation of the key. */
  def remove[K: Schema](key: K): IO[EclipseStoreError, Unit]

  /** Reads all values currently available in the key-value root. */
  def fetchAll[V: Schema]: IO[EclipseStoreError, List[V]]

  /** Streams all values currently available in the key-value root. */
  def streamAll[V: Schema]: ZStream[Any, EclipseStoreError, V]

  /** Accesses a typed root descriptor, enabling schema-driven root registration. */
  def typedRoot[A: Schema](descriptor: RootDescriptor[A]): IO[EclipseStoreError, A]

  /** Persists the provided value after schema round-trip validation. */
  def storePersist[A: Schema](value: A): IO[EclipseStoreError, Unit]

/** Default `TypedStore` implementation backed by `EclipseStoreService`. */
final case class TypedStoreLive(underlying: EclipseStoreService) extends TypedStore:
  private def isKnownOptionInstantEncodingBug(error: Throwable): Boolean =
    error match
      case _: NoSuchElementException => Option(error.getMessage).contains("None.get")
      case _                         => false

  private def validate[A: Schema](value: A, label: String): IO[EclipseStoreError, Unit] =
    val codec = JsonCodec.jsonCodec(summon[Schema[A]])
    ZIO
      .attempt {
        val json = codec.encodeJson(value, None).toString
        codec.decodeJson(json)
      }
      .catchSome {
        case e if isKnownOptionInstantEncodingBug(e) =>
          ZIO.logDebug(
            s"Skipping TypedStore JSON validation for $label due to known zio-json Option[Instant] bug: ${e.getMessage}"
          ) *> ZIO.succeed(Right(value))
      }
      .mapError(e => EclipseStoreError.QueryError(s"Schema validation failed for $label", Some(e)))
      .flatMap {
        case Right(_)    => ZIO.unit
        case Left(error) => ZIO.fail(EclipseStoreError.QueryError(s"Schema validation failed for $label: $error", None))
      }

  override def store[K: Schema, V: Schema](key: K, value: V): IO[EclipseStoreError, Unit] =
    validate(key, "key") *> validate(value, "value") *> underlying.put(key, value)

  override def fetch[K: Schema, V: Schema](key: K): IO[EclipseStoreError, Option[V]] =
    validate(key, "key") *> underlying.get[K, V](key)

  override def remove[K: Schema](key: K): IO[EclipseStoreError, Unit] =
    validate(key, "key") *> underlying.delete(key)

  override def fetchAll[V: Schema]: IO[EclipseStoreError, List[V]] =
    underlying.getAll[V]

  override def streamAll[V: Schema]: ZStream[Any, EclipseStoreError, V] =
    underlying.streamValues[V]

  override def typedRoot[A: Schema](descriptor: RootDescriptor[A]): IO[EclipseStoreError, A] =
    underlying.root(descriptor)

  override def storePersist[A: Schema](value: A): IO[EclipseStoreError, Unit] =
    validate(value, "value") *> underlying.persist(value)

object TypedStore:
  import scala.reflect.ClassTag

  import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig

  /** Layer constructor for `TypedStore`.
    *
    * @deprecated
    *   Prefer composing with [[handlersFor]] to ensure all stored types have a registered `BinaryTypeHandler` before
    *   the EclipseStore storage manager starts. Without handler registration, values containing Scala 3 enums or sealed
    *   traits may be serialised correctly on the first run but fail to deserialise correctly after a JVM restart.
    *
    * Migration:
    * {{{
    *   // Before
    *   EclipseStoreService.live >>> TypedStore.live
    *
    *   // After
    *   ZLayer.succeed(config) >>>
    *     TypedStore.handlersFor[String, MyValue] >>>
    *     EclipseStoreService.live >>>
    *     TypedStore.live
    * }}}
    */
  @deprecated(
    "Use TypedStore.handlersFor[K, V] >>> EclipseStoreService.live >>> TypedStore.live to ensure correct binary handler registration before store startup.",
    since = "0.x",
  )
  val live: ZLayer[EclipseStoreService, Nothing, TypedStore] =
    ZLayer.fromFunction(TypedStoreLive.apply)

  /** Config-enriching layer that derives and pre-registers `BinaryTypeHandler`s for key type `K` and value type `V`
    * before `EclipseStoreService.live` starts.
    *
    * Chain this layer between your `EclipseStoreConfig` provider and `EclipseStoreService.live`:
    *
    * {{{
    * val layer =
    *   ZLayer.succeed(EclipseStoreConfig.make(storagePath)) >>>
    *   TypedStore.handlersFor[String, AgentIssue] >>>
    *   EclipseStoreService.live >>>
    *   TypedStore.live
    * }}}
    *
    * For multiple value types, chain `handlersFor` calls:
    *
    * {{{
    * val layer =
    *   ZLayer.succeed(config) >>>
    *   TypedStore.handlersFor[String, AgentIssue] >>>
    *   TypedStore.handlersFor[String, ChatConversation] >>>
    *   EclipseStoreService.live >>>
    *   TypedStore.live
    * }}}
    *
    * EclipseStore deduplicates handlers by runtime class at startup, so overlapping handlers from multiple
    * `handlersFor` calls are harmless.
    */
  def handlersFor[K: Schema: ClassTag, V: Schema: ClassTag]
    : ZLayer[EclipseStoreConfig, Nothing, EclipseStoreConfig] =
    ZLayer.fromFunction { (config: EclipseStoreConfig) =>
      val keyHandlers   = SchemaBinaryCodec.handlers(Schema[K])
      val valueHandlers = SchemaBinaryCodec.handlers(Schema[V])
      config.copy(customTypeHandlers = config.customTypeHandlers ++ keyHandlers ++ valueHandlers)
    }

  def store[K: Schema, V: Schema](key: K, value: V): ZIO[TypedStore, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[TypedStore](_.store(key, value))

  def fetch[K: Schema, V: Schema](key: K): ZIO[TypedStore, EclipseStoreError, Option[V]] =
    ZIO.serviceWithZIO[TypedStore](_.fetch(key))

  def remove[K: Schema](key: K): ZIO[TypedStore, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[TypedStore](_.remove(key))

  def fetchAll[V: Schema]: ZIO[TypedStore, EclipseStoreError, List[V]] =
    ZIO.serviceWithZIO[TypedStore](_.fetchAll[V])

  def streamAll[V: Schema]: ZStream[TypedStore, EclipseStoreError, V] =
    ZStream.serviceWithStream[TypedStore](_.streamAll[V])

  def typedRoot[A: Schema](descriptor: RootDescriptor[A]): ZIO[TypedStore, EclipseStoreError, A] =
    ZIO.serviceWithZIO[TypedStore](_.typedRoot(descriptor))

  def storePersist[A: Schema](value: A): ZIO[TypedStore, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[TypedStore](_.storePersist(value))
