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
  private def validate[A: Schema](value: A, label: String): IO[EclipseStoreError, Unit] =
    val codec = JsonCodec.jsonCodec(summon[Schema[A]])
    val json  = codec.encodeJson(value, None).toString
    ZIO
      .fromEither(codec.decodeJson(json))
      .as(())
      .mapError(error => EclipseStoreError.QueryError(s"Schema validation failed for $label: $error", None))

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
  /** Layer constructor for `TypedStore`. */
  val live: ZLayer[EclipseStoreService, Nothing, TypedStore] =
    ZLayer.fromFunction(TypedStoreLive.apply)

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
