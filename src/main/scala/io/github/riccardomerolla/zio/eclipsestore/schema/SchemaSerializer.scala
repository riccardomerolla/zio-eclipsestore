package io.github.riccardomerolla.zio.eclipsestore.schema

import scala.reflect.ClassTag

import zio.*
import zio.schema.Schema
import zio.schema.codec.JsonCodec

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

/** Public Scala-native serialization boundary for schema-driven persistence. */
trait SchemaSerializer:
  def encode[A: Schema](value: A): IO[EclipseStoreError, String]
  def decode[A: Schema](payload: String): IO[EclipseStoreError, A]
  def typeHandler[A: Schema: ClassTag]: IO[EclipseStoreError, TypeHandler[A]]
  def handlersFor[A: Schema: ClassTag]: IO[EclipseStoreError, Chunk[TypeHandler[?]]]
  def register[A: Schema: ClassTag](config: EclipseStoreConfig): IO[EclipseStoreError, EclipseStoreConfig]

final case class SchemaSerializerLive() extends SchemaSerializer:
  override def encode[A: Schema](value: A): IO[EclipseStoreError, String] =
    ZIO
      .attempt(JsonCodec.jsonCodec(summon[Schema[A]]).encodeJson(value, None).toString)
      .mapError(cause => EclipseStoreError.QueryError("Failed to encode schema value", Some(cause)))

  override def decode[A: Schema](payload: String): IO[EclipseStoreError, A] =
    ZIO
      .attempt(JsonCodec.jsonCodec(summon[Schema[A]]).decodeJson(payload))
      .mapError(cause => EclipseStoreError.QueryError("Failed to decode schema payload", Some(cause)))
      .flatMap {
        case Right(value) => ZIO.succeed(value)
        case Left(error)  => ZIO.fail(EclipseStoreError.QueryError(s"Failed to decode schema payload: $error", None))
      }

  override def typeHandler[A: Schema: ClassTag]: IO[EclipseStoreError, TypeHandler[A]] =
    ZIO
      .attempt(TypeHandler.fromBinary(SchemaBinaryCodec.handler(Schema[A])))
      .mapError(cause => EclipseStoreError.InitializationError("Failed to derive schema type handler", Some(cause)))

  override def handlersFor[A: Schema: ClassTag]: IO[EclipseStoreError, Chunk[TypeHandler[?]]] =
    ZIO
      .attempt(SchemaBinaryCodec.handlers(Schema[A]).map(TypeHandler.fromUntyped))
      .mapError(cause => EclipseStoreError.InitializationError("Failed to derive schema handlers", Some(cause)))

  override def register[A: Schema: ClassTag](config: EclipseStoreConfig): IO[EclipseStoreError, EclipseStoreConfig] =
    ZIO.attempt(SchemaSerializer.registerInConfig[A](config)).mapError(cause =>
      EclipseStoreError.InitializationError("Failed to register schema handlers", Some(cause))
    )

object SchemaSerializer:
  import org.eclipse.serializer.persistence.binary.types.Binary
  import org.eclipse.serializer.persistence.types.PersistenceTypeHandler

  val live: ULayer[SchemaSerializer] =
    ZLayer.succeed(SchemaSerializerLive())

  private[schema] def registerInConfig[A: Schema: ClassTag](config: EclipseStoreConfig): EclipseStoreConfig =
    val handlers = SchemaBinaryCodec.handlers(Schema[A]).map(TypeHandler.fromUntyped)
    val merged   = config.customTypeHandlers ++ handlers.map(_.persistenceHandler)
    config.copy(customTypeHandlers = dedupeByRuntimeClass(merged))

  private def dedupeByRuntimeClass(
    handlers: Chunk[PersistenceTypeHandler[Binary, ?]]
  ): Chunk[PersistenceTypeHandler[Binary, ?]] =
    handlers.foldLeft((Chunk.empty[PersistenceTypeHandler[Binary, ?]], Set.empty[Class[?]])) {
      case ((acc, seen), handler) =>
        val runtimeClass = handler.`type`()
        if seen.contains(runtimeClass) then (acc, seen)
        else (acc :+ handler, seen + runtimeClass)
    }._1

  def encode[A: Schema](value: A): ZIO[SchemaSerializer, EclipseStoreError, String] =
    ZIO.serviceWithZIO[SchemaSerializer](_.encode(value))

  def decode[A: Schema](payload: String): ZIO[SchemaSerializer, EclipseStoreError, A] =
    ZIO.serviceWithZIO[SchemaSerializer](_.decode(payload))

  def typeHandler[A: Schema: ClassTag]: ZIO[SchemaSerializer, EclipseStoreError, TypeHandler[A]] =
    ZIO.serviceWithZIO[SchemaSerializer](_.typeHandler[A])

  def handlersFor[A: Schema: ClassTag]: ZIO[SchemaSerializer, EclipseStoreError, Chunk[TypeHandler[?]]] =
    ZIO.serviceWithZIO[SchemaSerializer](_.handlersFor[A])

  def register[A: Schema: ClassTag](config: EclipseStoreConfig)
    : ZIO[SchemaSerializer, EclipseStoreError, EclipseStoreConfig] =
    ZIO.serviceWithZIO[SchemaSerializer](_.register[A](config))
