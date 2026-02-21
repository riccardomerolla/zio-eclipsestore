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
  def store[K: Schema, V: Schema](key: K, value: V): IO[EclipseStoreError, Unit]
  def fetch[K: Schema, V: Schema](key: K): IO[EclipseStoreError, Option[V]]
  def remove[K: Schema](key: K): IO[EclipseStoreError, Unit]
  def fetchAll[V: Schema]: IO[EclipseStoreError, List[V]]
  def streamAll[V: Schema]: ZStream[Any, EclipseStoreError, V]
  def typedRoot[A: Schema](descriptor: RootDescriptor[A]): IO[EclipseStoreError, A]
  def storePersist[A: Schema](value: A): IO[EclipseStoreError, Unit]

final case class TypedStoreLive(underlying: EclipseStoreService) extends TypedStore:
  private def validate[A: Schema](value: A, label: String): IO[EclipseStoreError, Unit] =
    ZIO
      .attempt {
        val codec = JsonCodec.jsonCodec(summon[Schema[A]])
        val json  = codec.encodeJson(value, None).toString
        codec.decodeJson(json) match
          case Left(error) => throw new IllegalArgumentException(error)
          case Right(_)    => ()
      }
      .mapError(e => EclipseStoreError.QueryError(s"Schema validation failed for $label", Some(e)))

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
