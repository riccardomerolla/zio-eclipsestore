package io.github.riccardomerolla.zio.eclipsestore.service

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

/** Additive CRUD helpers for immutable `Map`-backed roots.
  *
  * These helpers fit the NativeLocal whole-root update model and are intended for small local-first aggregates. They do
  * not replace `TypedStore` or domain-specific repositories.
  */
trait LocalRepo[Key, Value]:
  def get(key: Key): IO[EclipseStoreError, Option[Value]]
  def upsert(key: Key, value: Value): IO[EclipseStoreError, Value]
  def delete(key: Key): IO[EclipseStoreError, Option[Value]]
  def list: IO[EclipseStoreError, Chunk[Value]]
  def entries: IO[EclipseStoreError, Map[Key, Value]]

final case class LocalRepoLive[Root, Key, Value](
  store: ObjectStore[Root],
  extract: Root => Map[Key, Value],
  replace: (Root, Map[Key, Value]) => Root,
) extends LocalRepo[Key, Value]:
  override def get(key: Key): IO[EclipseStoreError, Option[Value]] =
    store.load.map(root => extract(root).get(key))

  override def upsert(key: Key, value: Value): IO[EclipseStoreError, Value] =
    store.modify { root =>
      val next = extract(root).updated(key, value)
      ZIO.succeed((value, replace(root, next)))
    }

  override def delete(key: Key): IO[EclipseStoreError, Option[Value]] =
    store.modify { root =>
      val current = extract(root)
      val removed = current.get(key)
      ZIO.succeed((removed, replace(root, current - key)))
    }

  override def list: IO[EclipseStoreError, Chunk[Value]] =
    store.load.map(root => Chunk.fromIterable(extract(root).values))

  override def entries: IO[EclipseStoreError, Map[Key, Value]] =
    store.load.map(extract)

object LocalRepo:
  def get[Key: Tag, Value: Tag](key: Key): ZIO[LocalRepo[Key, Value], EclipseStoreError, Option[Value]] =
    ZIO.serviceWithZIO[LocalRepo[Key, Value]](_.get(key))

  def upsert[Key: Tag, Value: Tag](key: Key, value: Value): ZIO[LocalRepo[Key, Value], EclipseStoreError, Value] =
    ZIO.serviceWithZIO[LocalRepo[Key, Value]](_.upsert(key, value))

  def delete[Key: Tag, Value: Tag](key: Key): ZIO[LocalRepo[Key, Value], EclipseStoreError, Option[Value]] =
    ZIO.serviceWithZIO[LocalRepo[Key, Value]](_.delete(key))

  def list[Key: Tag, Value: Tag]: ZIO[LocalRepo[Key, Value], EclipseStoreError, Chunk[Value]] =
    ZIO.serviceWithZIO[LocalRepo[Key, Value]](_.list)

  def entries[Key: Tag, Value: Tag]: ZIO[LocalRepo[Key, Value], EclipseStoreError, Map[Key, Value]] =
    ZIO.serviceWithZIO[LocalRepo[Key, Value]](_.entries)

  def fromMap[Root: Tag, Key: Tag, Value: Tag](
    extract: Root => Map[Key, Value]
  )(
    replace: (Root, Map[Key, Value]) => Root
  ): ZLayer[ObjectStore[Root], Nothing, LocalRepo[Key, Value]] =
    ZLayer.fromFunction { (store: ObjectStore[Root]) =>
      LocalRepoLive(store, extract, replace)
    }
