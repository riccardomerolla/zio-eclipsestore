package io.github.riccardomerolla.zio.eclipsestore.service

import java.time.Instant

import zio.*
import zio.schema.{ Schema, derived }

import io.github.riccardomerolla.zio.eclipsestore.error.SnapshotStoreError

final case class SnapshotEnvelope[S](
  streamId: StreamId,
  revision: StreamRevision,
  recordedAt: Instant,
  state: S,
) derives Schema

trait SnapshotStore[S]:
  def loadLatest(streamId: StreamId): IO[SnapshotStoreError, Option[SnapshotEnvelope[S]]]
  def save(snapshot: SnapshotEnvelope[S]): IO[SnapshotStoreError, SnapshotEnvelope[S]]
  def delete(streamId: StreamId): IO[SnapshotStoreError, Unit]

object SnapshotStore:
  def loadLatest[S: Tag](streamId: StreamId): ZIO[SnapshotStore[S], SnapshotStoreError, Option[SnapshotEnvelope[S]]] =
    ZIO.serviceWithZIO[SnapshotStore[S]](_.loadLatest(streamId))

  def save[S: Tag](snapshot: SnapshotEnvelope[S]): ZIO[SnapshotStore[S], SnapshotStoreError, SnapshotEnvelope[S]] =
    ZIO.serviceWithZIO[SnapshotStore[S]](_.save(snapshot))

  def delete[S: Tag](streamId: StreamId): ZIO[SnapshotStore[S], SnapshotStoreError, Unit] =
    ZIO.serviceWithZIO[SnapshotStore[S]](_.delete(streamId))
