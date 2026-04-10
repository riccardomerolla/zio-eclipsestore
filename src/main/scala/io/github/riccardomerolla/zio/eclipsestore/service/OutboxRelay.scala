package io.github.riccardomerolla.zio.eclipsestore.service

import java.time.Instant

import zio.*
import zio.schema.{ Schema, derived }

import io.github.riccardomerolla.zio.eclipsestore.error.OutboxError

final case class OutboxMessage(
  id: String,
  streamId: StreamId,
  revision: StreamRevision,
  recordedAt: Instant,
  topic: String,
  key: String,
  headers: Chunk[(String, String)] = Chunk.empty,
  body: Chunk[Byte],
) derives Schema

trait OutboxProjector[E]:
  def project(envelope: EventEnvelope[E]): Chunk[OutboxMessage]

trait EventPublisher:
  def publish(message: OutboxMessage): IO[OutboxError, Unit]

final case class RelayStreamCheckpoint(
  streamId: StreamId,
  revision: StreamRevision,
) derives Schema

final case class RelayCheckpoint(
  relayId: String,
  updatedAt: Instant,
  streams: Chunk[RelayStreamCheckpoint],
) derives Schema:
  def revisionFor(streamId: StreamId): Option[StreamRevision] =
    streams.find(_.streamId == streamId).map(_.revision)

trait OutboxRelay[E]:
  def publishPending(limit: Int = Int.MaxValue): IO[OutboxError, Chunk[OutboxMessage]]
  def checkpoint: IO[OutboxError, RelayCheckpoint]

object OutboxRelay:
  def publishPending[E: Tag](limit: Int = Int.MaxValue): ZIO[OutboxRelay[E], OutboxError, Chunk[OutboxMessage]] =
    ZIO.serviceWithZIO[OutboxRelay[E]](_.publishPending(limit))

  def checkpoint[E: Tag]: ZIO[OutboxRelay[E], OutboxError, RelayCheckpoint] =
    ZIO.serviceWithZIO[OutboxRelay[E]](_.checkpoint)
