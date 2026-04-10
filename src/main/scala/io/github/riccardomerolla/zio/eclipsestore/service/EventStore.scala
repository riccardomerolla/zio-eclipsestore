package io.github.riccardomerolla.zio.eclipsestore.service

import java.time.Instant

import zio.*
import zio.schema.{ Schema, derived }

import io.github.riccardomerolla.zio.eclipsestore.error.EventStoreError

final case class StreamId(value: String) derives Schema

final case class StreamRevision(value: Long) derives Schema:
  def next: StreamRevision =
    StreamRevision(value + 1L)

object StreamRevision:
  val Zero: StreamRevision    = StreamRevision(0L)
  val Initial: StreamRevision = StreamRevision(1L)

enum ExpectedVersion derives Schema:
  case Any
  case NoStream
  case Exact(revision: StreamRevision)

final case class EventEnvelope[E](
  streamId: StreamId,
  revision: StreamRevision,
  recordedAt: Instant,
  payload: E,
  metadata: Chunk[(String, String)] = Chunk.empty,
) derives Schema

final case class AppendResult[E](
  envelopes: Chunk[EventEnvelope[E]],
  currentRevision: StreamRevision,
) derives Schema

trait EventStore[E]:
  def readStream(streamId: StreamId, from: Option[StreamRevision] = None): IO[EventStoreError, Chunk[EventEnvelope[E]]]
  def append(
    streamId: StreamId,
    expectedVersion: ExpectedVersion,
    events: Chunk[E],
    metadata: Chunk[(String, String)] = Chunk.empty,
  ): IO[EventStoreError, AppendResult[E]]
  def currentRevision(streamId: StreamId): IO[EventStoreError, Option[StreamRevision]]
  def streams: IO[EventStoreError, Chunk[StreamId]]

object EventStore:
  def readStream[E: Tag](
    streamId: StreamId,
    from: Option[StreamRevision] = None,
  ): ZIO[EventStore[E], EventStoreError, Chunk[EventEnvelope[E]]] =
    ZIO.serviceWithZIO[EventStore[E]](_.readStream(streamId, from))

  def append[E: Tag](
    streamId: StreamId,
    expectedVersion: ExpectedVersion,
    events: Chunk[E],
    metadata: Chunk[(String, String)] = Chunk.empty,
  ): ZIO[EventStore[E], EventStoreError, AppendResult[E]] =
    ZIO.serviceWithZIO[EventStore[E]](_.append(streamId, expectedVersion, events, metadata))

  def currentRevision[E: Tag](streamId: StreamId): ZIO[EventStore[E], EventStoreError, Option[StreamRevision]] =
    ZIO.serviceWithZIO[EventStore[E]](_.currentRevision(streamId))

  def streams[E: Tag]: ZIO[EventStore[E], EventStoreError, Chunk[StreamId]] =
    ZIO.serviceWithZIO[EventStore[E]](_.streams)
