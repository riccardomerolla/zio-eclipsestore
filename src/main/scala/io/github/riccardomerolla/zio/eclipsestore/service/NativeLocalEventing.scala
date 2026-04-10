package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Path }
import java.time.Instant
import java.util.Base64

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.{ Schema, derived }

import io.github.riccardomerolla.zio.eclipsestore.config.{ NativeLocalEventingConfig, NativeLocalSerde }
import io.github.riccardomerolla.zio.eclipsestore.error.{
  EclipseStoreError,
  EventStoreError,
  OutboxError,
  SnapshotStoreError,
}

final private case class EventJournalFile[E](
  streamId: StreamId,
  events: Chunk[EventEnvelope[E]],
) derives Schema

final private case class NativeLocalEventStoreLive[E: Schema](
  config: NativeLocalEventingConfig,
  gate: Semaphore,
) extends EventStore[E]:
  override def readStream(
    streamId: StreamId,
    from: Option[StreamRevision] = None,
  ): IO[EventStoreError, Chunk[EventEnvelope[E]]] =
    gate.withPermit {
      loadJournal(streamId).map { journal =>
        from match
          case Some(revision) => journal.events.filter(_.revision.value >= revision.value)
          case None           => journal.events
      }
    }

  override def append(
    streamId: StreamId,
    expectedVersion: ExpectedVersion,
    events: Chunk[E],
    metadata: Chunk[(String, String)] = Chunk.empty,
  ): IO[EventStoreError, AppendResult[E]] =
    gate.withPermit {
      for
        journal        <- loadJournal(streamId)
        actualRevision  = journal.events.lastOption.map(_.revision)
        _              <- verifyExpectedVersion(streamId, expectedVersion, actualRevision)
        recordedAt     <- Clock.instant
        appended        = events.zipWithIndex.map {
                            case (event, index) =>
                              EventEnvelope(
                                streamId = streamId,
                                revision =
                                  StreamRevision(actualRevision.map(_.value + index.toLong + 1L).getOrElse(index.toLong + 1L)),
                                recordedAt = recordedAt,
                                payload = event,
                                metadata = metadata,
                              )
                          }
        updated         = journal.copy(events = journal.events ++ appended)
        _              <- saveJournal(updated)
        currentRevision = appended.lastOption.map(_.revision).orElse(actualRevision).getOrElse(StreamRevision.Zero)
      yield AppendResult(appended, currentRevision)
    }

  override def currentRevision(streamId: StreamId): IO[EventStoreError, Option[StreamRevision]] =
    gate.withPermit(loadJournal(streamId).map(_.events.lastOption.map(_.revision)))

  override def streams: IO[EventStoreError, Chunk[StreamId]] =
    gate.withPermit {
      for
        files <- listFiles(NativeLocalEventingPaths.streamsDir(config))
        ids   <- ZIO.foreach(files)(path => loadJournalFrom(path).map(_.streamId))
      yield ids
    }

  private def verifyExpectedVersion(
    streamId: StreamId,
    expected: ExpectedVersion,
    actual: Option[StreamRevision],
  ): IO[EventStoreError, Unit] =
    expected match
      case ExpectedVersion.Any        => ZIO.unit
      case ExpectedVersion.NoStream   =>
        if actual.isEmpty then ZIO.unit
        else ZIO.fail(EventStoreError.WrongExpectedVersion(streamId, expected, actual))
      case ExpectedVersion.Exact(rev) =>
        if actual.contains(rev) then ZIO.unit
        else ZIO.fail(EventStoreError.WrongExpectedVersion(streamId, expected, actual))

  private def loadJournal(streamId: StreamId): IO[EventStoreError, EventJournalFile[E]] =
    exists(NativeLocalEventingPaths.streamPath(config, streamId)).flatMap {
      case false => ZIO.succeed(EventJournalFile(streamId, Chunk.empty))
      case true  =>
        loadJournalFrom(NativeLocalEventingPaths.streamPath(config, streamId), Some(streamId)).flatMap { journal =>
          if journal.streamId == streamId then ZIO.succeed(journal)
          else
            ZIO.fail(
              EventStoreError.CorruptStream(
                streamId,
                s"Journal at ${NativeLocalEventingPaths.streamPath(config, streamId)} belongs to ${journal.streamId.value}",
              )
            )
        }
    }

  private def loadJournalFrom(path: Path, expectedStreamId: Option[StreamId] = None)
    : IO[EventStoreError, EventJournalFile[E]] =
    val errorStreamId = expectedStreamId.getOrElse(StreamId(path.getFileName.toString))
    SnapshotCodec
      .loadEnvelopedOrElse(
        path,
        NativeLocalEventingPaths.EventJournalRootId,
        config.serde,
        EventJournalFile(StreamId(path.getFileName.toString), Chunk.empty),
      )
      .flatMap { loaded =>
        ZIO.when(loaded.rewriteRequired)(saveJournalTo(path, loaded.value)).as(loaded.value)
      }
      .catchAll {
        case EclipseStoreError.IncompatibleSchemaError(message, cause) =>
          ZIO.fail(EventStoreError.CorruptStream(errorStreamId, message, cause))
        case other                                                     =>
          ZIO.fail(EventStoreError.StorageError(other.toString, None))
      }

  private def saveJournal(journal: EventJournalFile[E]): IO[EventStoreError, Unit] =
    saveJournalTo(NativeLocalEventingPaths.streamPath(config, journal.streamId), journal)

  private def saveJournalTo(path: Path, journal: EventJournalFile[E]): IO[EventStoreError, Unit] =
    SnapshotCodec
      .saveEnveloped(path, journal, NativeLocalEventingPaths.EventJournalRootId, config.serde)
      .mapError(error => EventStoreError.StorageError(error.toString, None))

  private def listFiles(dir: Path): IO[EventStoreError, Chunk[Path]] =
    ZIO
      .attemptBlocking {
        if Files.exists(dir) then
          val stream = Files.list(dir)
          try Chunk.fromIterable(stream.iterator().asScala.filter(Files.isRegularFile(_)).toList)
          finally stream.close()
        else Chunk.empty
      }
      .mapError(cause =>
        EventStoreError.StorageError(s"Failed to list NativeLocal event streams under $dir", Some(cause))
      )

  private def exists(path: Path): IO[EventStoreError, Boolean] =
    ZIO
      .attemptBlocking(Files.exists(path))
      .mapError(cause =>
        EventStoreError.StorageError(s"Failed to inspect NativeLocal event stream path $path", Some(cause))
      )

final private case class NativeLocalSnapshotStoreLive[S: Schema](
  config: NativeLocalEventingConfig,
  gate: Semaphore,
) extends SnapshotStore[S]:
  override def loadLatest(streamId: StreamId): IO[SnapshotStoreError, Option[SnapshotEnvelope[S]]] =
    gate.withPermit {
      val path = NativeLocalEventingPaths.snapshotPath(config, streamId)
      exists(path).flatMap {
        case false => ZIO.succeed(None)
        case true  => loadSnapshotFrom(path, streamId)
      }
    }

  override def save(snapshot: SnapshotEnvelope[S]): IO[SnapshotStoreError, SnapshotEnvelope[S]] =
    gate.withPermit(saveTo(NativeLocalEventingPaths.snapshotPath(config, snapshot.streamId), snapshot).as(snapshot))

  override def delete(streamId: StreamId): IO[SnapshotStoreError, Unit] =
    gate.withPermit {
      ZIO
        .attemptBlocking(Files.deleteIfExists(NativeLocalEventingPaths.snapshotPath(config, streamId)))
        .unit
        .mapError(cause =>
          SnapshotStoreError.StorageError(s"Failed to delete NativeLocal snapshot for ${streamId.value}", Some(cause))
        )
    }

  private def exists(path: Path): IO[SnapshotStoreError, Boolean] =
    ZIO
      .attemptBlocking(Files.exists(path))
      .mapError(cause =>
        SnapshotStoreError.StorageError(s"Failed to inspect NativeLocal snapshot path $path", Some(cause))
      )

  private def loadSnapshotFrom(path: Path, streamId: StreamId): IO[SnapshotStoreError, Option[SnapshotEnvelope[S]]] =
    ZIO
      .attemptBlocking(Chunk.fromArray(Files.readAllBytes(path)))
      .mapError(cause =>
        SnapshotStoreError.StorageError(s"Failed to read NativeLocal snapshot from $path", Some(cause))
      )
      .flatMap { bytes =>
        SnapshotCodec.decodePayload[NativeLocalSnapshotEnvelope](bytes, config.serde).either.flatMap {
          case Right(envelope) =>
            if envelope.rootId != NativeLocalEventingPaths.SnapshotRootId then
              ZIO.fail(
                SnapshotStoreError.CorruptSnapshot(
                  streamId,
                  s"Snapshot at $path belongs to root ${envelope.rootId}",
                )
              )
            else
              SnapshotCodec.decodePayload[SnapshotEnvelope[S]](envelope.payload, config.serde).map(snapshot =>
                (snapshot, false)
              )
          case Left(_)         =>
            SnapshotCodec.decodePayload[SnapshotEnvelope[S]](bytes, config.serde).map(snapshot => (snapshot, true))
        }
      }
      .flatMap { (snapshot, rewriteRequired) =>
        ZIO.when(rewriteRequired)(saveTo(path, snapshot)).as(Some(snapshot))
      }
      .catchAll {
        case EclipseStoreError.IncompatibleSchemaError(message, cause) =>
          ZIO.fail(SnapshotStoreError.CorruptSnapshot(streamId, message, cause))
        case other                                                     =>
          ZIO.fail(SnapshotStoreError.StorageError(other.toString, None))
      }

  private def saveTo(path: Path, snapshot: SnapshotEnvelope[S]): IO[SnapshotStoreError, Unit] =
    SnapshotCodec
      .saveEnveloped(path, snapshot, NativeLocalEventingPaths.SnapshotRootId, config.serde)
      .mapError(error => SnapshotStoreError.StorageError(error.toString, None))

final private case class NativeLocalOutboxRelayLive[E: Schema](
  relayId: String,
  config: NativeLocalEventingConfig,
  eventStore: EventStore[E],
  projector: OutboxProjector[E],
  publisher: EventPublisher,
  gate: Semaphore,
) extends OutboxRelay[E]:
  override def publishPending(limit: Int = Int.MaxValue): IO[OutboxError, Chunk[OutboxMessage]] =
    gate.withPermit {
      for
        checkpoint <- loadCheckpoint
        streams    <- eventStore.streams.mapError(error => OutboxError.StorageError(error.toString, None))
        envelopes  <- ZIO.foreach(streams) { streamId =>
                        eventStore
                          .readStream(streamId, checkpoint.revisionFor(streamId).map(_.next))
                          .mapError(error => OutboxError.StorageError(error.toString, None))
                      }
        flattened   = envelopes.flatten.sortBy(envelope =>
                        (envelope.recordedAt.toEpochMilli, envelope.streamId.value, envelope.revision.value)
                      ).take(limit)
        result     <- ZIO.foldLeft(flattened)((checkpoint, Chunk.empty[OutboxMessage])) {
                        case ((currentCheckpoint, published), envelope) =>
                          val messages = projector.project(envelope)
                          ZIO.foreachDiscard(messages)(publisher.publish) *>
                            checkpointForEnvelope(currentCheckpoint, envelope).map { updatedCheckpoint =>
                              (updatedCheckpoint, published ++ messages)
                            }
                      }
      yield result._2
    }

  override def checkpoint: IO[OutboxError, RelayCheckpoint] =
    gate.withPermit(loadCheckpoint)

  private def checkpointForEnvelope(
    checkpoint: RelayCheckpoint,
    envelope: EventEnvelope[E],
  ): IO[OutboxError, RelayCheckpoint] =
    for
      now <- Clock.instant
      next = checkpoint.copy(
               updatedAt = now,
               streams = checkpoint.streams.filterNot(_.streamId == envelope.streamId) :+ RelayStreamCheckpoint(
                 envelope.streamId,
                 envelope.revision,
               ),
             )
      _   <- saveCheckpoint(next)
    yield next

  private def loadCheckpoint: IO[OutboxError, RelayCheckpoint] =
    exists(NativeLocalEventingPaths.relayPath(config, relayId)).flatMap {
      case false =>
        ZIO.succeed(RelayCheckpoint(relayId, Instant.EPOCH, Chunk.empty))
      case true  =>
        SnapshotCodec
          .loadEnvelopedOrElse(
            NativeLocalEventingPaths.relayPath(config, relayId),
            NativeLocalEventingPaths.RelayRootId,
            config.serde,
            RelayCheckpoint(relayId, Instant.EPOCH, Chunk.empty),
          )
          .flatMap { loaded =>
            ZIO.when(loaded.rewriteRequired)(saveCheckpoint(loaded.value)).as(loaded.value)
          }
          .catchAll(error => ZIO.fail(OutboxError.StorageError(error.toString, None)))
    }

  private def saveCheckpoint(checkpoint: RelayCheckpoint): IO[OutboxError, Unit] =
    SnapshotCodec
      .saveEnveloped(
        NativeLocalEventingPaths.relayPath(config, relayId),
        checkpoint,
        NativeLocalEventingPaths.RelayRootId,
        config.serde,
      )
      .mapError(error => OutboxError.StorageError(error.toString, None))

  private def exists(path: Path): IO[OutboxError, Boolean] =
    ZIO
      .attemptBlocking(Files.exists(path))
      .mapError(cause => OutboxError.StorageError(s"Failed to inspect relay checkpoint path $path", Some(cause)))

object NativeLocalEventStore:
  def live[E: Tag: Schema](config: NativeLocalEventingConfig): ZLayer[Any, Nothing, EventStore[E]] =
    ZLayer.fromZIO(Semaphore.make(1).map(NativeLocalEventStoreLive(config, _)))

object NativeLocalSnapshotStore:
  def live[S: Tag: Schema](config: NativeLocalEventingConfig): ZLayer[Any, Nothing, SnapshotStore[S]] =
    ZLayer.fromZIO(Semaphore.make(1).map(NativeLocalSnapshotStoreLive(config, _)))

object NativeLocalOutboxRelay:
  def live[E: Tag: Schema](
    relayId: String,
    config: NativeLocalEventingConfig,
    projector: OutboxProjector[E],
  ): ZLayer[EventStore[E] & EventPublisher, Nothing, OutboxRelay[E]] =
    ZLayer.fromZIO {
      for
        eventStore <- ZIO.service[EventStore[E]]
        publisher  <- ZIO.service[EventPublisher]
        gate       <- Semaphore.make(1)
      yield NativeLocalOutboxRelayLive(relayId, config, eventStore, projector, publisher, gate)
    }

private object NativeLocalEventingPaths:
  val EventJournalRootId: String = "native-local-event-journal"
  val SnapshotRootId: String     = "native-local-event-snapshot"
  val RelayRootId: String        = "native-local-event-relay"

  def streamsDir(config: NativeLocalEventingConfig): Path =
    config.baseDir.resolve("streams")

  def snapshotsDir(config: NativeLocalEventingConfig): Path =
    config.baseDir.resolve("snapshots")

  def relaysDir(config: NativeLocalEventingConfig): Path =
    config.baseDir.resolve("relays")

  def streamPath(config: NativeLocalEventingConfig, streamId: StreamId): Path =
    streamsDir(config).resolve(encoded(streamId.value) + extension(config.serde))

  def snapshotPath(config: NativeLocalEventingConfig, streamId: StreamId): Path =
    snapshotsDir(config).resolve(encoded(streamId.value) + extension(config.serde))

  def relayPath(config: NativeLocalEventingConfig, relayId: String): Path =
    relaysDir(config).resolve(encoded(relayId) + extension(config.serde))

  private def encoded(value: String): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8))

  private def extension(serde: NativeLocalSerde): String =
    serde match
      case NativeLocalSerde.Json     => ".json"
      case NativeLocalSerde.Protobuf => ".pb"
