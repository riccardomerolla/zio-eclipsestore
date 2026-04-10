package io.github.riccardomerolla.zio.eclipsestore

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Path }
import java.time.Instant
import java.util.Base64

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ NativeLocalEventingConfig, NativeLocalSerde }
import io.github.riccardomerolla.zio.eclipsestore.error.{ EventStoreError, OutboxError, SnapshotStoreError }
import io.github.riccardomerolla.zio.eclipsestore.service.*

object NativeLocalEventingSpec extends ZIOSpecDefault:

  enum CounterCommand:
    case Add(delta: Int)

  object CounterCommand:
    given Schema[CounterCommand] = DeriveSchema.gen[CounterCommand]

  enum CounterEvent:
    case Added(delta: Int)

  object CounterEvent:
    given Schema[CounterEvent] = DeriveSchema.gen[CounterEvent]

  enum CounterDomainError:
    case ZeroDelta

  final case class CounterState(total: Int)

  object CounterState:
    given Schema[CounterState] = DeriveSchema.gen[CounterState]

    val zero: CounterState =
      CounterState(0)

  object CounterBehavior extends EventSourcedBehavior[CounterState, CounterCommand, CounterEvent, CounterDomainError]:
    override val zero: CounterState =
      CounterState.zero

    override def decide(
      state: CounterState,
      command: CounterCommand,
    ): IO[CounterDomainError, Chunk[CounterEvent]] =
      command match
        case CounterCommand.Add(0)     => ZIO.fail(CounterDomainError.ZeroDelta)
        case CounterCommand.Add(delta) => ZIO.succeed(Chunk.single(CounterEvent.Added(delta)))

    override def evolve(
      state: CounterState,
      event: CounterEvent,
    ): CounterState =
      event match
        case CounterEvent.Added(delta) => state.copy(total = state.total + delta)

  private val CounterStreamId = StreamId("counter-stream")

  private type CounterRuntime = EventSourcedRuntime[CounterState, CounterCommand, CounterEvent, CounterDomainError]

  private def withTempDirectory[A](prefix: String)(use: Path => ZIO[Any, Throwable, A]): ZIO[Any, Throwable, A] =
    ZIO.scoped {
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory(prefix))) { path =>
        ZIO.attemptBlocking {
          if Files.exists(path) then
            Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
        }.ignore
      }.flatMap(use)
    }

  private def eventStoreLayer(config: NativeLocalEventingConfig): ULayer[EventStore[CounterEvent]] =
    NativeLocalEventStore.live[CounterEvent](config)

  private def snapshotStoreLayer(config: NativeLocalEventingConfig): ULayer[SnapshotStore[CounterState]] =
    NativeLocalSnapshotStore.live[CounterState](config)

  private def runtimeLayer(
    config: NativeLocalEventingConfig,
    snapshotPolicy: SnapshotPolicy[CounterState, CounterEvent] = SnapshotPolicy.everyNEvents(100),
  ): ULayer[EventStore[CounterEvent] & SnapshotStore[CounterState] & CounterRuntime] =
    ZLayer.make[EventStore[CounterEvent] & SnapshotStore[CounterState] & CounterRuntime](
      eventStoreLayer(config),
      snapshotStoreLayer(config),
      EventSourcedRuntimeLive.layer(CounterBehavior, snapshotPolicy),
    )

  private def streamPath(config: NativeLocalEventingConfig, streamId: StreamId): Path =
    config.baseDir.resolve("streams").resolve(encoded(streamId.value) + extension(config.serde))

  private def snapshotPath(config: NativeLocalEventingConfig, streamId: StreamId): Path =
    config.baseDir.resolve("snapshots").resolve(encoded(streamId.value) + extension(config.serde))

  private def encoded(value: String): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8))

  private def extension(serde: NativeLocalSerde): String =
    serde match
      case NativeLocalSerde.Json     => ".json"
      case NativeLocalSerde.Protobuf => ".pb"

  private def overwrite(path: Path, bytes: Array[Byte]): Task[Unit] =
    ZIO.attemptBlocking {
      Option(path.getParent).foreach(Files.createDirectories(_))
      Files.write(path, bytes)
      ()
    }

  private object CounterProjector extends OutboxProjector[CounterEvent]:
    override def project(envelope: EventEnvelope[CounterEvent]): Chunk[OutboxMessage] =
      val payload =
        envelope.payload match
          case CounterEvent.Added(delta) => s"added:$delta"
      Chunk.single(
        OutboxMessage(
          id = s"${envelope.streamId.value}:${envelope.revision.value}",
          streamId = envelope.streamId,
          revision = envelope.revision,
          recordedAt = envelope.recordedAt,
          topic = "counter-events",
          key = envelope.streamId.value,
          headers = envelope.metadata,
          body = Chunk.fromArray(payload.getBytes(StandardCharsets.UTF_8)),
        )
      )

  final private case class RecordingPublisher(
    publishedRef: Ref[Chunk[OutboxMessage]],
    failOnceForRevisionsRef: Ref[Set[Long]],
  ) extends EventPublisher:
    override def publish(message: OutboxMessage): IO[OutboxError, Unit] =
      failOnceForRevisionsRef.modify { pending =>
        if pending.contains(message.revision.value) then
          (
            Left(OutboxError.PublishError(s"Failed publish for ${message.revision.value}", None)),
            pending - message.revision.value,
          )
        else (Right(()), pending)
      }.flatMap {
        case Left(error)  => ZIO.fail(error)
        case Right(value) => publishedRef.update(_ :+ message).as(value)
      }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("NativeLocalEventing")(
      test("EventStore appends new and existing streams with contiguous revisions and survives restart") {
        withTempDirectory("native-local-event-store") { dir =>
          val config = NativeLocalEventingConfig(dir.resolve("json-store"))
          val layer  = eventStoreLayer(config)

          (for
            first      <- EventStore.append[CounterEvent](
                            CounterStreamId,
                            ExpectedVersion.NoStream,
                            Chunk(CounterEvent.Added(1)),
                          ).provideLayer(layer)
            second     <- EventStore.append[CounterEvent](
                            CounterStreamId,
                            ExpectedVersion.Exact(first.currentRevision),
                            Chunk(CounterEvent.Added(2), CounterEvent.Added(3)),
                          ).provideLayer(layer)
            all        <- EventStore.readStream[CounterEvent](CounterStreamId).provideLayer(layer)
            tail       <- EventStore.readStream[CounterEvent](CounterStreamId, Some(StreamRevision(2L))).provideLayer(layer)
            streams    <- EventStore.streams[CounterEvent].provideLayer(layer)
            afterFresh <- EventStore.readStream[CounterEvent](CounterStreamId).provideLayer(layer.fresh)
          yield assertTrue(
            first.currentRevision == StreamRevision(1L),
            second.currentRevision == StreamRevision(3L),
            all.map(_.revision.value) == Chunk(1L, 2L, 3L),
            tail.map(_.revision.value) == Chunk(2L, 3L),
            streams == Chunk(CounterStreamId),
            afterFresh.map(_.revision.value) == Chunk(1L, 2L, 3L),
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
      test("EventStore rejects stale expected versions without persisting new events") {
        withTempDirectory("native-local-event-store-conflict") { dir =>
          val config = NativeLocalEventingConfig(dir.resolve("json-store"))
          val layer  = eventStoreLayer(config)

          (for
            _       <- EventStore.append[CounterEvent](
                         CounterStreamId,
                         ExpectedVersion.NoStream,
                         Chunk(CounterEvent.Added(1)),
                       ).provideLayer(layer)
            failure <- EventStore.append[CounterEvent](
                         CounterStreamId,
                         ExpectedVersion.NoStream,
                         Chunk(CounterEvent.Added(2)),
                       ).provideLayer(layer).either
            journal <- EventStore.readStream[CounterEvent](CounterStreamId).provideLayer(layer)
          yield assertTrue(
            failure == Left(
              EventStoreError.WrongExpectedVersion(
                CounterStreamId,
                ExpectedVersion.NoStream,
                Some(StreamRevision(1L)),
                None,
              )
            ),
            journal.map(_.payload) == Chunk(CounterEvent.Added(1)),
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
      test("runtime rehydrates from the latest snapshot plus the journal tail") {
        withTempDirectory("native-local-event-runtime-tail") { dir =>
          val config = NativeLocalEventingConfig(dir.resolve("json-runtime"))
          val layer  = runtimeLayer(config)

          (for
            _      <- EventStore.append[CounterEvent](
                        CounterStreamId,
                        ExpectedVersion.NoStream,
                        Chunk(CounterEvent.Added(1)),
                      )
            saved  <- SnapshotStore.save[CounterState](
                        SnapshotEnvelope(CounterStreamId, StreamRevision(1L), Instant.EPOCH, CounterState(1))
                      )
            _      <- EventStore.append[CounterEvent](
                        CounterStreamId,
                        ExpectedVersion.Exact(StreamRevision(1L)),
                        Chunk(CounterEvent.Added(2)),
                      )
            loaded <-
              EventSourcedRuntime.load[CounterState, CounterCommand, CounterEvent, CounterDomainError](CounterStreamId)
          yield assertTrue(
            loaded.state == CounterState(3),
            loaded.snapshot.contains(saved),
            loaded.replayedTail.map(_.revision.value) == Chunk(2L),
          )).provideLayer(layer).mapError(err => new RuntimeException(err.toString))
        }
      },
      test("runtime rehydrates from full replay when no snapshot exists, including protobuf persistence") {
        withTempDirectory("native-local-event-runtime-protobuf") { dir =>
          val config = NativeLocalEventingConfig(dir.resolve("protobuf-runtime"), NativeLocalSerde.Protobuf)
          val layer  = runtimeLayer(config, SnapshotPolicy.never)

          (for
            _      <- EventSourcedRuntime.handle[CounterState, CounterCommand, CounterEvent, CounterDomainError](
                        CounterStreamId,
                        ExpectedVersion.NoStream,
                        CounterCommand.Add(2),
                      ).provideLayer(layer)
            _      <- EventSourcedRuntime.handle[CounterState, CounterCommand, CounterEvent, CounterDomainError](
                        CounterStreamId,
                        ExpectedVersion.Any,
                        CounterCommand.Add(3),
                      ).provideLayer(layer)
            loaded <- EventSourcedRuntime
                        .load[CounterState, CounterCommand, CounterEvent, CounterDomainError](CounterStreamId)
                        .provideLayer(layer.fresh)
          yield assertTrue(
            loaded.snapshot.isEmpty,
            loaded.state == CounterState(5),
            loaded.replayedTail.map(_.revision.value) == Chunk(1L, 2L),
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
      test("corrupt journal and snapshot files fail with typed errors") {
        withTempDirectory("native-local-event-corruption") { dir =>
          val config         = NativeLocalEventingConfig(dir.resolve("json-store"))
          val eventLayer     = eventStoreLayer(config)
          val snapshotsLayer = snapshotStoreLayer(config)

          (for
            _               <- EventStore.append[CounterEvent](
                                 CounterStreamId,
                                 ExpectedVersion.NoStream,
                                 Chunk(CounterEvent.Added(1)),
                               ).provideLayer(eventLayer)
            _               <- overwrite(streamPath(config, CounterStreamId), Array[Byte](1, 2, 3))
            journalFailure  <- EventStore.readStream[CounterEvent](CounterStreamId).provideLayer(eventLayer).either
            _               <- SnapshotStore.save[CounterState](
                                 SnapshotEnvelope(CounterStreamId, StreamRevision(1L), Instant.EPOCH, CounterState(1))
                               ).provideLayer(snapshotsLayer)
            _               <- overwrite(snapshotPath(config, CounterStreamId), Array[Byte](4, 5, 6))
            snapshotFailure <-
              SnapshotStore.loadLatest[CounterState](CounterStreamId).provideLayer(snapshotsLayer).either
          yield assertTrue(
            journalFailure.isLeft,
            journalFailure.left.exists {
              case EventStoreError.CorruptStream(streamId, _, _) => streamId == CounterStreamId
              case _                                             => false
            },
            snapshotFailure.isLeft,
            snapshotFailure.left.exists {
              case SnapshotStoreError.CorruptSnapshot(streamId, _, _) => streamId == CounterStreamId
              case _                                                  => false
            },
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
      test(
        "OutboxRelay publishes only persisted events, checkpoints progress, and retries from the last acknowledged revision"
      ) {
        withTempDirectory("native-local-outbox") { dir =>
          val config = NativeLocalEventingConfig(dir.resolve("json-outbox"))

          (for
            publishedRef  <- Ref.make(Chunk.empty[OutboxMessage])
            failOnceRef   <- Ref.make(Set(2L))
            publisherLayer = ZLayer.succeed(RecordingPublisher(publishedRef, failOnceRef))
            relayLayer     = ZLayer.make[EventStore[CounterEvent] & OutboxRelay[CounterEvent]](
                               eventStoreLayer(config),
                               publisherLayer,
                               NativeLocalOutboxRelay.live("counter-relay", config, CounterProjector),
                             )
            _             <- EventStore.append[CounterEvent](
                               CounterStreamId,
                               ExpectedVersion.NoStream,
                               Chunk(CounterEvent.Added(1)),
                             ).provideLayer(relayLayer)
            _             <- EventStore.append[CounterEvent](
                               CounterStreamId,
                               ExpectedVersion.Exact(StreamRevision(1L)),
                               Chunk(CounterEvent.Added(2)),
                             ).provideLayer(relayLayer)
            firstAttempt  <- OutboxRelay.publishPending[CounterEvent]().provideLayer(relayLayer).either
            firstBatch    <- publishedRef.get
            firstCkpt     <- OutboxRelay.checkpoint[CounterEvent].provideLayer(relayLayer)
            retried       <- OutboxRelay.publishPending[CounterEvent]().provideLayer(relayLayer)
            secondBatch   <- publishedRef.get
            finalCkpt     <- OutboxRelay.checkpoint[CounterEvent].provideLayer(relayLayer)
            afterFresh    <- OutboxRelay.publishPending[CounterEvent]().provideLayer(relayLayer.fresh)
          yield assertTrue(
            firstAttempt == Left(OutboxError.PublishError("Failed publish for 2", None)),
            firstBatch.map(_.revision.value) == Chunk(1L),
            firstCkpt.revisionFor(CounterStreamId).contains(StreamRevision(1L)),
            retried.map(_.revision.value) == Chunk(2L),
            secondBatch.map(_.revision.value) == Chunk(1L, 2L),
            finalCkpt.revisionFor(CounterStreamId).contains(StreamRevision(2L)),
            afterFresh.isEmpty,
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
      test("snapshot policies control cadence without changing append durability") {
        withTempDirectory("native-local-snapshot-policies") { dir =>
          val neverLayer  = runtimeLayer(NativeLocalEventingConfig(dir.resolve("never")), SnapshotPolicy.never)
          val alwaysLayer = runtimeLayer(NativeLocalEventingConfig(dir.resolve("always")), SnapshotPolicy.always)
          val everyLayer  =
            runtimeLayer(NativeLocalEventingConfig(dir.resolve("every-two")), SnapshotPolicy.everyNEvents(2))

          (for
            _              <- EventSourcedRuntime.handle[CounterState, CounterCommand, CounterEvent, CounterDomainError](
                                CounterStreamId,
                                ExpectedVersion.NoStream,
                                CounterCommand.Add(1),
                              ).provideLayer(neverLayer)
            neverSnapshot  <- SnapshotStore.loadLatest[CounterState](CounterStreamId).provideLayer(neverLayer)
            neverReloaded  <- EventSourcedRuntime
                                .load[CounterState, CounterCommand, CounterEvent, CounterDomainError](CounterStreamId)
                                .provideLayer(neverLayer.fresh)
            _              <- EventSourcedRuntime.handle[CounterState, CounterCommand, CounterEvent, CounterDomainError](
                                CounterStreamId,
                                ExpectedVersion.NoStream,
                                CounterCommand.Add(1),
                              ).provideLayer(alwaysLayer)
            alwaysSnapshot <- SnapshotStore.loadLatest[CounterState](CounterStreamId).provideLayer(alwaysLayer)
            _              <- EventSourcedRuntime.handle[CounterState, CounterCommand, CounterEvent, CounterDomainError](
                                CounterStreamId,
                                ExpectedVersion.NoStream,
                                CounterCommand.Add(1),
                              ).provideLayer(everyLayer)
            firstEveryTwo  <- SnapshotStore.loadLatest[CounterState](CounterStreamId).provideLayer(everyLayer)
            _              <- EventSourcedRuntime.handle[CounterState, CounterCommand, CounterEvent, CounterDomainError](
                                CounterStreamId,
                                ExpectedVersion.Any,
                                CounterCommand.Add(1),
                              ).provideLayer(everyLayer)
            secondEveryTwo <- SnapshotStore.loadLatest[CounterState](CounterStreamId).provideLayer(everyLayer)
          yield assertTrue(
            neverSnapshot.isEmpty,
            neverReloaded.state == CounterState(1),
            alwaysSnapshot.exists(_.revision == StreamRevision(1L)),
            firstEveryTwo.isEmpty,
            secondEveryTwo.exists(_.revision == StreamRevision(2L)),
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
    )
