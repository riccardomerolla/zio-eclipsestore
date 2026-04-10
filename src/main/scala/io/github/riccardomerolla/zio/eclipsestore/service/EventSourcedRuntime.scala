package io.github.riccardomerolla.zio.eclipsestore.service

import java.time.Instant

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.error.{ EventStoreError, PersistenceError, SnapshotStoreError }

trait EventSourcedBehavior[State, Command, Event, DomainError]:
  def zero: State
  def decide(state: State, command: Command): IO[DomainError, Chunk[Event]]
  def evolve(state: State, event: Event): State

trait SnapshotPolicy[State, Event]:
  def shouldSnapshot(
    currentState: State,
    appendResult: AppendResult[Event],
  ): Boolean

object SnapshotPolicy:
  def never[State, Event]: SnapshotPolicy[State, Event] =
    new SnapshotPolicy[State, Event]:
      override def shouldSnapshot(currentState: State, appendResult: AppendResult[Event]): Boolean = false

  def always[State, Event]: SnapshotPolicy[State, Event] =
    new SnapshotPolicy[State, Event]:
      override def shouldSnapshot(currentState: State, appendResult: AppendResult[Event]): Boolean = true

  def everyNEvents[State, Event](n: Int): SnapshotPolicy[State, Event] =
    new SnapshotPolicy[State, Event]:
      override def shouldSnapshot(currentState: State, appendResult: AppendResult[Event]): Boolean =
        n > 0 && appendResult.currentRevision.value % n.toLong == 0L

  def custom[State, Event](f: (State, AppendResult[Event]) => Boolean): SnapshotPolicy[State, Event] =
    new SnapshotPolicy[State, Event]:
      override def shouldSnapshot(currentState: State, appendResult: AppendResult[Event]): Boolean =
        f(currentState, appendResult)

enum EventSourcedError[+DomainError] extends PersistenceError:
  case DomainFailure(error: DomainError)
  case EventStoreFailure(error: EventStoreError)
  case SnapshotStoreFailure(error: SnapshotStoreError)

sealed trait SnapshotPersistenceStatus[+State] extends Product with Serializable

object SnapshotPersistenceStatus:
  case object Skipped                                                  extends SnapshotPersistenceStatus[Nothing]
  final case class Persisted[State](snapshot: SnapshotEnvelope[State]) extends SnapshotPersistenceStatus[State]
  final case class Failed(error: SnapshotStoreError)                   extends SnapshotPersistenceStatus[Nothing]

final case class RehydratedAggregate[State, Event](
  streamId: StreamId,
  state: State,
  revision: Option[StreamRevision],
  snapshot: Option[SnapshotEnvelope[State]],
  replayedTail: Chunk[EventEnvelope[Event]],
)

final case class EventSourcedResult[State, Event](
  state: State,
  appendResult: AppendResult[Event],
  snapshotStatus: SnapshotPersistenceStatus[State],
)

trait EventSourcedRuntime[State, Command, Event, DomainError]:
  def load(streamId: StreamId): IO[EventSourcedError[Nothing], RehydratedAggregate[State, Event]]
  def handle(
    streamId: StreamId,
    expectedVersion: ExpectedVersion,
    command: Command,
    metadata: Chunk[(String, String)] = Chunk.empty,
  ): IO[EventSourcedError[DomainError], EventSourcedResult[State, Event]]

object EventSourcedRuntime:
  def load[State: Tag, Command: Tag, Event: Tag, DomainError: Tag](streamId: StreamId)
    : ZIO[EventSourcedRuntime[State, Command, Event, DomainError], EventSourcedError[Nothing], RehydratedAggregate[State, Event]] =
    ZIO.serviceWithZIO[EventSourcedRuntime[State, Command, Event, DomainError]](_.load(streamId))

  def handle[State: Tag, Command: Tag, Event: Tag, DomainError: Tag](
    streamId: StreamId,
    expectedVersion: ExpectedVersion,
    command: Command,
    metadata: Chunk[(String, String)] = Chunk.empty,
  ): ZIO[
    EventSourcedRuntime[State, Command, Event, DomainError],
    EventSourcedError[DomainError],
    EventSourcedResult[State, Event],
  ] =
    ZIO.serviceWithZIO[EventSourcedRuntime[State, Command, Event, DomainError]](
      _.handle(streamId, expectedVersion, command, metadata)
    )

final case class EventSourcedRuntimeLive[State, Command, Event, DomainError](
  behavior: EventSourcedBehavior[State, Command, Event, DomainError],
  eventStore: EventStore[Event],
  snapshotStore: SnapshotStore[State],
  snapshotPolicy: SnapshotPolicy[State, Event],
) extends EventSourcedRuntime[State, Command, Event, DomainError]:
  override def load(streamId: StreamId): IO[EventSourcedError[Nothing], RehydratedAggregate[State, Event]] =
    for
      snapshot <- snapshotStore.loadLatest(streamId).mapError(EventSourcedError.SnapshotStoreFailure.apply)
      from      = snapshot.map(_.revision.next)
      tail     <- eventStore.readStream(streamId, from).mapError(EventSourcedError.EventStoreFailure.apply)
      state0    = snapshot.map(_.state).getOrElse(behavior.zero)
      state     = tail.foldLeft(state0) { (current, envelope) =>
                    behavior.evolve(current, envelope.payload)
                  }
      revision  = tail.lastOption.map(_.revision).orElse(snapshot.map(_.revision))
    yield RehydratedAggregate(streamId, state, revision, snapshot, tail)

  override def handle(
    streamId: StreamId,
    expectedVersion: ExpectedVersion,
    command: Command,
    metadata: Chunk[(String, String)] = Chunk.empty,
  ): IO[EventSourcedError[DomainError], EventSourcedResult[State, Event]] =
    for
      hydrated       <- load(streamId)
      decided        <- behavior.decide(hydrated.state, command).mapError(EventSourcedError.DomainFailure.apply)
      appendResult   <- eventStore.append(streamId, expectedVersion, decided, metadata).mapError(
                          EventSourcedError.EventStoreFailure.apply
                        )
      nextState       = appendResult.envelopes.foldLeft(hydrated.state) { (current, envelope) =>
                          behavior.evolve(current, envelope.payload)
                        }
      snapshotEnv     = SnapshotEnvelope(
                          streamId = streamId,
                          revision = appendResult.currentRevision,
                          recordedAt = appendResult.envelopes.lastOption.map(_.recordedAt).getOrElse(Instant.EPOCH),
                          state = nextState,
                        )
      snapshotStatus <-
        if snapshotPolicy.shouldSnapshot(nextState, appendResult) then
          snapshotStore
            .save(snapshotEnv)
            .map[SnapshotPersistenceStatus[State]](SnapshotPersistenceStatus.Persisted.apply)
            .catchAll(error => ZIO.succeed(SnapshotPersistenceStatus.Failed(error)))
        else ZIO.succeed(SnapshotPersistenceStatus.Skipped)
    yield EventSourcedResult(nextState, appendResult, snapshotStatus)

object EventSourcedRuntimeLive:
  def layer[State: Tag, Command: Tag, Event: Tag, DomainError: Tag](
    behavior: EventSourcedBehavior[State, Command, Event, DomainError],
    snapshotPolicy: SnapshotPolicy[State, Event] = SnapshotPolicy.everyNEvents(100),
  ): ZLayer[EventStore[Event] & SnapshotStore[State], Nothing, EventSourcedRuntime[State, Command, Event, DomainError]] =
    ZLayer.fromFunction { (eventStore: EventStore[Event], snapshotStore: SnapshotStore[State]) =>
      EventSourcedRuntimeLive(behavior, eventStore, snapshotStore, snapshotPolicy)
    }
