package io.github.riccardomerolla.zio.eclipsestore.examples.nativelocal

import java.nio.file.{ Path, Paths }

import zio.*
import zio.schema.{ DeriveSchema, Schema }

import io.github.riccardomerolla.zio.eclipsestore.config.{ NativeLocalEventingConfig, NativeLocalSerde }
import io.github.riccardomerolla.zio.eclipsestore.error.{ EclipseStoreError, EventStoreError, SnapshotStoreError }
import io.github.riccardomerolla.zio.eclipsestore.service.*

enum TodoEventCommand:
  case AddTodo(id: TodoId, title: String, priority: String)
  case CompleteTodo(id: TodoId)

object TodoEventCommand:
  given Schema[TodoEventCommand] = DeriveSchema.gen[TodoEventCommand]

enum TodoEvent:
  case TodoAdded(id: TodoId, title: String, priority: String)
  case TodoCompleted(id: TodoId)

object TodoEvent:
  given Schema[TodoEvent] = DeriveSchema.gen[TodoEvent]

final case class TodoProjectionItem(
  id: TodoId,
  title: String,
  completed: Boolean,
  priority: String,
)

object TodoProjectionItem:
  given Schema[TodoProjectionItem] = DeriveSchema.gen[TodoProjectionItem]

final case class TodoProjection(items: Chunk[TodoProjectionItem])

object TodoProjection:
  given Schema[TodoProjection] = DeriveSchema.gen[TodoProjection]

  val empty: TodoProjection =
    TodoProjection(Chunk.empty)

enum TodoEventSourcingError:
  case EmptyTitle
  case EmptyPriority
  case TodoNotFound(id: TodoId)
  case TodoAlreadyCompleted(id: TodoId)

  def message: String =
    this match
      case EmptyTitle               => "Todo title must not be empty"
      case EmptyPriority            => "Todo priority must not be empty"
      case TodoNotFound(id)         => s"Todo ${id.value} not found"
      case TodoAlreadyCompleted(id) => s"Todo ${id.value} is already completed"

object TodoEventSourcing
  extends EventSourcedBehavior[TodoProjection, TodoEventCommand, TodoEvent, TodoEventSourcingError]:
  val streamId: StreamId =
    StreamId("todo-event-sourced-list")

  override val zero: TodoProjection =
    TodoProjection.empty

  def replay(journal: Chunk[EventEnvelope[TodoEvent]]): TodoProjection =
    journal.foldLeft(zero) { (state, envelope) =>
      evolve(state, envelope.payload)
    }

  override def decide(
    state: TodoProjection,
    command: TodoEventCommand,
  ): IO[TodoEventSourcingError, Chunk[TodoEvent]] =
    ZIO.fromEither(decidePure(state, command))

  private def decidePure(
    state: TodoProjection,
    command: TodoEventCommand,
  ): Either[TodoEventSourcingError, Chunk[TodoEvent]] =
    command match
      case TodoEventCommand.AddTodo(_, title, _) if title.trim.isEmpty       =>
        Left(TodoEventSourcingError.EmptyTitle)
      case TodoEventCommand.AddTodo(_, _, priority) if priority.trim.isEmpty =>
        Left(TodoEventSourcingError.EmptyPriority)
      case TodoEventCommand.AddTodo(id, title, priority)                     =>
        Right(Chunk.single(TodoEvent.TodoAdded(id, title.trim, priority.trim)))
      case TodoEventCommand.CompleteTodo(id)                                 =>
        state.items.find(_.id == id) match
          case None                         => Left(TodoEventSourcingError.TodoNotFound(id))
          case Some(todo) if todo.completed =>
            Left(TodoEventSourcingError.TodoAlreadyCompleted(id))
          case Some(_)                      =>
            Right(Chunk.single(TodoEvent.TodoCompleted(id)))

  override def evolve(
    state: TodoProjection,
    event: TodoEvent,
  ): TodoProjection =
    event match
      case TodoEvent.TodoAdded(id, title, priority) =>
        state.copy(
          items = state.items :+ TodoProjectionItem(id, title, completed = false, priority)
        )
      case TodoEvent.TodoCompleted(id)              =>
        state.copy(
          items = state.items.map { item =>
            if item.id == id then item.copy(completed = true) else item
          }
        )

trait TodoEventSourcedService:
  def add(title: String, priority: String): IO[EclipseStoreError, TodoProjectionItem]
  def complete(id: TodoId): IO[EclipseStoreError, TodoProjectionItem]
  def process(command: TodoEventCommand): IO[EclipseStoreError, Chunk[EventEnvelope[TodoEvent]]]
  def currentState: IO[EclipseStoreError, TodoProjection]
  def journal: IO[EclipseStoreError, Chunk[EventEnvelope[TodoEvent]]]
  def replayedState: IO[EclipseStoreError, TodoProjection]
  def reload: IO[EclipseStoreError, RehydratedAggregate[TodoProjection, TodoEvent]]
  def latestSnapshot: IO[EclipseStoreError, Option[SnapshotEnvelope[TodoProjection]]]

final case class TodoEventSourcedServiceLive(
  runtime: EventSourcedRuntime[TodoProjection, TodoEventCommand, TodoEvent, TodoEventSourcingError],
  eventStore: EventStore[TodoEvent],
  snapshotStore: SnapshotStore[TodoProjection],
) extends TodoEventSourcedService:
  override def add(title: String, priority: String): IO[EclipseStoreError, TodoProjectionItem] =
    for
      id        <- Random.nextUUID.map(TodoId.apply)
      envelopes <- process(TodoEventCommand.AddTodo(id, title, priority))
      added     <- ZIO
                     .fromOption(
                       envelopes.collectFirst {
                         case EventEnvelope(_, _, _, TodoEvent.TodoAdded(todoId, todoTitle, todoPriority), _) =>
                           TodoProjectionItem(todoId, todoTitle, completed = false, todoPriority)
                       }
                     )
                     .orElseFail(EclipseStoreError.QueryError("Expected TodoAdded event after add command", None))
    yield added

  override def complete(id: TodoId): IO[EclipseStoreError, TodoProjectionItem] =
    process(TodoEventCommand.CompleteTodo(id)) *> currentState.flatMap { snapshot =>
      ZIO
        .fromOption(snapshot.items.find(_.id == id))
        .orElseFail(EclipseStoreError.QueryError(s"Todo ${id.value} not found after completion", None))
    }

  override def process(command: TodoEventCommand): IO[EclipseStoreError, Chunk[EventEnvelope[TodoEvent]]] =
    for
      aggregate <- reload
      expected   = aggregate.revision.fold[ExpectedVersion](ExpectedVersion.NoStream)(ExpectedVersion.Exact.apply)
      result    <- runtime
                     .handle(TodoEventSourcing.streamId, expected, command)
                     .mapError(toStoreError)
    yield result.appendResult.envelopes

  override def currentState: IO[EclipseStoreError, TodoProjection] =
    reload.map(_.state)

  override def journal: IO[EclipseStoreError, Chunk[EventEnvelope[TodoEvent]]] =
    eventStore.readStream(TodoEventSourcing.streamId).mapError(toStoreError)

  override def replayedState: IO[EclipseStoreError, TodoProjection] =
    journal.map(TodoEventSourcing.replay)

  override def reload: IO[EclipseStoreError, RehydratedAggregate[TodoProjection, TodoEvent]] =
    runtime.load(TodoEventSourcing.streamId).mapError(toStoreError)

  override def latestSnapshot: IO[EclipseStoreError, Option[SnapshotEnvelope[TodoProjection]]] =
    snapshotStore.loadLatest(TodoEventSourcing.streamId).mapError(toStoreError)

  private def toStoreError(error: EventSourcedError[?]): EclipseStoreError =
    error match
      case EventSourcedError.DomainFailure(domainError: TodoEventSourcingError)                              =>
        EclipseStoreError.QueryError(domainError.message, None)
      case EventSourcedError.DomainFailure(other)                                                            =>
        EclipseStoreError.QueryError(other.toString, None)
      case EventSourcedError.EventStoreFailure(EventStoreError.WrongExpectedVersion(_, expected, actual, _)) =>
        EclipseStoreError.ConflictError(
          s"Todo event stream changed while processing command (expected=$expected, actual=$actual)",
          None,
        )
      case EventSourcedError.EventStoreFailure(storeError)                                                   =>
        EclipseStoreError.StorageError(storeError.toString, None)
      case EventSourcedError.SnapshotStoreFailure(snapshotError)                                             =>
        EclipseStoreError.StorageError(snapshotError.toString, None)

  private def toStoreError(error: EventStoreError): EclipseStoreError =
    EclipseStoreError.StorageError(error.toString, None)

  private def toStoreError(error: SnapshotStoreError): EclipseStoreError =
    EclipseStoreError.StorageError(error.toString, None)

object TodoEventSourcedService:
  def add(title: String, priority: String): ZIO[TodoEventSourcedService, EclipseStoreError, TodoProjectionItem] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.add(title, priority))

  def complete(id: TodoId): ZIO[TodoEventSourcedService, EclipseStoreError, TodoProjectionItem] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.complete(id))

  def process(
    command: TodoEventCommand
  ): ZIO[TodoEventSourcedService, EclipseStoreError, Chunk[EventEnvelope[TodoEvent]]] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.process(command))

  val currentState: ZIO[TodoEventSourcedService, EclipseStoreError, TodoProjection] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.currentState)

  val journal: ZIO[TodoEventSourcedService, EclipseStoreError, Chunk[EventEnvelope[TodoEvent]]] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.journal)

  val replayedState: ZIO[TodoEventSourcedService, EclipseStoreError, TodoProjection] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.replayedState)

  val reload: ZIO[TodoEventSourcedService, EclipseStoreError, RehydratedAggregate[TodoProjection, TodoEvent]] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.reload)

  val latestSnapshot: ZIO[TodoEventSourcedService, EclipseStoreError, Option[SnapshotEnvelope[TodoProjection]]] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.latestSnapshot)

  def layer(
    baseDir: Path,
    serde: NativeLocalSerde = NativeLocalSerde.Json,
  ): ZLayer[Any, Nothing, TodoEventSourcedService] =
    val config = NativeLocalEventingConfig(baseDir, serde)
    ZLayer.make[TodoEventSourcedService](
      NativeLocalEventStore.live[TodoEvent](config),
      NativeLocalSnapshotStore.live[TodoProjection](config),
      EventSourcedRuntimeLive.layer(
        TodoEventSourcing,
        SnapshotPolicy.everyNEvents[TodoProjection, TodoEvent](100),
      ),
      ZLayer.fromFunction(TodoEventSourcedServiceLive.apply),
    )

object TodoNativeLocalEventSourcingApp extends ZIOAppDefault:
  private val baseDir = Paths.get("todo-native-local-eventing")

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    val program =
      for
        addedOne <- TodoEventSourcedService.add("write event journal guide", "high")
        _        <- TodoEventSourcedService.add("document replay semantics", "normal")
        _        <- TodoEventSourcedService.complete(addedOne.id)
        reloaded <- TodoEventSourcedService.reload
        replayed <- TodoEventSourcedService.replayedState
        snapshot <- TodoEventSourcedService.latestSnapshot
        _        <-
          ZIO.logInfo(
            s"Reloaded ${reloaded.replayedTail.size} journal entries with ${reloaded.state.items.size} projected todos"
          )
        _        <- ZIO.logDebug(s"Projection matches replay: ${reloaded.state == replayed}")
        _        <- ZIO.logDebug(s"Snapshot present: ${snapshot.nonEmpty}")
        _        <- ZIO.logInfo(s"Eventing base dir: $baseDir")
      yield ()

    program
      .provide(TodoEventSourcedService.layer(baseDir))
      .catchAll(error => ZIO.logError(error.toString))
