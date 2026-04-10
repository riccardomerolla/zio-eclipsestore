package io.github.riccardomerolla.zio.eclipsestore.examples.nativelocal

import java.nio.file.{ Path, Paths }

import zio.*
import zio.schema.{ DeriveSchema, Schema }

import io.github.riccardomerolla.zio.eclipsestore.config.{ BackendConfig, NativeLocalSerde }
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ ObjectStore, StorageBackend, StorageOps }

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

final case class TodoEventRecord(
  sequence: Long,
  event: TodoEvent,
)

object TodoEventRecord:
  given Schema[TodoEventRecord] = DeriveSchema.gen[TodoEventRecord]

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

final case class TodoEventJournalRoot(
  snapshot: TodoProjection,
  journal: Chunk[TodoEventRecord],
  nextSequence: Long,
)

object TodoEventJournalRoot:
  given Schema[TodoEventJournalRoot] = DeriveSchema.gen[TodoEventJournalRoot]

  val empty: TodoEventJournalRoot =
    TodoEventJournalRoot(
      snapshot = TodoProjection.empty,
      journal = Chunk.empty,
      nextSequence = 1L,
    )

  val descriptor: RootDescriptor[TodoEventJournalRoot] =
    RootDescriptor.fromSchema(
      id = "todo-native-local-event-sourcing-root",
      initializer = () => empty,
    )

enum TodoEventSourcingError:
  case EmptyTitle
  case EmptyPriority
  case TodoNotFound(id: TodoId)
  case TodoAlreadyCompleted(id: TodoId)
  case SnapshotDrift(expected: TodoProjection, replayed: TodoProjection)

  def message: String =
    this match
      case EmptyTitle                        => "Todo title must not be empty"
      case EmptyPriority                     => "Todo priority must not be empty"
      case TodoNotFound(id)                  => s"Todo ${id.value} not found"
      case TodoAlreadyCompleted(id)          => s"Todo ${id.value} is already completed"
      case SnapshotDrift(expected, replayed) =>
        s"Stored snapshot drift detected. Expected=${expected.items.size} replayed=${replayed.items.size}"

object TodoEventSourcing:
  def replay(journal: Chunk[TodoEventRecord]): TodoProjection =
    journal.foldLeft(TodoProjection.empty) { (state, record) =>
      evolve(state, record.event)
    }

  def decide(
    state: TodoProjection,
    command: TodoEventCommand,
  ): Either[TodoEventSourcingError, Chunk[TodoEvent]] =
    command match
      case TodoEventCommand.AddTodo(_, title, _) if title.trim.isEmpty           =>
        Left(TodoEventSourcingError.EmptyTitle)
      case TodoEventCommand.AddTodo(_, title, priority) if priority.trim.isEmpty =>
        Left(TodoEventSourcingError.EmptyPriority)
      case TodoEventCommand.AddTodo(id, title, priority)                         =>
        Right(
          Chunk.single(
            TodoEvent.TodoAdded(
              id = id,
              title = title.trim,
              priority = priority.trim,
            )
          )
        )
      case TodoEventCommand.CompleteTodo(id)                                     =>
        state.items.find(_.id == id) match
          case None                         => Left(TodoEventSourcingError.TodoNotFound(id))
          case Some(todo) if todo.completed =>
            Left(TodoEventSourcingError.TodoAlreadyCompleted(id))
          case Some(_)                      =>
            Right(Chunk.single(TodoEvent.TodoCompleted(id)))

  def runCommand(
    root: TodoEventJournalRoot,
    command: TodoEventCommand,
  ): Either[TodoEventSourcingError, (Chunk[TodoEventRecord], TodoEventJournalRoot)] =
    for
      _      <- validate(root)
      events <- decide(root.snapshot, command)
    yield
      val records      =
        events.zipWithIndex.map {
          case (event, index) =>
            TodoEventRecord(root.nextSequence + index.toLong, event)
        }
      val nextSnapshot =
        records.foldLeft(root.snapshot) { (state, record) =>
          evolve(state, record.event)
        }
      (
        records,
        root.copy(
          snapshot = nextSnapshot,
          journal = root.journal ++ records,
          nextSequence = root.nextSequence + records.size.toLong,
        ),
      )

  def validate(root: TodoEventJournalRoot): Either[TodoEventSourcingError, Unit] =
    val replayed = replay(root.journal)
    Either.cond(
      replayed == root.snapshot,
      (),
      TodoEventSourcingError.SnapshotDrift(root.snapshot, replayed),
    )

  private def evolve(
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
  def process(command: TodoEventCommand): IO[EclipseStoreError, Chunk[TodoEventRecord]]
  def currentState: IO[EclipseStoreError, TodoProjection]
  def journal: IO[EclipseStoreError, Chunk[TodoEventRecord]]
  def replayedState: IO[EclipseStoreError, TodoProjection]
  def checkpointAndReload: IO[EclipseStoreError, TodoEventJournalRoot]

final case class TodoEventSourcedServiceLive(
  store: ObjectStore[TodoEventJournalRoot],
  ops: StorageOps[TodoEventJournalRoot],
) extends TodoEventSourcedService:
  override def add(title: String, priority: String): IO[EclipseStoreError, TodoProjectionItem] =
    for
      id      <- Random.nextUUID.map(TodoId.apply)
      records <- process(TodoEventCommand.AddTodo(id, title, priority))
      added   <- ZIO
                   .fromOption(
                     records.collectFirst {
                       case TodoEventRecord(_, TodoEvent.TodoAdded(todoId, todoTitle, todoPriority)) =>
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

  override def process(command: TodoEventCommand): IO[EclipseStoreError, Chunk[TodoEventRecord]] =
    for
      records <- store.modify { root =>
                   ZIO.fromEither(TodoEventSourcing.runCommand(root, command)).mapError(toStoreError)
                 }
      _       <- ops.checkpoint
    yield records

  override def currentState: IO[EclipseStoreError, TodoProjection] =
    store.load.map(_.snapshot)

  override def journal: IO[EclipseStoreError, Chunk[TodoEventRecord]] =
    store.load.map(_.journal)

  override def replayedState: IO[EclipseStoreError, TodoProjection] =
    store.load.flatMap { root =>
      ZIO.fromEither(
        TodoEventSourcing.validate(root).map(_ => TodoEventSourcing.replay(root.journal))
      ).mapError(toStoreError)
    }

  override def checkpointAndReload: IO[EclipseStoreError, TodoEventJournalRoot] =
    ops.checkpoint *> ops.restart *> store.load

  private def toStoreError(error: TodoEventSourcingError): EclipseStoreError =
    EclipseStoreError.QueryError(error.message, None)

object TodoEventSourcedService:
  def add(title: String, priority: String): ZIO[TodoEventSourcedService, EclipseStoreError, TodoProjectionItem] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.add(title, priority))

  def complete(id: TodoId): ZIO[TodoEventSourcedService, EclipseStoreError, TodoProjectionItem] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.complete(id))

  def process(command: TodoEventCommand): ZIO[TodoEventSourcedService, EclipseStoreError, Chunk[TodoEventRecord]] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.process(command))

  val currentState: ZIO[TodoEventSourcedService, EclipseStoreError, TodoProjection] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.currentState)

  val journal: ZIO[TodoEventSourcedService, EclipseStoreError, Chunk[TodoEventRecord]] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.journal)

  val replayedState: ZIO[TodoEventSourcedService, EclipseStoreError, TodoProjection] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.replayedState)

  val checkpointAndReload: ZIO[TodoEventSourcedService, EclipseStoreError, TodoEventJournalRoot] =
    ZIO.serviceWithZIO[TodoEventSourcedService](_.checkpointAndReload)

  def layer(
    snapshotPath: Path,
    serde: NativeLocalSerde = NativeLocalSerde.Json,
  ): ZLayer[Any, EclipseStoreError, TodoEventSourcedService] =
    ZLayer.make[TodoEventSourcedService](
      ZLayer.succeed(BackendConfig.NativeLocal(snapshotPath, serde)),
      StorageBackend.rootServices(TodoEventJournalRoot.descriptor),
      ZLayer.fromFunction(TodoEventSourcedServiceLive.apply),
    )

object TodoNativeLocalEventSourcingApp extends ZIOAppDefault:
  private val snapshotPath = Paths.get("todo-native-local-event-sourcing.snapshot.json")

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    val program =
      for
        addedOne <- TodoEventSourcedService.add("write event journal guide", "high")
        _        <- TodoEventSourcedService.add("document replay semantics", "normal")
        _        <- TodoEventSourcedService.complete(addedOne.id)
        root     <- TodoEventSourcedService.checkpointAndReload
        replayed <- TodoEventSourcedService.replayedState
        _        <- ZIO.logInfo(
                      s"Reloaded ${root.journal.size} journal entries and ${root.snapshot.items.size} projected todos"
                    )
        _        <- ZIO.logDebug(s"Projection matches replay: ${root.snapshot == replayed}")
        _        <- ZIO.logInfo(s"Snapshot file: $snapshotPath")
      yield ()

    program
      .provide(TodoEventSourcedService.layer(snapshotPath))
      .catchAll(error => ZIO.logError(error.toString))
