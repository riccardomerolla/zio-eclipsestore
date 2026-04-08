package io.github.riccardomerolla.zio.eclipsestore.examples.nativelocal

import java.nio.file.{ Path, Paths }
import java.util.UUID

import zio.*
import zio.schema.{ DeriveSchema, Schema }

import io.github.riccardomerolla.zio.eclipsestore.config.BackendConfig
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ ObjectStore, StorageBackend, StorageOps }

final case class TodoId(value: UUID)

final case class TodoItem(
  id: TodoId,
  title: String,
  completed: Boolean,
)

final case class TodoRoot(items: Chunk[TodoItem])

object TodoId:
  given Schema[TodoId] = DeriveSchema.gen[TodoId]

object TodoItem:
  given Schema[TodoItem] = DeriveSchema.gen[TodoItem]

object TodoRoot:
  given Schema[TodoRoot] = DeriveSchema.gen[TodoRoot]

  val descriptor: RootDescriptor[TodoRoot] =
    RootDescriptor.fromSchema(
      id = "todo-native-local-root",
      initializer = () => TodoRoot(Chunk.empty),
    )

trait TodoService:
  def add(title: String): IO[EclipseStoreError, TodoItem]
  def complete(id: TodoId): IO[EclipseStoreError, TodoItem]
  def list: IO[EclipseStoreError, Chunk[TodoItem]]
  def checkpointAndReload: IO[EclipseStoreError, Chunk[TodoItem]]

final case class TodoServiceLive(
  store: ObjectStore[TodoRoot],
  ops: StorageOps[TodoRoot],
) extends TodoService:
  override def add(title: String): IO[EclipseStoreError, TodoItem] =
    store.modify { root =>
      val item = TodoItem(TodoId(UUID.randomUUID()), title, completed = false)
      ZIO.succeed((item, root.copy(items = root.items :+ item)))
    }

  override def complete(id: TodoId): IO[EclipseStoreError, TodoItem] =
    store.modify { root =>
      root.items.indexWhere(_.id == id) match
        case -1    =>
          ZIO.fail(EclipseStoreError.QueryError(s"Todo ${id.value} not found", None))
        case index =>
          val updated = root.items(index).copy(completed = true)
          ZIO.succeed(
            (
              updated,
              root.copy(items = root.items.updated(index, updated)),
            )
          )
    }

  override def list: IO[EclipseStoreError, Chunk[TodoItem]] =
    store.load.map(_.items)

  override def checkpointAndReload: IO[EclipseStoreError, Chunk[TodoItem]] =
    ops.checkpoint *> ops.restart *> list

object TodoService:
  def add(title: String): ZIO[TodoService, EclipseStoreError, TodoItem] =
    ZIO.serviceWithZIO[TodoService](_.add(title))

  def complete(id: TodoId): ZIO[TodoService, EclipseStoreError, TodoItem] =
    ZIO.serviceWithZIO[TodoService](_.complete(id))

  val list: ZIO[TodoService, EclipseStoreError, Chunk[TodoItem]] =
    ZIO.serviceWithZIO[TodoService](_.list)

  val checkpointAndReload: ZIO[TodoService, EclipseStoreError, Chunk[TodoItem]] =
    ZIO.serviceWithZIO[TodoService](_.checkpointAndReload)

  def layer(snapshotPath: Path): ZLayer[Any, EclipseStoreError, TodoService] =
    ZLayer.make[TodoService](
      ZLayer.succeed(BackendConfig.NativeLocal(snapshotPath)),
      StorageBackend.rootServices(TodoRoot.descriptor),
      ZLayer.fromFunction(TodoServiceLive.apply),
    )

object TodoNativeLocalApp extends ZIOAppDefault:
  private val snapshotPath = Paths.get("todo-native-local.snapshot.json")

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    val program =
      for
        first    <- TodoService.add("write snapshot guide")
        second   <- TodoService.add("verify restart semantics")
        _        <- TodoService.complete(first.id)
        reloaded <- TodoService.checkpointAndReload
        _        <- ZIO.logInfo(s"Reloaded todos: ${reloaded.map(todo => s"${todo.title}:${todo.completed}").mkString(", ")}")
        _        <- ZIO.logInfo(s"Snapshot file: $snapshotPath")
        _        <- ZIO.logDebug(
                      s"Second todo still pending: ${reloaded.exists(todo => todo.id == second.id && !todo.completed)}"
                    )
      yield ()

    program
      .provide(TodoService.layer(snapshotPath))
      .catchAll(error => ZIO.logError(error.toString))
