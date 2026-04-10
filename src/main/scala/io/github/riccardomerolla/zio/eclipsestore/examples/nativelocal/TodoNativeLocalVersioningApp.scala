package io.github.riccardomerolla.zio.eclipsestore.examples.nativelocal

import java.nio.file.{ Path, Paths }
import java.util.UUID

import zio.*
import zio.json.ast.Json
import zio.schema.{ DeriveSchema, Schema }

import io.github.riccardomerolla.zio.eclipsestore.config.{ BackendConfig, NativeLocalSerde }
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.schema.DerivedMigrationPlan
import io.github.riccardomerolla.zio.eclipsestore.service.*

enum TodoSchemaVersion:
  case V1
  case V2

object TodoSchemaVersion:
  given Schema[TodoSchemaVersion] = DeriveSchema.gen[TodoSchemaVersion]

final case class TodoItemV1(
  id: TodoId,
  title: String,
  completed: Boolean,
  legacyCategory: String,
)

object TodoItemV1:
  given Schema[TodoItemV1] = DeriveSchema.gen[TodoItemV1]

final case class TodoRootV1(items: Chunk[TodoItemV1])

object TodoRootV1:
  given Schema[TodoRootV1] = DeriveSchema.gen[TodoRootV1]

  val descriptor: RootDescriptor[TodoRootV1] =
    RootDescriptor.fromSchema(
      id = "todo-native-local-versioned-root",
      initializer = () => TodoRootV1(Chunk.empty),
    )

final case class TodoItemV2(
  id: TodoId,
  title: String,
  completed: Boolean,
  priority: String,
)

object TodoItemV2:
  given Schema[TodoItemV2] = DeriveSchema.gen[TodoItemV2]

final case class TodoRootV2(items: Chunk[TodoItemV2])

object TodoRootV2:
  given Schema[TodoRootV2] = DeriveSchema.gen[TodoRootV2]

  val descriptor: RootDescriptor[TodoRootV2] =
    RootDescriptor.fromSchema(
      id = "todo-native-local-versioned-root",
      initializer = () => TodoRootV2(Chunk.empty),
    )

final case class TodoMigrationReport(
  from: TodoSchemaVersion,
  to: TodoSchemaVersion,
  migratedTodos: Int,
)

object TodoMigrationReport:
  given Schema[TodoMigrationReport] = DeriveSchema.gen[TodoMigrationReport]

trait TodoV1Service:
  def add(title: String, legacyCategory: String): IO[EclipseStoreError, TodoItemV1]
  def complete(id: TodoId): IO[EclipseStoreError, TodoItemV1]
  def checkpoint: IO[EclipseStoreError, Unit]

final case class TodoV1ServiceLive(
  store: ObjectStore[TodoRootV1],
  ops: StorageOps[TodoRootV1],
) extends TodoV1Service:
  override def add(title: String, legacyCategory: String): IO[EclipseStoreError, TodoItemV1] =
    store.modify { root =>
      val item = TodoItemV1(TodoId(UUID.randomUUID()), title, completed = false, legacyCategory)
      ZIO.succeed((item, root.copy(items = root.items :+ item)))
    }

  override def complete(id: TodoId): IO[EclipseStoreError, TodoItemV1] =
    store.modify { root =>
      root.items.indexWhere(_.id == id) match
        case -1    =>
          ZIO.fail(EclipseStoreError.QueryError(s"Legacy todo ${id.value} not found", None))
        case index =>
          val updated = root.items(index).copy(completed = true)
          ZIO.succeed((updated, root.copy(items = root.items.updated(index, updated))))
    }

  override def checkpoint: IO[EclipseStoreError, Unit] =
    ops.checkpoint.unit

object TodoV1Service:
  def add(title: String, legacyCategory: String): ZIO[TodoV1Service, EclipseStoreError, TodoItemV1] =
    ZIO.serviceWithZIO[TodoV1Service](_.add(title, legacyCategory))

  def complete(id: TodoId): ZIO[TodoV1Service, EclipseStoreError, TodoItemV1] =
    ZIO.serviceWithZIO[TodoV1Service](_.complete(id))

  val checkpoint: ZIO[TodoV1Service, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[TodoV1Service](_.checkpoint)

  def layer(
    snapshotPath: Path,
    serde: NativeLocalSerde = NativeLocalSerde.Json,
  ): ZLayer[Any, EclipseStoreError, TodoV1Service] =
    ZLayer.make[TodoV1Service](
      ZLayer.succeed(BackendConfig.NativeLocal(snapshotPath, serde)),
      StorageBackend.rootServices(TodoRootV1.descriptor),
      ZLayer.fromFunction(TodoV1ServiceLive.apply),
    )

trait TodoV2Service:
  def add(title: String, priority: String): IO[EclipseStoreError, TodoItemV2]
  def list: IO[EclipseStoreError, Chunk[TodoItemV2]]
  def checkpointAndReload: IO[EclipseStoreError, Chunk[TodoItemV2]]

final case class TodoV2ServiceLive(
  store: ObjectStore[TodoRootV2],
  ops: StorageOps[TodoRootV2],
) extends TodoV2Service:
  override def add(title: String, priority: String): IO[EclipseStoreError, TodoItemV2] =
    store.modify { root =>
      val item = TodoItemV2(TodoId(UUID.randomUUID()), title, completed = false, priority)
      ZIO.succeed((item, root.copy(items = root.items :+ item)))
    }

  override def list: IO[EclipseStoreError, Chunk[TodoItemV2]] =
    store.load.map(_.items)

  override def checkpointAndReload: IO[EclipseStoreError, Chunk[TodoItemV2]] =
    ops.checkpoint *> ops.restart *> list

object TodoV2Service:
  def add(title: String, priority: String): ZIO[TodoV2Service, EclipseStoreError, TodoItemV2] =
    ZIO.serviceWithZIO[TodoV2Service](_.add(title, priority))

  val list: ZIO[TodoV2Service, EclipseStoreError, Chunk[TodoItemV2]] =
    ZIO.serviceWithZIO[TodoV2Service](_.list)

  val checkpointAndReload: ZIO[TodoV2Service, EclipseStoreError, Chunk[TodoItemV2]] =
    ZIO.serviceWithZIO[TodoV2Service](_.checkpointAndReload)

  def layer(
    snapshotPath: Path,
    serde: NativeLocalSerde = NativeLocalSerde.Json,
    migrationRegistry: NativeLocalSnapshotMigrationRegistry[TodoRootV2] = NativeLocalSnapshotMigrationRegistry
      .none[TodoRootV2],
  ): ZLayer[Any, EclipseStoreError, TodoV2Service] =
    ZLayer.make[TodoV2Service](
      ZLayer.succeed(BackendConfig.NativeLocal(snapshotPath, serde)),
      StorageBackend.rootServices(TodoRootV2.descriptor, migrationRegistry = migrationRegistry),
      ZLayer.fromFunction(TodoV2ServiceLive.apply),
    )

  def autoLayer(
    snapshotPath: Path,
    serde: NativeLocalSerde = NativeLocalSerde.Json,
  ): ZLayer[Any, EclipseStoreError, TodoV2Service] =
    layer(snapshotPath, serde, TodoVersioning.migrationRegistry)

object TodoVersioning:
  val itemV1ToV2: DerivedMigrationPlan[TodoItemV1, TodoItemV2] =
    DerivedMigrationPlan.derive[TodoItemV1, TodoItemV2](
      newFieldDefaults = Map("priority" -> Json.Str("normal"))
    )

  val snapshotMigrationPlan: NativeLocalMigrationPlan[TodoRootV2] =
    NativeLocalMigrationPlan.fromFunction[TodoRootV1, TodoRootV2](
      rootId = TodoRootV1.descriptor.id,
      sourceSchemaVersion = None,
      targetSchemaVersion = Some(TodoSchemaVersion.V2.ordinal),
    ) { legacy =>
      itemV1ToV2.validate *>
        ZIO.foreach(legacy.items)(itemV1ToV2.migrate).map(TodoRootV2.apply)
    }

  val migrationRegistry: NativeLocalSnapshotMigrationRegistry[TodoRootV2] =
    NativeLocalSnapshotMigrationRegistry.single(snapshotMigrationPlan)

  def migrateSnapshot(
    snapshotPath: Path,
    serde: NativeLocalSerde = NativeLocalSerde.Json,
  ): IO[EclipseStoreError, TodoMigrationReport] =
    for
      existing <- SnapshotCodec.loadEnvelopedOrElse(
                    snapshotPath,
                    TodoRootV1.descriptor.id,
                    serde,
                    TodoRootV1(Chunk.empty),
                  )
      migrated <- NativeLocalSnapshotMigrationTool.rewrite(
                    snapshotPath,
                    TodoRootV2.descriptor.id,
                    serde,
                    TodoRootV2(Chunk.empty),
                    migrationRegistry,
                  )
    yield TodoMigrationReport(
      from = TodoSchemaVersion.V1,
      to = TodoSchemaVersion.V2,
      migratedTodos = existing.value.items.size.max(migrated.value.items.size),
    )

object TodoNativeLocalVersioningApp extends ZIOAppDefault:
  private val snapshotPath = Paths.get("todo-native-local-versioned.snapshot.json")

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    val seedV1 =
      (for
        first  <- TodoV1Service.add("write migration guide", "docs")
        second <- TodoV1Service.add("retire v1 category field", "ops")
        _      <- TodoV1Service.complete(first.id)
        _      <- TodoV1Service.checkpoint
      yield second.id).provide(TodoV1Service.layer(snapshotPath))

    val continueWithV2 =
      (for
        before   <- TodoV2Service.list
        added    <- TodoV2Service.add("create v2-only todo", "high")
        reloaded <- TodoV2Service.checkpointAndReload
        _        <- ZIO.logInfo(
                      s"Loaded ${before.size} migrated todos and ${reloaded.size} total todos after adding ${added.title}"
                    )
        _        <- ZIO.logInfo(s"Snapshot file: $snapshotPath")
      yield ()).provide(TodoV2Service.autoLayer(snapshotPath))

    (for
      _ <- seedV1
      _ <- continueWithV2
    yield ()).catchAll(error => ZIO.logError(error.toString))
