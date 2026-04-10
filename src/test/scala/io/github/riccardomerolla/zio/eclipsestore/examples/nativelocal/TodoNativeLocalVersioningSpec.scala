package io.github.riccardomerolla.zio.eclipsestore.examples.nativelocal

import java.nio.file.{ Files, Path }

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde
import io.github.riccardomerolla.zio.eclipsestore.service.{ NativeLocalSnapshotEnvelope, SnapshotCodec }

object TodoNativeLocalVersioningSpec extends ZIOSpecDefault:

  private def withTempDirectory[A](prefix: String)(use: Path => ZIO[Any, Throwable, A]): ZIO[Any, Throwable, A] =
    ZIO.scoped {
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory(prefix))) { path =>
        ZIO.attemptBlocking {
          if Files.exists(path) then
            Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
        }.ignore
      }.flatMap(use)
    }

  private def versioningScenario(
    serde: NativeLocalSerde,
    extension: String,
  ): ZIO[Any, Throwable, TestResult] =
    withTempDirectory(s"todo-versioning-${extension.replace(".", "")}") { dir =>
      val snapshotPath = dir.resolve(s"todo-versioned$extension")
      val v1Layer      = TodoV1Service.layer(snapshotPath, serde)
      val v2Layer      = TodoV2Service.layer(snapshotPath, serde)

      (for
        seeded <- (for
                    first  <- TodoV1Service.add("keep existing todos", "docs")
                    second <- TodoV1Service.add("remove legacy field", "ops")
                    _      <- TodoV1Service.complete(second.id)
                    _      <- TodoV1Service.checkpoint
                  yield (first, second)).provideLayer(v1Layer)
        report <- TodoVersioning.migrateSnapshot(snapshotPath, serde)
        after  <- (for
                    migrated  <- TodoV2Service.list
                    createdV2 <- TodoV2Service.add("new v2 todo", "high")
                    reopened  <- TodoV2Service.checkpointAndReload
                  yield (migrated, createdV2, reopened)).provideLayer(v2Layer)
      yield {
        val migrated     = after._1
        val createdV2    = after._2
        val reopened     = after._3
        val oldCompleted =
          reopened.find(_.id == seeded._2.id).exists(todo => todo.completed && todo.priority == "normal")
        val oldPending   =
          reopened.find(_.id == seeded._1.id).exists(todo => !todo.completed && todo.priority == "normal")
        val newTodo      =
          reopened.exists(todo => todo.id == createdV2.id && todo.priority == "high" && !todo.completed)

        assertTrue(
          report == TodoMigrationReport(TodoSchemaVersion.V1, TodoSchemaVersion.V2, migratedTodos = 2),
          migrated.size == 2,
          migrated.forall(_.priority == "normal"),
          reopened.size == 3,
          oldCompleted,
          oldPending,
          newTodo,
        )
      }).mapError(err => new RuntimeException(err.toString))
    }

  private def autoVersioningScenario(
    serde: NativeLocalSerde,
    extension: String,
  ): ZIO[Any, Throwable, TestResult] =
    withTempDirectory(s"todo-auto-versioning-${extension.replace(".", "")}") { dir =>
      val snapshotPath = dir.resolve(s"todo-auto-versioned$extension")
      val v1Layer      = TodoV1Service.layer(snapshotPath, serde)
      val autoV2Layer  = TodoV2Service.autoLayer(snapshotPath, serde)
      val plainV2Layer = TodoV2Service.layer(snapshotPath, serde)

      (for
        seeded   <- (for
                      first  <- TodoV1Service.add("auto migrate first", "docs")
                      second <- TodoV1Service.add("auto migrate second", "ops")
                      _      <- TodoV1Service.complete(first.id)
                      _      <- TodoV1Service.checkpoint
                    yield (first, second)).provideLayer(v1Layer)
        migrated <- TodoV2Service.list.provideLayer(autoV2Layer)
        after    <- (for
                      createdV2 <- TodoV2Service.add("post-migration todo", "high")
                      reopened  <- TodoV2Service.checkpointAndReload
                    yield (createdV2, reopened)).provideLayer(autoV2Layer)
        plain    <- TodoV2Service.list.provideLayer(plainV2Layer)
        envelope <- SnapshotCodec.load[NativeLocalSnapshotEnvelope](
                      snapshotPath
                    )(using SnapshotCodec.forSerde[NativeLocalSnapshotEnvelope](serde))
      yield {
        val createdV2 = after._1
        val reopened  = after._2

        assertTrue(
          migrated.size == 2,
          migrated.exists(todo => todo.id == seeded._1.id && todo.completed && todo.priority == "normal"),
          migrated.exists(todo => todo.id == seeded._2.id && !todo.completed && todo.priority == "normal"),
          reopened.size == 3,
          reopened.exists(todo => todo.id == createdV2.id && todo.priority == "high" && !todo.completed),
          plain.size == 3,
          plain.exists(_.id == createdV2.id),
          envelope.schemaVersion.contains(TodoSchemaVersion.V2.ordinal),
          envelope.migratedFromFingerprint.nonEmpty,
          envelope.migratedAtEpochMillis.nonEmpty,
        )
      }).mapError(err => new RuntimeException(err.toString))
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("TodoNativeLocalVersioning")(
      test("manual JSON snapshot migration upgrades v1 todos to v2 and keeps writes working") {
        versioningScenario(NativeLocalSerde.Json, ".json")
      },
      test("manual protobuf snapshot migration upgrades v1 todos to v2 and keeps writes working") {
        versioningScenario(NativeLocalSerde.Protobuf, ".pb")
      },
      test("auto JSON snapshot migration upgrades v1 todos on v2 startup and rewrites the snapshot") {
        autoVersioningScenario(NativeLocalSerde.Json, ".json")
      },
      test("auto protobuf snapshot migration upgrades v1 todos on v2 startup and rewrites the snapshot") {
        autoVersioningScenario(NativeLocalSerde.Protobuf, ".pb")
      },
    )
