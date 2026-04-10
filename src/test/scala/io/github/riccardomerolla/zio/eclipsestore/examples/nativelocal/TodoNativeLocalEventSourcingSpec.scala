package io.github.riccardomerolla.zio.eclipsestore.examples.nativelocal

import java.nio.file.{ Files, Path }
import java.util.UUID

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde

object TodoNativeLocalEventSourcingSpec extends ZIOSpecDefault:

  private def withTempDirectory[A](prefix: String)(use: Path => ZIO[Any, Throwable, A]): ZIO[Any, Throwable, A] =
    ZIO.scoped {
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory(prefix))) { path =>
        ZIO.attemptBlocking {
          if Files.exists(path) then
            Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
        }.ignore
      }.flatMap(use)
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("TodoNativeLocalEventSourcing")(
      test("accepted commands append journal entries and keep the snapshot aligned with replay") {
        withTempDirectory("todo-native-local-event-sourcing") { dir =>
          val baseDir = dir.resolve("todo-eventing")
          val layer   = TodoEventSourcedService.layer(baseDir)

          (for
            first    <- TodoEventSourcedService.add("map the article", "high")
            _        <- TodoEventSourcedService.add("document the shell", "normal")
            _        <- TodoEventSourcedService.complete(first.id)
            journal  <- TodoEventSourcedService.journal
            snapshot <- TodoEventSourcedService.currentState
            replayed <- TodoEventSourcedService.replayedState
          yield assertTrue(
            journal.map(_.revision.value) == Chunk(1L, 2L, 3L),
            snapshot == replayed,
            snapshot.items.size == 2,
            snapshot.items.exists(todo => todo.id == first.id && todo.completed),
            snapshot.items.exists(todo => todo.title == "document the shell" && !todo.completed),
          )).provideLayer(layer).mapError(err => new RuntimeException(err.toString))
        }
      },
      test("rejected commands keep the persisted journal and snapshot unchanged") {
        withTempDirectory("todo-native-local-event-sourcing-failure") { dir =>
          val baseDir   = dir.resolve("todo-eventing")
          val layer     = TodoEventSourcedService.layer(baseDir)
          val missingId = TodoId(UUID.randomUUID())

          (for
            created       <- TodoEventSourcedService.add("persist one todo", "low")
            beforeJournal <- TodoEventSourcedService.journal
            beforeState   <- TodoEventSourcedService.currentState
            failure       <- TodoEventSourcedService.complete(missingId).either
            afterJournal  <- TodoEventSourcedService.journal
            afterState    <- TodoEventSourcedService.currentState
          yield assertTrue(
            created.title == "persist one todo",
            failure.isLeft,
            beforeJournal == afterJournal,
            beforeState == afterState,
            afterState.items.size == 1,
            afterState.items.exists(todo => todo.id == created.id && !todo.completed),
          )).provideLayer(layer).mapError(err => new RuntimeException(err.toString))
        }
      },
      test(
        "each accepted command is durable across a fresh reopen because the journal append is the durability boundary"
      ) {
        withTempDirectory("todo-native-local-event-sourcing-reopen") { dir =>
          val baseDir = dir.resolve("todo-eventing")
          val layer   = TodoEventSourcedService.layer(baseDir)

          (for
            first       <- (for
                             first <- TodoEventSourcedService.add("persist the journal", "high")
                             _     <- TodoEventSourcedService.complete(first.id)
                           yield first).provideLayer(layer)
            afterReopen <- TodoEventSourcedService.currentState.provideLayer(layer.fresh)
            journal     <- TodoEventSourcedService.journal.provideLayer(layer.fresh)
            replayed    <- TodoEventSourcedService.replayedState.provideLayer(layer.fresh)
            snapshot    <- TodoEventSourcedService.latestSnapshot.provideLayer(layer.fresh)
          yield assertTrue(
            afterReopen == replayed,
            journal.size == 2,
            snapshot.isEmpty,
            afterReopen.items.exists(todo => todo.id == first.id && todo.completed),
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
      test("protobuf eventing supports the same event-sourced reopen flow") {
        withTempDirectory("todo-native-local-event-sourcing-protobuf") { dir =>
          val baseDir = dir.resolve("todo-eventing")
          val layer   = TodoEventSourcedService.layer(baseDir, NativeLocalSerde.Protobuf)

          (for
            _           <- TodoEventSourcedService.add("persist protobuf journal", "high").provideLayer(layer)
            afterReopen <- TodoEventSourcedService.currentState.provideLayer(layer.fresh)
            journal     <- TodoEventSourcedService.journal.provideLayer(layer.fresh)
          yield assertTrue(
            afterReopen.items.map(_.title) == Chunk("persist protobuf journal"),
            journal.size == 1,
            journal.map(_.revision.value) == Chunk(1L),
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
    )
