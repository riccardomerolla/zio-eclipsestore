package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.Files

import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.service.{ LocalRepo, NativeLocal, StorageOps }

object LocalRepoSpec extends ZIOSpecDefault:

  final case class TodoEntry(title: String, completed: Boolean)
  final case class TodoRegistry(entries: Map[String, TodoEntry])

  given Schema[TodoEntry]    = DeriveSchema.gen[TodoEntry]
  given Schema[TodoRegistry] = DeriveSchema.gen[TodoRegistry]
  given Tag[TodoRegistry]    = Tag.derived

  private val descriptor =
    RootDescriptor.fromSchema[TodoRegistry](
      id = "todo-registry",
      initializer = () => TodoRegistry(Map.empty),
    )

  private def layer(path: java.nio.file.Path) =
    NativeLocal.live(path, descriptor) >>> LocalRepo.fromMap[TodoRegistry, String, TodoEntry](_.entries) {
      (root, entries) =>
        root.copy(entries = entries)
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("LocalRepo")(
      test("upserts, loads, lists, and deletes entries in a NativeLocal root") {
        ZIO.scoped {
          for
            path                  <- ZIO.attemptBlocking(Files.createTempFile("local-repo", ".json"))
            _                     <- ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore
            _                     <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
            repo                   = layer(path)
            result                <- (for
                                       _    <- LocalRepo.upsert("todo-1", TodoEntry("write helper", completed = false))
                                       _    <- LocalRepo.upsert("todo-2", TodoEntry("checkpoint root", completed = true))
                                       one  <- LocalRepo.get[String, TodoEntry]("todo-1")
                                       all  <- LocalRepo.entries[String, TodoEntry]
                                       gone <- LocalRepo.delete[String, TodoEntry]("todo-1")
                                       miss <- LocalRepo.get[String, TodoEntry]("todo-1")
                                     yield (one, all, gone, miss)).provideLayer(repo)
            (one, all, gone, miss) = result
          yield assertTrue(
            one.contains(TodoEntry("write helper", completed = false)),
            all.keySet == Set("todo-1", "todo-2"),
            gone.contains(TodoEntry("write helper", completed = false)),
            miss.isEmpty,
          )
        }
      },
      test("persists repo updates across checkpoint and restart") {
        ZIO.scoped {
          for
            path    <- ZIO.attemptBlocking(Files.createTempFile("local-repo-restart", ".json"))
            _       <- ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore
            _       <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
            services = NativeLocal.live(path, descriptor)
            repo     = services >>> LocalRepo.fromMap[TodoRegistry, String, TodoEntry](_.entries) { (root, entries) =>
                         root.copy(entries = entries)
                       }
            out     <- (for
                         _   <- LocalRepo.upsert("todo-1", TodoEntry("survive restart", completed = false))
                         _   <- StorageOps.checkpoint[TodoRegistry]
                         _   <- StorageOps.restart[TodoRegistry]
                         out <- LocalRepo.entries[String, TodoEntry]
                       yield out).provideLayer(repo ++ services)
          yield assertTrue(
            out == Map("todo-1" -> TodoEntry("survive restart", completed = false))
          )
        }
      },
    )
