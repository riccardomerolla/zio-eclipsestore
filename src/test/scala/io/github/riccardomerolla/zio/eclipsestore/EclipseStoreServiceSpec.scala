package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.{ Files, Path }
import java.util.Comparator

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.domain.{ Query, RootDescriptor }
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, LifecycleCommand, LifecycleStatus }

object EclipseStoreServiceSpec extends ZIOSpecDefault:

  private def deleteDirectory(path: Path): Unit =
    if Files.exists(path) then Files.walk(path).sorted(Comparator.reverseOrder()).forEach(Files.delete)

  private def liveLayer(path: Path) =
    ZLayer.succeed(EclipseStoreConfig.make(path)) >>> EclipseStoreService.live

  private def sqliteLayer(path: Path) =
    ZLayer.succeed(EclipseStoreConfig(StorageTarget.Sqlite(path))) >>> EclipseStoreService.live

  override def spec: Spec[Environment & (TestEnvironment & Scope), Any] =
    suite("EclipseStoreService")(
      test("stores and retrieves a single value") {
        for
          _      <- EclipseStoreService.put("key1", "value1")
          result <- EclipseStoreService.get[String, String]("key1")
        yield assertTrue(result.contains("value1"))
      },
      test("returns None for non-existent key") {
        for result <- EclipseStoreService.get[String, String]("nonexistent")
        yield assertTrue(result.isEmpty)
      },
      test("stores and retrieves multiple values") {
        for
          _ <- EclipseStoreService.put("user:1", "Alice")
          _ <- EclipseStoreService.put("user:2", "Bob")
          _ <- EclipseStoreService.put("user:3", "Charlie")

          user1 <- EclipseStoreService.get[String, String]("user:1")
          user2 <- EclipseStoreService.get[String, String]("user:2")
          user3 <- EclipseStoreService.get[String, String]("user:3")
        yield assertTrue(
          user1.contains("Alice") &&
          user2.contains("Bob") &&
          user3.contains("Charlie")
        )
      },
      test("deletes a value") {
        for
          _            <- EclipseStoreService.put("toDelete", "value")
          beforeDelete <- EclipseStoreService.get[String, String]("toDelete")
          _            <- EclipseStoreService.delete("toDelete")
          afterDelete  <- EclipseStoreService.get[String, String]("toDelete")
        yield assertTrue(
          beforeDelete.contains("value") &&
          afterDelete.isEmpty
        )
      },
      test("retrieves all values") {
        for
          _ <- EclipseStoreService.put("a", 1)
          _ <- EclipseStoreService.put("b", 2)
          _ <- EclipseStoreService.put("c", 3)

          allValues <- EclipseStoreService.getAll[Int]
        yield assertTrue(
          allValues.toSet == Set(1, 2, 3)
        )
      },
      test("executes multiple queries in batch") {
        for
          _ <- EclipseStoreService.put("batch:1", "First")
          _ <- EclipseStoreService.put("batch:2", "Second")
          _ <- EclipseStoreService.put("batch:3", "Third")

          queries  = List(
                       Query.Get[String, String]("batch:1"),
                       Query.Get[String, String]("batch:2"),
                       Query.Get[String, String]("batch:3"),
                     )
          results <- EclipseStoreService.executeMany(queries)
        yield assertTrue(
          results == List(Some("First"), Some("Second"), Some("Third"))
        )
      },
      test("handles mixed batchable and non-batchable queries") {
        for
          _ <- EclipseStoreService.put("item:1", "A")
          _ <- EclipseStoreService.put("item:2", "B")

          queries  = List(
                       Query.Get[String, String]("item:1"), // batchable
                       Query.GetAllValues[String](),        // non-batchable
                       Query.Get[String, String]("item:2"), // batchable
                     )
          results <- EclipseStoreService.executeMany(queries)

          getValue1    = results.head.asInstanceOf[Option[String]]
          getAllValues = results(1).asInstanceOf[List[String]]
          getValue2    = results(2).asInstanceOf[Option[String]]
        yield assertTrue(
          getValue1.contains("A") &&
          getAllValues.toSet.contains("A") &&
          getAllValues.toSet.contains("B") &&
          getValue2.contains("B")
        )
      },
      test("overwrites existing value") {
        for
          _      <- EclipseStoreService.put("key", "original")
          before <- EclipseStoreService.get[String, String]("key")
          _      <- EclipseStoreService.put("key", "updated")
          after  <- EclipseStoreService.get[String, String]("key")
        yield assertTrue(
          before.contains("original") &&
          after.contains("updated")
        )
      },
      test("stores batch entries and streams them") {
        for
          _        <- EclipseStoreService.putAll(List("stream:1" -> "one", "stream:2" -> "two"))
          streamed <- EclipseStoreService.streamValues[String].runCollect
        yield assertTrue(streamed.toSet.contains("one") && streamed.toSet.contains("two"))
      },
      test("exposes typed roots") {
        val descriptor = RootDescriptor(
          id = "users",
          initializer = () => ListBuffer.empty[String],
        )
        for
          root     <- EclipseStoreService.root(descriptor)
          _        <- ZIO.succeed(root.addOne("alice"))
          hydrated <- EclipseStoreService.root(descriptor)
        yield assertTrue(hydrated.contains("alice"))
      },
      test("executes custom queries against root context") {
        val countQuery = Query.Custom[Int](
          operation = "count",
          run = ctx => ctx.container.ensure(RootDescriptor.concurrentMap[Any, Any]("kv-root")).size(),
        )
        for
          _     <- EclipseStoreService.put("ctx:1", 1)
          count <- EclipseStoreService.execute(countQuery)
        yield assertTrue(count >= 1)
      },
      test("lifecycle maintenance updates status") {
        for
          _       <- EclipseStoreService.maintenance(LifecycleCommand.Checkpoint)
          running <- EclipseStoreService.status
          _       <- EclipseStoreService.maintenance(LifecycleCommand.Shutdown)
          stopped <- EclipseStoreService.status
        yield assertTrue(
          running match
            case LifecycleStatus.Running(_) => true
            case _                          => false,
          stopped == LifecycleStatus.Stopped,
        )
      },
      test("backup directory configuration applies without error") {
        ZIO.scoped {
          for
            path   <- ZIO.attemptBlocking(Files.createTempDirectory("backup-config"))
            _      <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer   = ZLayer.succeed(
                        EclipseStoreConfig(
                          storageTarget = StorageTarget.FileSystem(path),
                          backupDirectory = Some(path.resolve("backup-dir")),
                          backupTruncationDirectory = Some(path.resolve("backup-trunc")),
                          backupDeletionDirectory = Some(path.resolve("backup-delete")),
                          backupExternalProperties = Map("backup-filesystem.sql.sqlite.url" -> "jdbc:sqlite:test"),
                        )
                      ) >>>
                        EclipseStoreService.live
            result <- EclipseStoreService.put("bk", "val").provideLayer(layer).either
          yield assertTrue(result.isRight)
        }
      },
      test("maintenance backup copies files into target directory") {
        ZIO.scoped {
          for
            primary     <- ZIO.attemptBlocking(Files.createTempDirectory("primary-store"))
            backup      <- ZIO.attemptBlocking(Files.createTempDirectory("backup-store"))
            _           <- ZIO.addFinalizer(ZIO.attemptBlocking {
                             deleteDirectory(primary)
                             deleteDirectory(backup)
                           }.orDie)
            layer        = liveLayer(primary)
            _           <- (for
                             _ <- EclipseStoreService.put("bk-key", "bk-val")
                             _ <- EclipseStoreService.maintenance(LifecycleCommand.Backup(backup))
                           yield ()).provideLayer(layer)
            backupFiles <- ZIO.attemptBlocking {
                             Files.walk(backup).iterator().asScala.count(Files.isRegularFile(_))
                           }
          yield assertTrue(backupFiles > 0)
        }
      },
      test("fails initialization when storage path is a file") {
        ZIO.scoped {
          for
            tmpFile <- ZIO.attemptBlocking(Files.createTempFile("eclipsestore-file-path", ".tmp"))
            _       <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(tmpFile)).orDie)
            layer    = liveLayer(tmpFile)
            result  <- EclipseStoreService.put("bad", "path").provideLayer(layer).either
          yield assertTrue(result.isLeft)
        }
      },
      test("exports and imports storage contents") {
        ZIO.scoped {
          for
            src      <- ZIO.attemptBlocking(Files.createTempDirectory("store-src"))
            exported <- ZIO.attemptBlocking(Files.createTempDirectory("store-export"))
            dest     <- ZIO.attemptBlocking(Files.createTempDirectory("store-dest"))
            _        <- ZIO.addFinalizer(
                          ZIO.attemptBlocking {
                            deleteDirectory(src)
                            deleteDirectory(exported)
                            deleteDirectory(dest)
                          }.orDie
                        )
            srcLayer  = liveLayer(src)
            destLayer = liveLayer(dest)
            _        <- (for
                          _ <- EclipseStoreService.put("k1", "v1")
                          _ <- EclipseStoreService.exportData(exported)
                        yield ()).provideLayer(srcLayer)
            restored <- (for
                          _ <- EclipseStoreService.importData(exported)
                          v <- EclipseStoreService.get[String, String]("k1")
                        yield v).provideLayer(destLayer)
          yield assertTrue(restored.contains("v1"))
        }
      },
      test("sqlite storage target persists values") {
        ZIO.scoped {
          for
            dbDir <- ZIO.attemptBlocking(Files.createTempDirectory("sqlite-target"))
            _     <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(dbDir)).orDie)
            layer  = sqliteLayer(dbDir)
            _     <- (for _ <- EclipseStoreService.put("user:1", "alice")
                     yield ()).provideLayer(layer)
            read  <- (for v <- EclipseStoreService.get[String, String]("user:1")
                     yield v).provideLayer(layer)
          yield assertTrue(read.contains("alice"))
        }
      },
      test("filesystem storage survives close + reopen with checkpointed kv-root data") {
        ZIO.scoped {
          for
            path <- ZIO.attemptBlocking(Files.createTempDirectory("filesystem-reopen"))
            _    <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            rawLayer = ZLayer.succeed(EclipseStoreConfig.make(path)) >>> EclipseStoreService.live.fresh
            _    <- (for
                      _ <- EclipseStoreService.put("key1", "value1")
                      _ <- EclipseStoreService.put("key2", "value2")
                      _ <- EclipseStoreService.maintenance(LifecycleCommand.Checkpoint)
                    yield ()).provideLayer(rawLayer)
            out  <- (for
                      _    <- EclipseStoreService.reloadRoots
                      keys <- EclipseStoreService.streamKeys[String].runCollect
                      v1   <- EclipseStoreService.get[String, String]("key1")
                      v2   <- EclipseStoreService.get[String, String]("key2")
                    yield (keys, v1, v2)).provideLayer(rawLayer)
          yield assertTrue(
            out._1.toSet == Set("key1", "key2"),
            out._2.contains("value1"),
            out._3.contains("value2"),
          )
        }
      },
    ).provide(EclipseStoreService.inMemory)
