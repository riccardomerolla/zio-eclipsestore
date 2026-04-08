package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.{ Files, Path }
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.service.{ LifecycleStatus, StorageOps }

object StorageOpsSpec extends ZIOSpecDefault:

  private val rootDescriptor =
    RootDescriptor.concurrentMap[String, String]("storage-ops-root")

  private def deleteDirectory(path: Path): Unit =
    if Files.exists(path) then Files.walk(path).sorted(Comparator.reverseOrder()).forEach(Files.delete)

  private def opsLayer(path: Path) =
    ZLayer.succeed(EclipseStoreConfig.make(path).copy(rootDescriptors = Chunk.single(rootDescriptor))) >>>
      io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService.live >>>
      StorageOps.live(rootDescriptor)

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StorageOps")(
      test("backup then restore reconstructs the original typed root graph") {
        ZIO.scoped {
          for
            primary   <- ZIO.attemptBlocking(Files.createTempDirectory("storage-ops-primary"))
            backup    <- ZIO.attemptBlocking(Files.createTempDirectory("storage-ops-backup"))
            restored  <- ZIO.attemptBlocking(Files.createTempDirectory("storage-ops-restored"))
            _         <- ZIO.addFinalizer(
                           ZIO.attemptBlocking {
                             deleteDirectory(primary)
                             deleteDirectory(backup)
                             deleteDirectory(restored)
                           }.orDie
                         )
            _         <- (for
                           root <- StorageOps.load[ConcurrentHashMap[String, String]]
                           _    <- ZIO.succeed {
                                     root.put("user-1", "alice")
                                     root.put("user-2", "bob")
                                   }
                           _    <- StorageOps.checkpoint[ConcurrentHashMap[String, String]]
                           _    <- StorageOps.backup[ConcurrentHashMap[String, String]](backup)
                         yield ()).provideLayer(opsLayer(primary))
            roundTrip <- (for
                           _    <- StorageOps.restoreFrom[ConcurrentHashMap[String, String]](backup)
                           root <- StorageOps.load[ConcurrentHashMap[String, String]]
                         yield Map(
                           "user-1" -> Option(root.get("user-1")),
                           "user-2" -> Option(root.get("user-2")),
                         )).provideLayer(opsLayer(restored))
          yield assertTrue(roundTrip == Map("user-1" -> Some("alice"), "user-2" -> Some("bob")))
        }
      },
      test("export/import keeps the root accessible through Scala-native APIs") {
        ZIO.scoped {
          for
            primary  <- ZIO.attemptBlocking(Files.createTempDirectory("storage-ops-export-primary"))
            exported <- ZIO.attemptBlocking(Files.createTempDirectory("storage-ops-exported"))
            restored <- ZIO.attemptBlocking(Files.createTempDirectory("storage-ops-import-restored"))
            _        <- ZIO.addFinalizer(
                          ZIO.attemptBlocking {
                            deleteDirectory(primary)
                            deleteDirectory(exported)
                            deleteDirectory(restored)
                          }.orDie
                        )
            _        <- (for
                          root <- StorageOps.load[ConcurrentHashMap[String, String]]
                          _    <- ZIO.succeed {
                                    root.put("user-3", "carol")
                                    root.put("user-4", "dave")
                                  }
                          _    <- StorageOps.checkpoint[ConcurrentHashMap[String, String]]
                          _    <- StorageOps.exportTo[ConcurrentHashMap[String, String]](exported)
                        yield ()).provideLayer(opsLayer(primary))
            imported <- (for
                          _    <- StorageOps.importFrom[ConcurrentHashMap[String, String]](exported)
                          root <- StorageOps.load[ConcurrentHashMap[String, String]]
                        yield Map(
                          "user-3" -> Option(root.get("user-3")),
                          "user-4" -> Option(root.get("user-4")),
                        )).provideLayer(opsLayer(restored))
          yield assertTrue(imported == Map("user-3" -> Some("carol"), "user-4" -> Some("dave")))
        }
      },
      test("scheduled checkpoints persist in-memory root mutations after simulated time advances") {
        ZIO.scoped {
          for
            path   <- ZIO.attemptBlocking(Files.createTempDirectory("storage-ops-scheduled"))
            _      <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            before <- ZIO.scoped {
                        for
                          env      <- opsLayer(path).build
                          scopeEnv <- ZIO.environment[Scope]
                          _        <- StorageOps
                                        .scheduleCheckpoints[ConcurrentHashMap[String, String]](Schedule.spaced(1.hour))
                                        .provideEnvironment(scopeEnv ++ env)
                          root     <- StorageOps.load[ConcurrentHashMap[String, String]].provideEnvironment(env)
                          _        <- ZIO.succeed(root.put("scheduled", "true"))
                          before   <- StorageOps
                                        .load[ConcurrentHashMap[String, String]]
                                        .provideLayer(opsLayer(path).fresh)
                                        .map(root => Option(root.get("scheduled")))
                                        .either
                          _        <- TestClock.adjust(1.hour)
                        yield before
                      }
            after  <- StorageOps
                        .load[ConcurrentHashMap[String, String]]
                        .provideLayer(opsLayer(path).fresh)
                        .map(root => Option(root.get("scheduled")))
          yield assertTrue(before.isLeft, after.contains("true"))
        }
      },
      test("housekeeping checkpoints and reloads the configured root") {
        ZIO.scoped {
          for
            path     <- ZIO.attemptBlocking(Files.createTempDirectory("storage-ops-housekeep"))
            _        <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            status   <- ZIO.scoped {
                          for
                            env    <- opsLayer(path).build
                            root   <- StorageOps.load[ConcurrentHashMap[String, String]].provideEnvironment(env)
                            _      <- ZIO.succeed(root.put("retained", "yes"))
                            status <- StorageOps.housekeep[ConcurrentHashMap[String, String]].provideEnvironment(env)
                          yield status
                        }
            reopened <- StorageOps
                          .load[ConcurrentHashMap[String, String]]
                          .provideLayer(opsLayer(path).fresh)
                          .map(root => Option(root.get("retained")))
          yield assertTrue(
            status match
              case LifecycleStatus.Running(_) => true
              case _                          => false,
            reopened.contains("yes"),
          )
        }
      },
    )
