package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.{ Files, Path }
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, ObjectStore, StorageLock }

object StorageLockSpec extends ZIOSpecDefault:
  given Tag[ConcurrentHashMap[String, Int]] = Tag.derived

  private val descriptor =
    RootDescriptor.concurrentMap[String, Int]("lock-root")

  private def deleteDirectory(path: Path): Unit =
    if Files.exists(path) then Files.walk(path).sorted(Comparator.reverseOrder()).forEach(Files.delete)

  private def lockLayer(path: Path) =
    ZLayer.succeed(EclipseStoreConfig.make(path).copy(rootDescriptors = Chunk.single(descriptor))) >>>
      EclipseStoreService.live >>>
      ObjectStore.live(descriptor) >>>
      StorageLock.live[ConcurrentHashMap[String, Int]]

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StorageLock")(
      test("many readers complete concurrently with correct values") {
        ZIO.scoped {
          for
            path <- ZIO.attemptBlocking(Files.createTempDirectory("storage-lock-readers"))
            _    <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer = lockLayer(path)
            env  <- layer.build
            _    <- StorageLock.writeLock[ConcurrentHashMap[String, Int], Unit](root =>
                      ZIO.succeed(root.put("count", 1)).unit
                    ).provideEnvironment(env)
            out  <- ZIO
                      .foreachPar(1 to 20)(_ =>
                        StorageLock.readLock[ConcurrentHashMap[String, Int], Int](root =>
                          ZIO.succeed(root.get("count"))
                        ).provideEnvironment(env)
                      )
                      .withParallelism(10)
          yield assertTrue(out.forall(_ == 1))
        }
      },
      test("writer waits for active readers and then proceeds exclusively") {
        ZIO.scoped {
          for
            path          <- ZIO.attemptBlocking(Files.createTempDirectory("storage-lock-writer"))
            _             <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer          = lockLayer(path)
            env           <- layer.build
            readerEntered <- Promise.make[Nothing, Unit]
            releaseReader <- Promise.make[Nothing, Unit]
            readerFiber   <- StorageLock
                               .readLock[ConcurrentHashMap[String, Int], Int] { root =>
                                 readerEntered.succeed(()) *> releaseReader.await.as(root.getOrDefault("count", 0))
                               }
                               .provideEnvironment(env)
                               .fork
            _             <- readerEntered.await
            writerFiber   <- StorageLock
                               .writeLock[ConcurrentHashMap[String, Int], Unit](root =>
                                 ZIO.succeed(root.put("count", 2)).unit
                               )
                               .provideEnvironment(env)
                               .fork
            writerPoll1   <- writerFiber.poll
            _             <- releaseReader.succeed(())
            _             <- readerFiber.join
            _             <- writerFiber.join
            finalValue    <- StorageLock
                               .readLock[ConcurrentHashMap[String, Int], Int](root =>
                                 ZIO.succeed(root.getOrDefault("count", 0))
                               )
                               .provideEnvironment(env)
          yield assertTrue(writerPoll1.isEmpty, finalValue == 2)
        }
      },
      test("optimisticUpdate retries on conflicts and commits a consistent final state") {
        ZIO.scoped {
          for
            path       <- ZIO.attemptBlocking(Files.createTempDirectory("storage-lock-optimistic"))
            _          <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer       = lockLayer(path)
            env        <- layer.build
            _          <- StorageLock.writeLock[ConcurrentHashMap[String, Int], Unit](root =>
                            ZIO.succeed(root.put("count", 0)).unit
                          ).provideEnvironment(env)
            _          <- ZIO
                            .foreachPar(1 to 25)(_ =>
                              StorageLock
                                .optimisticUpdate[ConcurrentHashMap[String, Int], Int](root =>
                                  ZIO.succeed(root.getOrDefault("count", 0))
                                )((root, current) =>
                                  ZIO.succeed(root.put("count", current + 1)).as(current + 1)
                                )
                                .provideEnvironment(env)
                            )
                            .withParallelism(8)
            finalValue <- StorageLock
                            .readLock[ConcurrentHashMap[String, Int], Int](root =>
                              ZIO.succeed(root.getOrDefault("count", 0))
                            )
                            .provideEnvironment(env)
          yield assertTrue(finalValue == 25)
        }
      },
      test("interrupted fibers waiting for a lock do not leak the lock") {
        ZIO.scoped {
          for
            path          <- ZIO.attemptBlocking(Files.createTempDirectory("storage-lock-interrupt"))
            _             <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer          = lockLayer(path)
            env           <- layer.build
            releaseWriter <- Promise.make[Nothing, Unit]
            writerFiber   <- StorageLock
                               .writeLock[ConcurrentHashMap[String, Int], Unit](_ => releaseWriter.await)
                               .provideEnvironment(env)
                               .fork
            waiter        <- StorageLock
                               .writeLock[ConcurrentHashMap[String, Int], Unit](root =>
                                 ZIO.succeed(root.put("count", 1)).unit
                               )
                               .provideEnvironment(env)
                               .fork
            _             <- TestClock.adjust(100.millis)
            _             <- waiter.interrupt
            _             <- releaseWriter.succeed(())
            _             <- writerFiber.join
            _             <- StorageLock
                               .writeLock[ConcurrentHashMap[String, Int], Unit](root =>
                                 ZIO.succeed(root.put("count", 2)).unit
                               )
                               .provideEnvironment(env)
            finalValue    <- StorageLock
                               .readLock[ConcurrentHashMap[String, Int], Int](root =>
                                 ZIO.succeed(root.getOrDefault("count", 0))
                               )
                               .provideEnvironment(env)
          yield assertTrue(finalValue == 2)
        }
      },
      test("failing writes roll the root back to its pre-mutation state") {
        ZIO.scoped {
          for
            path       <- ZIO.attemptBlocking(Files.createTempDirectory("storage-lock-rollback"))
            _          <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer       = lockLayer(path)
            env        <- layer.build
            _          <- StorageLock.writeLock[ConcurrentHashMap[String, Int], Unit](root =>
                            ZIO.succeed(root.put("count", 1)).unit
                          ).provideEnvironment(env)
            _          <- StorageLock
                            .writeLock[ConcurrentHashMap[String, Int], Unit](root =>
                              ZIO.succeed(root.put("count", 999)) *>
                                ZIO.fail(EclipseStoreError.StorageError("boom", None))
                            )
                            .provideEnvironment(env)
                            .either
            finalValue <- StorageLock
                            .readLock[ConcurrentHashMap[String, Int], Int](root =>
                              ZIO.succeed(root.getOrDefault("count", 0))
                            )
                            .provideEnvironment(env)
          yield assertTrue(finalValue == 1)
        }
      },
    )
