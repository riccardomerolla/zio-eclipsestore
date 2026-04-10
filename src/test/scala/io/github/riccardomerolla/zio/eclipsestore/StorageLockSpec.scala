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
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, ObjectStore, StorageLock, Transaction }

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
            env           <- memoryLockLayer.build
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
            _             <- ZIO.yieldNow.repeatN(32)
            _             <- waiter.interruptFork
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
      } @@ TestAspect.timeout(5.seconds),
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

  private val memoryLockLayer: ULayer[StorageLock[ConcurrentHashMap[String, Int]]] =
    ZLayer.succeed[ObjectStore[ConcurrentHashMap[String, Int]]](FakeLockObjectStore()) >>>
      StorageLock.live[ConcurrentHashMap[String, Int]]

final private case class FakeLockObjectStore() extends ObjectStore[ConcurrentHashMap[String, Int]]:
  override val descriptor: RootDescriptor[ConcurrentHashMap[String, Int]] =
    RootDescriptor.concurrentMap[String, Int]("lock-root-memory")
  private val root                                                        = new ConcurrentHashMap[String, Int]()

  override def load: IO[EclipseStoreError, ConcurrentHashMap[String, Int]] =
    ZIO.succeed(root)

  override def replace(root: ConcurrentHashMap[String, Int]): IO[EclipseStoreError, Unit] =
    ZIO.succeed {
      this.root.clear()
      this.root.putAll(root)
    }

  override def modify[A](
    f: ConcurrentHashMap[String, Int] => IO[EclipseStoreError, (A, ConcurrentHashMap[String, Int])]
  ): IO[EclipseStoreError, A] =
    f(root).flatMap { case (result, updated) => replace(updated).as(result) }

  override def storeSubgraph(subgraph: AnyRef): IO[EclipseStoreError, Unit] =
    ZIO.unit

  override def storeRoot: IO[EclipseStoreError, Unit] =
    ZIO.unit

  override def checkpoint: IO[EclipseStoreError, Unit] =
    ZIO.unit

  override def transact[A](transaction: Transaction[ConcurrentHashMap[String, Int], A]): IO[EclipseStoreError, A] =
    transaction.run(root)
