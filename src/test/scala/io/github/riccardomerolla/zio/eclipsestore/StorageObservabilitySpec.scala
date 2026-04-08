package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.observability.{ StorageEvent, StorageMetrics, StorageObservability }
import io.github.riccardomerolla.zio.eclipsestore.service.{
  LifecycleStatus,
  ObjectStore,
  StorageLock,
  StorageOps,
  Transaction,
}

object StorageObservabilitySpec extends ZIOSpecDefault:
  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StorageObservability")(
      test("instrumented object store preserves behavior and increments persisted counters") {
        for
          metrics  <- StorageMetrics.inMemory.build.map(_.get[StorageMetrics])
          store     = StorageObservability.instrumentObjectStore(
                        FakeObjectStore(),
                        metrics,
                        StorageObservability.Settings(slowOperationThreshold = 5.millis),
                      )
          env       = ZEnvironment[ObjectStore[ConcurrentHashMap[String, Int]]](store)
          result   <- ObjectStore
                        .transact[ConcurrentHashMap[String, Int], Int](
                          Transaction.effect(root =>
                            ZIO.succeed {
                              root.put("a", 1)
                              root.size()
                            }
                          )
                        )
                        .provideEnvironment(env)
          snapshot <- StorageMetrics.snapshot.provideEnvironment(ZEnvironment(metrics))
        yield assertTrue(
          result == 1,
          snapshot.persistedObjects == 1L,
          snapshot.operationCounts.get("object-store.transact.success").contains(1L),
        )
      },
      test("slow storage operations emit duration metrics and slow events") {
        for
          metrics  <- StorageMetrics.inMemory.build.map(_.get[StorageMetrics])
          ops       = StorageObservability.instrumentStorageOps(
                        FakeStorageOps(),
                        metrics,
                        StorageObservability.Settings(slowOperationThreshold = 10.millis),
                      )
          env       = ZEnvironment[StorageOps[ConcurrentHashMap[String, String]]](ops)
          _        <- StorageOps
                        .backup[ConcurrentHashMap[String, String]](Path.of("/tmp/backup"))
                        .provideEnvironment(env)
          _        <- StorageOps.housekeep[ConcurrentHashMap[String, String]].provideEnvironment(env)
          snapshot <- StorageMetrics.snapshot.provideEnvironment(ZEnvironment(metrics))
        yield assertTrue(
          snapshot.latestDurationGaugeMillis.contains("storage-ops.backup"),
          snapshot.latestDurationGaugeMillis.contains("storage-ops.housekeep"),
          snapshot.events.exists {
            case StorageEvent.SlowOperationDetected("storage-ops.backup", _, _)    => true
            case StorageEvent.SlowOperationDetected("storage-ops.housekeep", _, _) => true
            case _                                                                 => false
          },
        )
      },
      test("failed operations preserve typed errors and are recorded") {
        for
          metrics  <- StorageMetrics.inMemory.build.map(_.get[StorageMetrics])
          ops       = StorageObservability.instrumentStorageOps(
                        FakeStorageOps(),
                        metrics,
                        StorageObservability.Settings(slowOperationThreshold = 10.millis),
                      )
          env       = ZEnvironment[StorageOps[ConcurrentHashMap[String, String]]](ops)
          result   <- StorageOps
                        .exportTo[ConcurrentHashMap[String, String]](Path.of("/tmp/export"))
                        .provideEnvironment(env)
                        .either
          snapshot <- StorageMetrics.snapshot.provideEnvironment(ZEnvironment(metrics))
        yield assertTrue(
          result == Left(EclipseStoreError.StorageError("export failed", None)),
          snapshot.failedOperations == 1L,
          snapshot.operationCounts.get("storage-ops.export.failure").contains(1L),
        )
      },
      test("heavy write-lock contention records contention events") {
        ZIO.scoped {
          for
            metrics  <- StorageMetrics.inMemory.build.map(_.get[StorageMetrics])
            lock      =
              StorageObservability.instrumentStorageLock(
                FakeStorageLock(),
                metrics,
                StorageObservability.Settings(slowOperationThreshold = 10.millis, lockContentionThreshold = 10.millis),
              )
            env       = ZEnvironment[StorageLock[ConcurrentHashMap[String, Int]]](lock)
            _        <- StorageLock
                          .writeLock[ConcurrentHashMap[String, Int], Unit](_ => TestClock.adjust(50.millis))
                          .provideEnvironment(env)
            snapshot <- StorageMetrics.snapshot.provideEnvironment(ZEnvironment(metrics))
          yield assertTrue(
            snapshot.lockContentionEvents >= 1L,
            snapshot.events.exists {
              case StorageEvent.LockContentionDetected("storage-lock.write", _, _) => true
              case _                                                               => false
            },
          )
        }
      },
    )

final private case class FakeObjectStore() extends ObjectStore[ConcurrentHashMap[String, Int]]:
  override val descriptor: RootDescriptor[ConcurrentHashMap[String, Int]] =
    RootDescriptor.concurrentMap[String, Int]("obs-root")
  private val root                                                        = new ConcurrentHashMap[String, Int]()

  override def load: IO[EclipseStoreError, ConcurrentHashMap[String, Int]] =
    ZIO.succeed(root)

  override def storeSubgraph(subgraph: AnyRef): IO[EclipseStoreError, Unit] =
    ZIO.unit

  override def storeRoot: IO[EclipseStoreError, Unit] =
    ZIO.unit

  override def checkpoint: IO[EclipseStoreError, Unit] =
    ZIO.unit

  override def transact[A](transaction: Transaction[ConcurrentHashMap[String, Int], A]): IO[EclipseStoreError, A] =
    transaction.run(root)

final private case class FakeStorageOps() extends StorageOps[ConcurrentHashMap[String, String]]:
  override val descriptor: RootDescriptor[ConcurrentHashMap[String, String]] =
    RootDescriptor.concurrentMap[String, String]("ops-root")
  private val root                                                           = new ConcurrentHashMap[String, String]()

  override def load: IO[EclipseStoreError, ConcurrentHashMap[String, String]] =
    ZIO.succeed(root)

  override val status: UIO[LifecycleStatus] =
    Clock.instant.map(LifecycleStatus.Running(_))

  override def checkpoint: IO[EclipseStoreError, LifecycleStatus] =
    status

  override def backup(target: Path, includeConfig: Boolean = true): IO[EclipseStoreError, LifecycleStatus] =
    TestClock.adjust(20.millis) *> status

  override def exportTo(target: Path): IO[EclipseStoreError, LifecycleStatus] =
    ZIO.fail(EclipseStoreError.StorageError("export failed", None))

  override def importFrom(source: Path): IO[EclipseStoreError, LifecycleStatus] =
    status

  override def restoreFrom(source: Path): IO[EclipseStoreError, LifecycleStatus] =
    status

  override def restart: IO[EclipseStoreError, LifecycleStatus] =
    status

  override def shutdown: IO[EclipseStoreError, LifecycleStatus] =
    ZIO.succeed(LifecycleStatus.Stopped)

  override def housekeep: IO[EclipseStoreError, LifecycleStatus] =
    TestClock.adjust(25.millis) *> status

  override def scheduleCheckpoints(schedule: Schedule[Any, Any, Any]): URIO[Scope, Fiber.Runtime[Nothing, Unit]] =
    ZIO.unit.forkScoped

final private case class FakeStorageLock() extends StorageLock[ConcurrentHashMap[String, Int]]:
  private val root = new ConcurrentHashMap[String, Int]()

  override def readLock[A](effect: ConcurrentHashMap[String, Int] => IO[EclipseStoreError, A])
    : IO[EclipseStoreError, A] =
    effect(root)

  override def writeLock[A](effect: ConcurrentHashMap[String, Int] => IO[EclipseStoreError, A])
    : IO[EclipseStoreError, A] =
    effect(root)

  override def optimisticUpdate[A](
    prepare: ConcurrentHashMap[String, Int] => IO[EclipseStoreError, A]
  )(
    commit: (ConcurrentHashMap[String, Int], A) => IO[EclipseStoreError, A],
    maxRetries: Int = 8,
  ): IO[EclipseStoreError, A] =
    for
      prepared <- prepare(root)
      result   <- commit(root, prepared)
    yield result
