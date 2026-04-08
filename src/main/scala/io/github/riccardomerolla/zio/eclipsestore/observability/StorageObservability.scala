package io.github.riccardomerolla.zio.eclipsestore.observability

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{
  LifecycleStatus,
  ObjectStore,
  StorageLock,
  StorageOps,
  Transaction,
}

object StorageObservability:
  final case class Settings(
    slowOperationThreshold: Duration = 100.millis,
    lockContentionThreshold: Duration = 50.millis,
  )

  def objectStore[Root: Tag](settings: Settings = Settings())
    : ZLayer[ObjectStore[Root] & StorageMetrics, Nothing, ObjectStore[Root]] =
    ZLayer.fromZIO {
      for
        store   <- ZIO.service[ObjectStore[Root]]
        metrics <- ZIO.service[StorageMetrics]
      yield instrumentObjectStore(store, metrics, settings)
    }

  def storageOps[Root: Tag](settings: Settings = Settings())
    : ZLayer[StorageOps[Root] & StorageMetrics, Nothing, StorageOps[Root]] =
    ZLayer.fromZIO {
      for
        ops     <- ZIO.service[StorageOps[Root]]
        metrics <- ZIO.service[StorageMetrics]
      yield instrumentStorageOps(ops, metrics, settings)
    }

  def storageLock[Root: Tag](settings: Settings = Settings())
    : ZLayer[StorageLock[Root] & StorageMetrics, Nothing, StorageLock[Root]] =
    ZLayer.fromZIO {
      for
        lock    <- ZIO.service[StorageLock[Root]]
        metrics <- ZIO.service[StorageMetrics]
      yield instrumentStorageLock(lock, metrics, settings)
    }

  def instrumentObjectStore[Root](
    underlying: ObjectStore[Root],
    metrics: StorageMetrics,
    settings: Settings = Settings(),
  ): ObjectStore[Root] =
    InstrumentedObjectStore(underlying, metrics, settings)

  def instrumentStorageOps[Root](
    underlying: StorageOps[Root],
    metrics: StorageMetrics,
    settings: Settings = Settings(),
  ): StorageOps[Root] =
    InstrumentedStorageOps(underlying, metrics, settings)

  def instrumentStorageLock[Root](
    underlying: StorageLock[Root],
    metrics: StorageMetrics,
    settings: Settings = Settings(),
  ): StorageLock[Root] =
    InstrumentedStorageLock(underlying, metrics, settings)

  private[observability] def logAndRecord(metrics: StorageMetrics, event: StorageEvent): UIO[Unit] =
    metrics.record(event) *>
      ZIO.logAnnotate("storage.operation", event.operationName) {
        event match
          case _: StorageEvent.OperationSucceeded => ZIO.logInfo(event.message)
          case _: StorageEvent.OperationFailed    => ZIO.logError(event.message)
          case _: StorageEvent.SlowOperationDetected |
               _: StorageEvent.LockContentionDetected =>
            ZIO.logWarning(event.message)
      }

  private[observability] def observe[A](
    metrics: StorageMetrics,
    settings: Settings,
    operation: String,
    persistedObjects: Int = 0,
    contentionThreshold: Option[Duration] = None,
    detail: Option[String] = None,
  )(
    effect: IO[EclipseStoreError, A]
  ): IO[EclipseStoreError, A] =
    for
      started <- Clock.nanoTime
      exit    <- effect.exit
      ended   <- Clock.nanoTime
      duration = (ended - started).nanos
      _       <- exit match
                   case Exit.Success(_)     =>
                     logAndRecord(metrics, StorageEvent.OperationSucceeded(operation, duration, persistedObjects, detail))
                   case Exit.Failure(cause) =>
                     cause.failureOption match
                       case Some(error: EclipseStoreError) =>
                         logAndRecord(metrics, StorageEvent.OperationFailed(operation, duration, error))
                       case _                              =>
                         ZIO.unit
      _       <- ZIO.when(duration >= settings.slowOperationThreshold) {
                   logAndRecord(
                     metrics,
                     StorageEvent.SlowOperationDetected(operation, duration, settings.slowOperationThreshold),
                   )
                 }
      _       <- ZIO.foreachDiscard(contentionThreshold)(threshold =>
                   ZIO.when(duration >= threshold) {
                     logAndRecord(metrics, StorageEvent.LockContentionDetected(operation, duration, threshold))
                   }
                 )
      result  <- ZIO.done(exit)
    yield result

final private case class InstrumentedObjectStore[Root](
  underlying: ObjectStore[Root],
  metrics: StorageMetrics,
  settings: StorageObservability.Settings,
) extends ObjectStore[Root]:
  override def descriptor: RootDescriptor[Root] = underlying.descriptor

  override def load: IO[EclipseStoreError, Root] =
    StorageObservability.observe(metrics, settings, "object-store.load")(underlying.load)

  override def storeSubgraph(subgraph: AnyRef): IO[EclipseStoreError, Unit] =
    StorageObservability.observe(metrics, settings, "object-store.store-subgraph", persistedObjects = 1)(
      underlying.storeSubgraph(subgraph)
    )

  override def storeRoot: IO[EclipseStoreError, Unit] =
    StorageObservability.observe(metrics, settings, "object-store.store-root", persistedObjects = 1)(
      underlying.storeRoot
    )

  override def checkpoint: IO[EclipseStoreError, Unit] =
    StorageObservability.observe(metrics, settings, "object-store.checkpoint", persistedObjects = 1)(
      underlying.checkpoint
    )

  override def transact[A](transaction: Transaction[Root, A]): ZIO[Any, EclipseStoreError, A] =
    StorageObservability.observe(metrics, settings, "object-store.transact", persistedObjects = 1)(
      underlying.transact(transaction)
    )

final private case class InstrumentedStorageOps[Root](
  underlying: StorageOps[Root],
  metrics: StorageMetrics,
  settings: StorageObservability.Settings,
) extends StorageOps[Root]:
  override def descriptor: RootDescriptor[Root] = underlying.descriptor

  override def load: IO[EclipseStoreError, Root] =
    StorageObservability.observe(metrics, settings, "storage-ops.load")(underlying.load)

  override def status: ZIO[Any, Nothing, LifecycleStatus] =
    underlying.status

  override def checkpoint: ZIO[Any, EclipseStoreError, LifecycleStatus] =
    StorageObservability.observe(metrics, settings, "storage-ops.checkpoint", persistedObjects = 1)(
      underlying.checkpoint
    )

  override def backup(target: java.nio.file.Path, includeConfig: Boolean = true)
    : ZIO[Any, EclipseStoreError, LifecycleStatus] =
    StorageObservability.observe(metrics, settings, "storage-ops.backup", detail = Some(target.toString))(
      underlying.backup(target, includeConfig)
    )

  override def exportTo(target: java.nio.file.Path): ZIO[Any, EclipseStoreError, LifecycleStatus] =
    StorageObservability.observe(metrics, settings, "storage-ops.export", detail = Some(target.toString))(
      underlying.exportTo(target)
    )

  override def importFrom(source: java.nio.file.Path): ZIO[Any, EclipseStoreError, LifecycleStatus] =
    StorageObservability.observe(
      metrics,
      settings,
      "storage-ops.import",
      persistedObjects = 1,
      detail = Some(source.toString),
    )(
      underlying.importFrom(source)
    )

  override def restoreFrom(source: java.nio.file.Path): ZIO[Any, EclipseStoreError, LifecycleStatus] =
    StorageObservability.observe(
      metrics,
      settings,
      "storage-ops.restore",
      persistedObjects = 1,
      detail = Some(source.toString),
    )(
      underlying.restoreFrom(source)
    )

  override def restart: ZIO[Any, EclipseStoreError, LifecycleStatus] =
    StorageObservability.observe(metrics, settings, "storage-ops.restart")(underlying.restart)

  override def shutdown: ZIO[Any, EclipseStoreError, LifecycleStatus] =
    StorageObservability.observe(metrics, settings, "storage-ops.shutdown")(underlying.shutdown)

  override def housekeep: ZIO[Any, EclipseStoreError, LifecycleStatus] =
    StorageObservability.observe(metrics, settings, "storage-ops.housekeep")(underlying.housekeep)

  override def scheduleCheckpoints(schedule: Schedule[Any, Any, Any])
    : ZIO[Scope, Nothing, Fiber.Runtime[Nothing, Unit]] =
    underlying.scheduleCheckpoints(schedule)

final private case class InstrumentedStorageLock[Root](
  underlying: StorageLock[Root],
  metrics: StorageMetrics,
  settings: StorageObservability.Settings,
) extends StorageLock[Root]:
  override def readLock[A](effect: Root => IO[EclipseStoreError, A]): ZIO[Any, EclipseStoreError, A] =
    StorageObservability.observe(
      metrics,
      settings,
      "storage-lock.read",
      contentionThreshold = Some(settings.lockContentionThreshold),
    )(underlying.readLock(effect))

  override def writeLock[A](effect: Root => IO[EclipseStoreError, A]): ZIO[Any, EclipseStoreError, A] =
    StorageObservability.observe(
      metrics,
      settings,
      "storage-lock.write",
      persistedObjects = 1,
      contentionThreshold = Some(settings.lockContentionThreshold),
    )(underlying.writeLock(effect))

  override def optimisticUpdate[A](
    prepare: Root => IO[EclipseStoreError, A]
  )(
    commit: (Root, A) => IO[EclipseStoreError, A],
    maxRetries: Int = 8,
  ): ZIO[Any, EclipseStoreError, A] =
    StorageObservability.observe(
      metrics,
      settings,
      "storage-lock.optimistic-update",
      persistedObjects = 1,
      contentionThreshold = Some(settings.lockContentionThreshold),
    )(underlying.optimisticUpdate(prepare)(commit, maxRetries))
