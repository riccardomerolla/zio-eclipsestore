package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.file.Path

import zio.*
import zio.schema.Schema

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

final private case class NativeLocalState[Root](
  descriptor: RootDescriptor[Root],
  snapshotPath: Path,
  rootRef: Ref[Root],
  statusRef: Ref[LifecycleStatus],
  gate: Semaphore,
)(using val codec: SnapshotCodec[Root]
):
  def normalize(root: Root): IO[EclipseStoreError, Root] =
    ZIO
      .attempt {
        val migrated = descriptor.migrate(root)
        descriptor.onLoad(migrated)
        migrated
      }
      .mapError(cause =>
        EclipseStoreError.ResourceError(s"Failed to prepare NativeLocal root ${descriptor.id}", Some(cause))
      )

  def snapshotCurrent: IO[EclipseStoreError, Unit] =
    gate.withPermit {
      snapshotCurrentUnsafe
    }

  def snapshotCurrentUnsafe: IO[EclipseStoreError, Unit] =
    rootRef.get.flatMap(root => SnapshotCodec.save(snapshotPath, root))

  def loadSnapshot(default: => Root): IO[EclipseStoreError, Root] =
    SnapshotCodec.loadOrElse(snapshotPath, default).flatMap(normalize)

final private case class NativeLocalObjectStore[Root](state: NativeLocalState[Root]) extends ObjectStore[Root]:
  override def descriptor: RootDescriptor[Root] =
    state.descriptor

  override def load: IO[EclipseStoreError, Root] =
    state.rootRef.get

  override def storeSubgraph(subgraph: AnyRef): IO[EclipseStoreError, Unit] =
    state.snapshotCurrent

  override def storeRoot: IO[EclipseStoreError, Unit] =
    state.snapshotCurrent

  override def checkpoint: IO[EclipseStoreError, Unit] =
    state.snapshotCurrent

  override def transact[A](transaction: Transaction[Root, A]): IO[EclipseStoreError, A] =
    state.gate.withPermit {
      for
        root   <- state.rootRef.get
        result <- transaction.run(root)
        _      <- state.snapshotCurrentUnsafe
      yield result
    }

  override def replace(root: Root): IO[EclipseStoreError, Unit] =
    state.gate.withPermit {
      for
        normalized <- state.normalize(root)
        _          <- state.rootRef.set(normalized)
      yield ()
    }

  override def modify[A](f: Root => IO[EclipseStoreError, (A, Root)]): IO[EclipseStoreError, A] =
    state.gate.withPermit {
      for
        current            <- state.rootRef.get
        (result, nextRoot) <- f(current)
        normalizedNextRoot <- state.normalize(nextRoot)
        _                  <- state.rootRef.set(normalizedNextRoot)
      yield result
    }

final private case class NativeLocalStorageOps[Root](state: NativeLocalState[Root]) extends StorageOps[Root]:
  override def descriptor: RootDescriptor[Root] =
    state.descriptor

  override def load: IO[EclipseStoreError, Root] =
    state.rootRef.get

  override def status: UIO[LifecycleStatus] =
    state.statusRef.get

  override def checkpoint: IO[EclipseStoreError, LifecycleStatus] =
    state.snapshotCurrent *> status

  override def backup(target: Path, includeConfig: Boolean = true): IO[EclipseStoreError, LifecycleStatus] =
    state.snapshotCurrent *> SnapshotCodec.copy(state.snapshotPath, target) *> status

  override def exportTo(target: Path): IO[EclipseStoreError, LifecycleStatus] =
    state.snapshotCurrent *> SnapshotCodec.copy(state.snapshotPath, target) *> status

  override def importFrom(source: Path): IO[EclipseStoreError, LifecycleStatus] =
    restoreFrom(source)

  override def restoreFrom(source: Path): IO[EclipseStoreError, LifecycleStatus] =
    state.gate.withPermit {
      for
        restored <- SnapshotCodec.load(source)(using state.codec).flatMap(state.normalize)
        _        <- state.rootRef.set(restored)
        _        <- state.snapshotCurrentUnsafe
        now      <- Clock.instant
        _        <- state.statusRef.set(LifecycleStatus.Running(now))
      yield LifecycleStatus.Running(now)
    }

  override def restart: IO[EclipseStoreError, LifecycleStatus] =
    state.gate.withPermit {
      for
        restartingAt <- Clock.instant
        _            <- state.statusRef.set(LifecycleStatus.Restarting(restartingAt))
        reloaded     <- state.loadSnapshot(state.descriptor.initializer())
        _            <- state.rootRef.set(reloaded)
        runningAt    <- Clock.instant
        _            <- state.statusRef.set(LifecycleStatus.Running(runningAt))
      yield LifecycleStatus.Running(runningAt)
    }

  override def shutdown: IO[EclipseStoreError, LifecycleStatus] =
    state.gate.withPermit {
      for
        shuttingDownAt <- Clock.instant
        _              <- state.statusRef.set(LifecycleStatus.ShuttingDown(shuttingDownAt))
        _              <- state.snapshotCurrentUnsafe
        _              <- state.statusRef.set(LifecycleStatus.Stopped)
      yield LifecycleStatus.Stopped
    }

  override def housekeep: IO[EclipseStoreError, LifecycleStatus] =
    checkpoint

  override def scheduleCheckpoints(schedule: Schedule[Any, Any, Any]): URIO[Scope, Fiber.Runtime[Nothing, Unit]] =
    checkpoint
      .ignore
      .repeat(schedule)
      .unit
      .forkScoped

object NativeLocal:
  def live[Root: Tag: Schema](
    snapshotPath: Path,
    descriptor: RootDescriptor[Root],
  ): ZLayer[Any, EclipseStoreError, ObjectStore[Root] & StorageOps[Root]] =
    given SnapshotCodec[Root] = SnapshotCodec.json[Root]

    ZLayer.scopedEnvironment {
      for
        loaded    <- SnapshotCodec.loadOrElse(snapshotPath, descriptor.initializer())
        initial   <- ZIO
                       .attempt {
                         val migrated = descriptor.migrate(loaded)
                         descriptor.onLoad(migrated)
                         migrated
                       }
                       .mapError(cause =>
                         EclipseStoreError.InitializationError(
                           s"Failed to initialize NativeLocal root ${descriptor.id}",
                           Some(cause),
                         )
                       )
        rootRef   <- Ref.make(initial)
        startedAt <- Clock.instant
        statusRef <- Ref.make[LifecycleStatus](LifecycleStatus.Running(startedAt))
        gate      <- Semaphore.make(1)
        state      = NativeLocalState(descriptor, snapshotPath, rootRef, statusRef, gate)
        store      = NativeLocalObjectStore(state)
        ops        = NativeLocalStorageOps(state)
      yield ZEnvironment[ObjectStore[Root], StorageOps[Root]](
        store,
        ops,
      )
    }
