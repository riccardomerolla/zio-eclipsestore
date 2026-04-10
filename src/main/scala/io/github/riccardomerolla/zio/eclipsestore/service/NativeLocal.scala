package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.file.Path

import zio.*
import zio.schema.Schema
import zio.stm.TRef

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

final private case class NativeLocalState[Root](
  descriptor: RootDescriptor[Root],
  snapshotPath: Path,
  serde: NativeLocalSerde,
  migrationRegistry: NativeLocalSnapshotMigrationRegistry[Root],
  rootRef: Ref[Root],
  rootTRef: TRef[Root],
  statusRef: Ref[LifecycleStatus],
  gate: Semaphore,
)(using
  val codec: SnapshotCodec[Root],
  val schema: Schema[Root],
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
    rootRef.get.flatMap(root => SnapshotCodec.saveEnveloped(snapshotPath, root, descriptor.id, serde))

  def loadSnapshot(default: => Root): IO[EclipseStoreError, Root] =
    loadSnapshotFrom(snapshotPath, default)

  def loadSnapshotFrom(path: Path, default: => Root): IO[EclipseStoreError, Root] =
    SnapshotCodec
      .loadEnvelopedOrElse(path, descriptor.id, serde, default, migrationRegistry)
      .flatMap { loaded =>
        normalize(loaded.value).tap(root =>
          ZIO.when(loaded.rewriteRequired)(SnapshotCodec.saveEnveloped(snapshotPath, root, descriptor.id, serde))
        )
      }

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
        _          <- state.rootTRef.set(normalized).commit
      yield ()
    }

  override def modify[A](f: Root => IO[EclipseStoreError, (A, Root)]): IO[EclipseStoreError, A] =
    state.gate.withPermit {
      for
        current            <- state.rootRef.get
        (result, nextRoot) <- f(current)
        normalizedNextRoot <- state.normalize(nextRoot)
        _                  <- state.rootRef.set(normalizedNextRoot)
        _                  <- state.rootTRef.set(normalizedNextRoot).commit
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
        restored <- state.loadSnapshotFrom(source, state.descriptor.initializer())
        _        <- state.rootRef.set(restored)
        _        <- state.rootTRef.set(restored).commit
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
        _            <- state.rootTRef.set(reloaded).commit
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

final private case class NativeLocalSTMLive[Root](state: NativeLocalState[Root]) extends NativeLocalSTM[Root]:
  override def atomically[A](effect: TRef[Root] => zio.stm.STM[EclipseStoreError, A]): IO[EclipseStoreError, A] =
    state.gate.withPermit {
      for
        current <- state.rootRef.get
        _       <- state.rootTRef.set(current).commit
        result  <- effect(state.rootTRef).commit
        staged  <- state.rootTRef.get.commit
        next    <- state.normalize(staged).catchAll { error =>
                     state.rootTRef.set(current).commit *> ZIO.fail(error)
                   }
        _       <- state.rootTRef.set(next).commit
        _       <- state.rootRef.set(next)
      yield result
    }

  override def snapshot: UIO[Root] =
    state.rootRef.get

object NativeLocal:
  private def allServices[Root: Tag: Schema](
    snapshotPath: Path,
    descriptor: RootDescriptor[Root],
    serde: NativeLocalSerde,
    migrationRegistry: NativeLocalSnapshotMigrationRegistry[Root],
  ): ZLayer[Any, EclipseStoreError, ObjectStore[Root] & StorageOps[Root] & NativeLocalSTM[Root]] =
    given SnapshotCodec[Root] = SnapshotCodec.forSerde[Root](serde)

    ZLayer.scopedEnvironment {
      for
        loaded    <- SnapshotCodec.loadEnvelopedOrElse(
                       snapshotPath,
                       descriptor.id,
                       serde,
                       descriptor.initializer(),
                       migrationRegistry,
                     )
        initial   <- ZIO
                       .attempt {
                         val migrated = descriptor.migrate(loaded.value)
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
        rootTRef  <- TRef.make(initial).commit
        startedAt <- Clock.instant
        statusRef <- Ref.make[LifecycleStatus](LifecycleStatus.Running(startedAt))
        gate      <- Semaphore.make(1)
        state      = NativeLocalState(descriptor, snapshotPath, serde, migrationRegistry, rootRef, rootTRef, statusRef, gate)
        _         <- ZIO.when(loaded.rewriteRequired)(SnapshotCodec.saveEnveloped(snapshotPath, initial, descriptor.id, serde))
        store      = NativeLocalObjectStore(state)
        ops        = NativeLocalStorageOps(state)
        stm        = NativeLocalSTMLive(state)
      yield ZEnvironment[ObjectStore[Root], StorageOps[Root], NativeLocalSTM[Root]](
        store,
        ops,
        stm,
      )
    }

  def live[Root: Tag: Schema](
    snapshotPath: Path,
    descriptor: RootDescriptor[Root],
    serde: NativeLocalSerde = NativeLocalSerde.Json,
    migrationRegistry: NativeLocalSnapshotMigrationRegistry[Root] = NativeLocalSnapshotMigrationRegistry.none[Root],
  ): ZLayer[Any, EclipseStoreError, ObjectStore[Root] & StorageOps[Root]] =
    ZLayer.scopedEnvironment {
      allServices(snapshotPath, descriptor, serde, migrationRegistry).build.map { env =>
        ZEnvironment[ObjectStore[Root], StorageOps[Root]](
          env.get[ObjectStore[Root]],
          env.get[StorageOps[Root]],
        )
      }
    }

  def liveWithSTM[Root: Tag: Schema](
    snapshotPath: Path,
    descriptor: RootDescriptor[Root],
    serde: NativeLocalSerde = NativeLocalSerde.Json,
    migrationRegistry: NativeLocalSnapshotMigrationRegistry[Root] = NativeLocalSnapshotMigrationRegistry.none[Root],
  ): ZLayer[Any, EclipseStoreError, ObjectStore[Root] & StorageOps[Root] & NativeLocalSTM[Root]] =
    allServices(snapshotPath, descriptor, serde, migrationRegistry)

  def stm[Root: Tag: Schema](
    snapshotPath: Path,
    descriptor: RootDescriptor[Root],
    serde: NativeLocalSerde = NativeLocalSerde.Json,
    migrationRegistry: NativeLocalSnapshotMigrationRegistry[Root] = NativeLocalSnapshotMigrationRegistry.none[Root],
  ): ZLayer[Any, EclipseStoreError, NativeLocalSTM[Root]] =
    ZLayer.scopedEnvironment {
      allServices(snapshotPath, descriptor, serde, migrationRegistry).build.map { env =>
        ZEnvironment[NativeLocalSTM[Root]](env.get[NativeLocalSTM[Root]])
      }
    }
