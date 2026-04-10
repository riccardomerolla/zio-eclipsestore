package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.file.Path

import zio.*
import zio.schema.Schema
import zio.stm.TRef

import io.github.riccardomerolla.zio.eclipsestore.config.{
  CorruptSnapshotPolicy,
  MissingSnapshotPolicy,
  NativeLocalSerde,
  NativeLocalStartupPolicy,
}
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.schema.SchemaIntrospection

final private case class NativeLocalState[Root](
  descriptor: RootDescriptor[Root],
  snapshotPath: Path,
  serde: NativeLocalSerde,
  startupPolicy: NativeLocalStartupPolicy,
  migrationRegistry: NativeLocalSnapshotMigrationRegistry[Root],
  rootRef: Ref[Root],
  rootTRef: TRef[Root],
  schemaVersionRef: Ref[Option[Int]],
  migrationProvenanceRef: Ref[Option[NativeLocalMigrationProvenance]],
  statusRef: Ref[LifecycleStatus],
  nativeStatusRef: Ref[NativeLocalStatus],
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
    for
      root       <- rootRef.get
      version    <- schemaVersionRef.get
      provenance <- migrationProvenanceRef.get
      _          <- SnapshotCodec.saveEnveloped(snapshotPath, root, descriptor.id, serde, version, provenance)
      _          <- updateSnapshotStatus()
    yield ()

  def loadSnapshot(default: => Root): IO[EclipseStoreError, Root] =
    loadSnapshotFrom(snapshotPath, default)

  def bootSnapshot(default: => Root): IO[EclipseStoreError, Root] =
    loadSnapshot(default).catchAll {
      case error @ EclipseStoreError.CorruptSnapshotError(_, _)    =>
        startupPolicy.corruptSnapshot match
          case CorruptSnapshotPolicy.StartFromEmpty => normalize(default)
          case CorruptSnapshotPolicy.Fail           => ZIO.fail(error)
      case error @ EclipseStoreError.PartialWriteError(_, _)       =>
        startupPolicy.corruptSnapshot match
          case CorruptSnapshotPolicy.StartFromEmpty => normalize(default)
          case CorruptSnapshotPolicy.Fail           => ZIO.fail(error)
      case error @ EclipseStoreError.IncompatibleSchemaError(_, _) =>
        startupPolicy.corruptSnapshot match
          case CorruptSnapshotPolicy.StartFromEmpty => normalize(default)
          case CorruptSnapshotPolicy.Fail           => ZIO.fail(error)
      case other                                                   => ZIO.fail(other)
    }

  def loadSnapshotFrom(path: Path, default: => Root): IO[EclipseStoreError, Root] =
    SnapshotCodec
      .loadEnvelopedOrElse(path, descriptor.id, serde, default, migrationRegistry)
      .flatMap { loaded =>
        normalize(loaded.value).tap { root =>
          schemaVersionRef.set(loaded.targetSchemaVersion) *>
            migrationProvenanceRef.set(loaded.migrationProvenance) *>
            updateLoadedStatus(loaded.loadedSchemaFingerprint) *>
            ZIO.when(loaded.rewriteRequired)(
              SnapshotCodec.saveEnveloped(
                snapshotPath,
                root,
                descriptor.id,
                serde,
                loaded.targetSchemaVersion,
                loaded.migrationProvenance,
              ) *> updateSnapshotStatus()
            )
        }
      }

  def updateLoadedStatus(loadedSchemaFingerprint: String): UIO[Unit] =
    nativeStatusRef.update(_.copy(lastLoadedSchemaFingerprint = loadedSchemaFingerprint))

  def updateSnapshotStatus(): IO[EclipseStoreError, Unit] =
    for
      now          <- Clock.instant
      snapshotSize <- ZIO
                        .attemptBlocking(if java.nio.file.Files.exists(snapshotPath) then
                          java.nio.file.Files.size(snapshotPath)
                        else 0L)
                        .mapError(cause =>
                          EclipseStoreError.StorageError(
                            s"Failed to inspect NativeLocal snapshot metadata at $snapshotPath",
                            Some(cause),
                          )
                        )
      lifecycle    <- statusRef.get
      _            <- nativeStatusRef.update(
                        _.copy(
                          lifecycle = lifecycle,
                          lastSuccessfulCheckpointAt = Some(now),
                          snapshotBytes = snapshotSize,
                        )
                      )
    yield ()

  def setLifecycleStatus(status: LifecycleStatus): UIO[Unit] =
    statusRef.set(status) *> nativeStatusRef.update(_.copy(lifecycle = status))

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
        _        <- state.setLifecycleStatus(LifecycleStatus.Running(now))
      yield LifecycleStatus.Running(now)
    }

  override def restart: IO[EclipseStoreError, LifecycleStatus] =
    state.gate.withPermit {
      for
        restartingAt <- Clock.instant
        _            <- state.setLifecycleStatus(LifecycleStatus.Restarting(restartingAt))
        reloaded     <- state.bootSnapshot(state.descriptor.initializer())
        _            <- state.rootRef.set(reloaded)
        _            <- state.rootTRef.set(reloaded).commit
        runningAt    <- Clock.instant
        _            <- state.setLifecycleStatus(LifecycleStatus.Running(runningAt))
      yield LifecycleStatus.Running(runningAt)
    }

  override def shutdown: IO[EclipseStoreError, LifecycleStatus] =
    state.gate.withPermit {
      for
        shuttingDownAt <- Clock.instant
        _              <- state.setLifecycleStatus(LifecycleStatus.ShuttingDown(shuttingDownAt))
        _              <- state.snapshotCurrentUnsafe
        _              <- state.setLifecycleStatus(LifecycleStatus.Stopped)
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

final private case class NativeLocalInspectorLive[Root](state: NativeLocalState[Root])
  extends NativeLocalInspector[Root]:
  override def status: UIO[NativeLocalStatus] =
    state.nativeStatusRef.get

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
    startupPolicy: NativeLocalStartupPolicy,
  ): ZLayer[Any, EclipseStoreError, ObjectStore[Root] & StorageOps[Root] & NativeLocalSTM[Root] & NativeLocalInspector[Root]] =
    given SnapshotCodec[Root] = SnapshotCodec.forSerde[Root](serde)

    ZLayer.scopedEnvironment {
      for
        presence               <- ZIO
                                    .attemptBlocking(java.nio.file.Files.exists(snapshotPath))
                                    .mapError(cause =>
                                      EclipseStoreError.InitializationError(
                                        s"Failed to inspect NativeLocal root snapshot at $snapshotPath",
                                        Some(cause),
                                      )
                                    )
        _                      <- ZIO.fail(
                                    EclipseStoreError.InitializationError(
                                      s"NativeLocal root ${descriptor.id} requires an existing snapshot at $snapshotPath",
                                      None,
                                    )
                                  ).when(!presence && startupPolicy.missingSnapshot == MissingSnapshotPolicy.RequireExistingSnapshot)
        loaded                 <- SnapshotCodec.loadEnvelopedOrElse(
                                    snapshotPath,
                                    descriptor.id,
                                    serde,
                                    descriptor.initializer(),
                                    migrationRegistry,
                                  ).catchAll {
                                    case error @ EclipseStoreError.CorruptSnapshotError(_, _)    =>
                                      startupPolicy.corruptSnapshot match
                                        case CorruptSnapshotPolicy.StartFromEmpty =>
                                          ZIO.succeed(
                                            SnapshotCodec.SnapshotLoadResult(
                                              value = descriptor.initializer(),
                                              rewriteRequired = false,
                                              loadedSchemaFingerprint = SchemaIntrospection.fingerprint(summon[Schema[Root]]),
                                            )
                                          )
                                        case CorruptSnapshotPolicy.Fail           => ZIO.fail(error)
                                    case error @ EclipseStoreError.PartialWriteError(_, _)       =>
                                      startupPolicy.corruptSnapshot match
                                        case CorruptSnapshotPolicy.StartFromEmpty =>
                                          ZIO.succeed(
                                            SnapshotCodec.SnapshotLoadResult(
                                              value = descriptor.initializer(),
                                              rewriteRequired = false,
                                              loadedSchemaFingerprint = SchemaIntrospection.fingerprint(summon[Schema[Root]]),
                                            )
                                          )
                                        case CorruptSnapshotPolicy.Fail           => ZIO.fail(error)
                                    case error @ EclipseStoreError.IncompatibleSchemaError(_, _) =>
                                      startupPolicy.corruptSnapshot match
                                        case CorruptSnapshotPolicy.StartFromEmpty =>
                                          ZIO.succeed(
                                            SnapshotCodec.SnapshotLoadResult(
                                              value = descriptor.initializer(),
                                              rewriteRequired = false,
                                              loadedSchemaFingerprint = SchemaIntrospection.fingerprint(summon[Schema[Root]]),
                                            )
                                          )
                                        case CorruptSnapshotPolicy.Fail           => ZIO.fail(error)
                                    case other                                                   => ZIO.fail(other)
                                  }
        initial                <- ZIO
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
        rootRef                <- Ref.make(initial)
        rootTRef               <- TRef.make(initial).commit
        schemaVersionRef       <- Ref.make(loaded.targetSchemaVersion)
        migrationProvenanceRef <- Ref.make(loaded.migrationProvenance)
        startedAt              <- Clock.instant
        statusRef              <- Ref.make[LifecycleStatus](LifecycleStatus.Running(startedAt))
        size                   <- ZIO
                                    .attemptBlocking(if java.nio.file.Files.exists(snapshotPath) then
                                      java.nio.file.Files.size(snapshotPath)
                                    else 0L)
                                    .orElseSucceed(0L)
        nativeStatusRef        <- Ref.make(
                                    NativeLocalStatus(
                                      lifecycle = LifecycleStatus.Running(startedAt),
                                      snapshotPath = snapshotPath,
                                      serde = serde,
                                      startupPolicy = startupPolicy,
                                      lastSuccessfulCheckpointAt = None,
                                      lastLoadedSchemaFingerprint = loaded.loadedSchemaFingerprint,
                                      snapshotBytes = size,
                                    )
                                  )
        gate                   <- Semaphore.make(1)
        state                   = NativeLocalState(
                                    descriptor,
                                    snapshotPath,
                                    serde,
                                    startupPolicy,
                                    migrationRegistry,
                                    rootRef,
                                    rootTRef,
                                    schemaVersionRef,
                                    migrationProvenanceRef,
                                    statusRef,
                                    nativeStatusRef,
                                    gate,
                                  )
        _                      <- ZIO.when(loaded.rewriteRequired)(
                                    SnapshotCodec.saveEnveloped(
                                      snapshotPath,
                                      initial,
                                      descriptor.id,
                                      serde,
                                      loaded.targetSchemaVersion,
                                      loaded.migrationProvenance,
                                    ) *> state.updateSnapshotStatus()
                                  )
        store                   = NativeLocalObjectStore(state)
        ops                     = NativeLocalStorageOps(state)
        stm                     = NativeLocalSTMLive(state)
        inspector               = NativeLocalInspectorLive(state)
      yield ZEnvironment[ObjectStore[Root], StorageOps[Root], NativeLocalSTM[Root], NativeLocalInspector[Root]](
        store,
        ops,
        stm,
        inspector,
      )
    }

  def live[Root: Tag: Schema](
    snapshotPath: Path,
    descriptor: RootDescriptor[Root],
    serde: NativeLocalSerde = NativeLocalSerde.Json,
    migrationRegistry: NativeLocalSnapshotMigrationRegistry[Root] = NativeLocalSnapshotMigrationRegistry.none[Root],
    startupPolicy: NativeLocalStartupPolicy = NativeLocalStartupPolicy.default,
  ): ZLayer[Any, EclipseStoreError, ObjectStore[Root] & StorageOps[Root]] =
    ZLayer.scopedEnvironment {
      allServices(snapshotPath, descriptor, serde, migrationRegistry, startupPolicy).build.map { env =>
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
    startupPolicy: NativeLocalStartupPolicy = NativeLocalStartupPolicy.default,
  ): ZLayer[Any, EclipseStoreError, ObjectStore[Root] & StorageOps[Root] & NativeLocalSTM[Root]] =
    ZLayer.scopedEnvironment {
      allServices(snapshotPath, descriptor, serde, migrationRegistry, startupPolicy).build.map { env =>
        ZEnvironment[ObjectStore[Root], StorageOps[Root], NativeLocalSTM[Root]](
          env.get[ObjectStore[Root]],
          env.get[StorageOps[Root]],
          env.get[NativeLocalSTM[Root]],
        )
      }
    }

  def liveWithInspector[Root: Tag: Schema](
    snapshotPath: Path,
    descriptor: RootDescriptor[Root],
    serde: NativeLocalSerde = NativeLocalSerde.Json,
    migrationRegistry: NativeLocalSnapshotMigrationRegistry[Root] = NativeLocalSnapshotMigrationRegistry.none[Root],
    startupPolicy: NativeLocalStartupPolicy = NativeLocalStartupPolicy.default,
  ): ZLayer[Any, EclipseStoreError, ObjectStore[Root] & StorageOps[Root] & NativeLocalInspector[Root]] =
    ZLayer.scopedEnvironment {
      allServices(snapshotPath, descriptor, serde, migrationRegistry, startupPolicy).build.map { env =>
        ZEnvironment[ObjectStore[Root], StorageOps[Root], NativeLocalInspector[Root]](
          env.get[ObjectStore[Root]],
          env.get[StorageOps[Root]],
          env.get[NativeLocalInspector[Root]],
        )
      }
    }

  def stm[Root: Tag: Schema](
    snapshotPath: Path,
    descriptor: RootDescriptor[Root],
    serde: NativeLocalSerde = NativeLocalSerde.Json,
    migrationRegistry: NativeLocalSnapshotMigrationRegistry[Root] = NativeLocalSnapshotMigrationRegistry.none[Root],
    startupPolicy: NativeLocalStartupPolicy = NativeLocalStartupPolicy.default,
  ): ZLayer[Any, EclipseStoreError, NativeLocalSTM[Root]] =
    ZLayer.scopedEnvironment {
      allServices(snapshotPath, descriptor, serde, migrationRegistry, startupPolicy).build.map { env =>
        ZEnvironment[NativeLocalSTM[Root]](env.get[NativeLocalSTM[Root]])
      }
    }
