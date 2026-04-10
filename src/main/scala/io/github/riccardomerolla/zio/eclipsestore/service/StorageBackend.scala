package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.file.Files

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.Schema

import io.github.riccardomerolla.zio.eclipsestore.config.{ BackendConfig, EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

/** Layer-managed backend selection for the EclipseStore substrate. */
trait StorageBackend:
  def config: BackendConfig
  def storageTarget: StorageTarget

final case class StorageBackendLive(
  config: BackendConfig,
  storageTarget: StorageTarget,
) extends StorageBackend

object StorageBackend:
  def live: ZLayer[BackendConfig, EclipseStoreError, StorageBackend] =
    ZLayer.scoped {
      for
        config  <- ZIO.service[BackendConfig]
        backend <- fromConfig(config)
      yield backend
    }

  def configLayer(
    configure: EclipseStoreConfig => EclipseStoreConfig = identity
  ): ZLayer[BackendConfig, EclipseStoreError, EclipseStoreConfig] =
    live >>> ZLayer.fromZIO {
      for
        backend <- ZIO.service[StorageBackend]
      yield configure(EclipseStoreConfig(storageTarget = backend.storageTarget))
    }

  def service(
    configure: EclipseStoreConfig => EclipseStoreConfig = identity
  ): ZLayer[BackendConfig, EclipseStoreError, EclipseStoreService] =
    configLayer(configure) >>> EclipseStoreService.live >>> serializedService

  def rootServices[Root: Tag: Schema](
    descriptor: RootDescriptor[Root],
    configure: EclipseStoreConfig => EclipseStoreConfig = identity,
  ): ZLayer[BackendConfig, EclipseStoreError, ObjectStore[Root] & StorageOps[Root]] =
    ZLayer.scopedEnvironment {
      for
        config       <- ZIO.service[BackendConfig]
        selectedLayer = config match
                          case nativeLocal: BackendConfig.NativeLocal =>
                            NativeLocal.live(nativeLocal.snapshotPath, descriptor, nativeLocal.serde)
                          case other                                  =>
                            ZLayer.succeed(other) >>> service(
                              configure
                            ) >>> (ObjectStore.live(descriptor) ++ StorageOps.live(descriptor))
        built        <- selectedLayer.build
      yield ZEnvironment[ObjectStore[Root], StorageOps[Root]](
        built.get[ObjectStore[Root]],
        built.get[StorageOps[Root]],
      )
    }

  private val serializedService: ZLayer[EclipseStoreService, Nothing, EclipseStoreService] =
    ZLayer.fromZIO {
      for
        underlying <- ZIO.service[EclipseStoreService]
        semaphore  <- Semaphore.make(1)
      yield new EclipseStoreService:
        private def serialized[A](effect: => IO[EclipseStoreError, A]): IO[EclipseStoreError, A] =
          semaphore.withPermit(effect)

        override def execute[A](query: io.github.riccardomerolla.zio.eclipsestore.domain.Query[A])
          : IO[EclipseStoreError, A] =
          serialized(underlying.execute(query))

        override def executeMany[A](queries: List[io.github.riccardomerolla.zio.eclipsestore.domain.Query[A]])
          : IO[EclipseStoreError, List[A]] =
          serialized(underlying.executeMany(queries))

        override def get[K, V](key: K): IO[EclipseStoreError, Option[V]] =
          serialized(underlying.get(key))

        override def put[K, V](key: K, value: V): IO[EclipseStoreError, Unit] =
          serialized(underlying.put(key, value))

        override def putAll[K, V](entries: Iterable[(K, V)]): IO[EclipseStoreError, Unit] =
          serialized(underlying.putAll(entries))

        override def delete[K](key: K): IO[EclipseStoreError, Unit] =
          serialized(underlying.delete(key))

        override def getAll[V]: IO[EclipseStoreError, List[V]] =
          serialized(underlying.getAll)

        override def streamValues[V]: zio.stream.ZStream[Any, EclipseStoreError, V] =
          zio.stream.ZStream.fromZIO(getAll[V]).flatMap(zio.stream.ZStream.fromIterable(_))

        override def streamKeys[K]: zio.stream.ZStream[Any, EclipseStoreError, K] =
          zio.stream.ZStream.fromZIO(serialized(
            underlying.execute(io.github.riccardomerolla.zio.eclipsestore.domain.Query.GetAllKeys[K]())
          ))
            .flatMap(zio.stream.ZStream.fromIterable(_))

        override def root[A](descriptor: io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor[A])
          : IO[EclipseStoreError, A] =
          serialized(underlying.root(descriptor))

        override def persist[A](value: A): IO[EclipseStoreError, Unit] =
          serialized(underlying.persist(value))

        override def persistAll[A](values: Iterable[A]): IO[EclipseStoreError, Unit] =
          serialized(underlying.persistAll(values))

        override val reloadRoots: IO[EclipseStoreError, Unit] =
          serialized(underlying.reloadRoots)

        override def maintenance(command: LifecycleCommand): IO[EclipseStoreError, LifecycleStatus] =
          serialized(underlying.maintenance(command))

        override def status: UIO[LifecycleStatus] =
          underlying.status

        override def exportData(target: java.nio.file.Path): IO[EclipseStoreError, Unit] =
          serialized(underlying.exportData(target))

        override def importData(source: java.nio.file.Path): IO[EclipseStoreError, Unit] =
          serialized(underlying.importData(source))
    }

  private def fromConfig(config: BackendConfig): ZIO[Scope, EclipseStoreError, StorageBackend] =
    config match
      case inMemory: BackendConfig.InMemory     =>
        ZIO.acquireRelease(createTempDirectory(inMemory.prefix)) { path =>
          deleteDirectory(path).ignore
        }.map(path => StorageBackendLive(inMemory, StorageTarget.FileSystem(path)))
      case fileSystem: BackendConfig.FileSystem =>
        ensureDirectory(fileSystem.path).as(StorageBackendLive(fileSystem, StorageTarget.FileSystem(fileSystem.path)))
      case mmap: BackendConfig.MemoryMapped     =>
        ensureDirectory(mmap.path).as(StorageBackendLive(mmap, StorageTarget.MemoryMapped(mmap.path)))
      case sqlite: BackendConfig.Sqlite         =>
        ensureDirectory(sqlite.path).as(
          StorageBackendLive(
            sqlite,
            StorageTarget.Sqlite(
              path = sqlite.path,
              storageName = sqlite.storageName,
              connectionString = sqlite.connectionString,
              busyTimeoutMs = sqlite.busyTimeoutMs,
              cacheSizeKb = sqlite.cacheSizeKb,
              pageSizeBytes = sqlite.pageSizeBytes,
              journalMode = sqlite.journalMode,
              synchronousMode = sqlite.synchronousMode,
            ),
          )
        )
      case other                                =>
        ZIO.fromEither(other.toStorageTarget).map(target => StorageBackendLive(other, target))

  private def createTempDirectory(prefix: String): ZIO[Any, EclipseStoreError, java.nio.file.Path] =
    ZIO
      .attempt(Files.createTempDirectory(prefix))
      .mapError(cause => EclipseStoreError.BackendError(s"Failed to create backend directory for $prefix", Some(cause)))

  private def ensureDirectory(path: java.nio.file.Path): ZIO[Any, EclipseStoreError, Unit] =
    ZIO
      .attemptBlocking(Files.createDirectories(path))
      .unit
      .mapError(cause => EclipseStoreError.BackendError(s"Failed to initialize backend path $path", Some(cause)))

  private def deleteDirectory(path: java.nio.file.Path): Task[Unit] =
    ZIO.attemptBlocking {
      if Files.exists(path) then
        Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
    }
