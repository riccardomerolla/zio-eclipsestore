package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.*
import scala.util.Using

import zio.*
import zio.stream.ZStream

import io.github.riccardomerolla.zio.eclipsestore.config.{
  CompressionSetting,
  EclipseStoreConfig,
  StoragePerformanceConfig,
}
import io.github.riccardomerolla.zio.eclipsestore.domain.{ Query, RootContainer, RootContext, RootDescriptor }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import org.eclipse.serializer.persistence.types.{ Persister, Storer }
import org.eclipse.store.storage.embedded.types.{ EmbeddedStorage, EmbeddedStorageManager }

enum LifecycleCommand:
  case Checkpoint
  case Backup(target: Path, includeConfig: Boolean = true)
  case Export(target: Path)
  case Import(source: Path)
  case Restart
  case Shutdown

enum LifecycleStatus:
  case Starting(startedAt: Instant)
  case Running(startedAt: Instant)
  case Restarting(startedAt: Instant)
  case ShuttingDown(startedAt: Instant)
  case Stopped
  case Failed(message: String)

/** Service for type-safe access to EclipseStore */
trait EclipseStoreService:
  /** Executes a single query */
  def execute[A](query: Query[A]): IO[EclipseStoreError, A]

  /** Executes multiple queries, batching where possible and running un-batchable queries in parallel */
  def executeMany[A](queries: List[Query[A]]): IO[EclipseStoreError, List[A]]

  /** Gets a value by key */
  def get[K, V](key: K): IO[EclipseStoreError, Option[V]]

  /** Stores a value with a key */
  def put[K, V](key: K, value: V): IO[EclipseStoreError, Unit]

  /** Stores multiple values with their keys */
  def putAll[K, V](entries: Iterable[(K, V)]): IO[EclipseStoreError, Unit]

  /** Deletes a value by key */
  def delete[K](key: K): IO[EclipseStoreError, Unit]

  /** Retrieves all stored values */
  def getAll[V]: IO[EclipseStoreError, List[V]]

  /** Streams all values */
  def streamValues[V]: ZStream[Any, EclipseStoreError, V]

  /** Streams all keys */
  def streamKeys[K]: ZStream[Any, EclipseStoreError, K]

  /** Retrieves a typed root instance */
  def root[A](descriptor: RootDescriptor[A]): IO[EclipseStoreError, A]

  /** Persists arbitrary objects with the underlying storer */
  def persist[A](value: A): IO[EclipseStoreError, Unit]

  /** Persists a batch of objects */
  def persistAll[A](values: Iterable[A]): IO[EclipseStoreError, Unit]

  /** Reloads the configured roots */
  def reloadRoots: IO[EclipseStoreError, Unit]

  /** Executes lifecycle command */
  def maintenance(command: LifecycleCommand): IO[EclipseStoreError, LifecycleStatus]

  /** Observes lifecycle status */
  def status: UIO[LifecycleStatus]

  /** Exports the current store to the given directory */
  def exportData(target: Path): IO[EclipseStoreError, Unit]

  /** Imports store contents from the given directory into the configured storage target */
  def importData(source: Path): IO[EclipseStoreError, Unit]

/** Live implementation of EclipseStoreService backed by EclipseStore */
final case class EclipseStoreServiceLive(
  config: EclipseStoreConfig,
  storageManager: EmbeddedStorageManager,
  rootContainer: RootContainer,
  storer: Option[Storer],
  persister: Option[Persister],
  statusRef: Ref[LifecycleStatus],
  configuredDescriptors: Chunk[RootDescriptor[?]],
  keyValueDescriptor: RootDescriptor[ConcurrentHashMap[Any, Any]],
  startedAt: Instant = Instant.now(), // scalafix:ok DisableSyntax.noInstantNow
) extends EclipseStoreService:
  private val rootContext = RootContext(rootContainer, Some(storageManager), storer, persister)

  private def kvStore: ConcurrentHashMap[Any, Any] =
    rootContainer.ensure(keyValueDescriptor)

  private def persistRootState: IO[EclipseStoreError, Unit] =
    ZIO
      .attempt {
        storageManager.store(rootContainer.instanceState)
        storageManager.setRoot(rootContainer)
        storageManager.storeRoot()
      }
      .unit
      .mapError(e => EclipseStoreError.StorageError("Failed to persist root state", Some(e)))

  private def withTimeout[A](label: String)(effect: => IO[EclipseStoreError, A]): IO[EclipseStoreError, A] =
    effect.timeoutFail(EclipseStoreError.QueryError(s"$label timed out after ${config.queryTimeout}", None))(
      config.queryTimeout
    )

  private def runQuery[A](query: Query[A]): IO[EclipseStoreError, A] =
    query match
      case Query.Get(key, _) =>
        ZIO
          .attempt {
            Option(kvStore.get(key)).asInstanceOf[A]
          }
          .mapError(e => EclipseStoreError.QueryError(s"Failed to get value for key: $key", Some(e)))

      case Query.Put(key, value, _) =>
        for
          _ <- ZIO
                 .attempt(kvStore.put(key, value))
                 .mapError(e => EclipseStoreError.QueryError(s"Failed to put value for key: $key", Some(e)))
          _ <- persistRootState
        yield ().asInstanceOf[A]

      case Query.Delete(key, _) =>
        for
          _ <- ZIO
                 .attempt(kvStore.remove(key))
                 .mapError(e => EclipseStoreError.QueryError(s"Failed to delete value for key: $key", Some(e)))
          _ <- persistRootState
        yield ().asInstanceOf[A]

      case Query.GetAllKeys(_) =>
        ZIO
          .attempt {
            kvStore.keySet().asScala.toSet.asInstanceOf[A]
          }
          .mapError(e => EclipseStoreError.QueryError("Failed to get all keys", Some(e)))

      case Query.GetAllValues(_) =>
        ZIO
          .attempt {
            kvStore.values().asScala.toList.asInstanceOf[A]
          }
          .mapError(e => EclipseStoreError.QueryError("Failed to get all values", Some(e)))

      case Query.Custom(operation, run, _, _) =>
        ZIO
          .attempt(run(rootContext))
          .mapError(e => EclipseStoreError.QueryError(s"Custom query failed: $operation", Some(e)))

  override def execute[A](query: Query[A]): IO[EclipseStoreError, A] =
    withTimeout(query.queryId)(runQuery(query))

  override def executeMany[A](queries: List[Query[A]]): IO[EclipseStoreError, List[A]] =
    if queries.isEmpty then ZIO.succeed(Nil)
    else
      val indexed                   = queries.zipWithIndex
      val (batchable, nonBatchable) = indexed.partition(_._1.batchable)
      val batchSize                 = math.max(1, config.batchSize)
      for
        batchResults    <- ZIO
                             .foreach(batchable.grouped(batchSize).toList) { chunk =>
                               ZIO.foreach(chunk) {
                                 case (query, idx) =>
                                   withTimeout(query.queryId)(runQuery(query)).map(idx -> _)
                               }
                             }
                             .map(_.flatten)
        nonBatchResults <- ZIO
                             .foreachPar(nonBatchable) {
                               case (query, idx) =>
                                 withTimeout(query.queryId)(runQuery(query)).map(idx -> _)
                             }
                             .withParallelism(config.maxParallelism)
        combined         = (batchResults ++ nonBatchResults).sortBy(_._1).map(_._2)
      yield combined

  override def get[K, V](key: K): IO[EclipseStoreError, Option[V]] =
    execute(Query.Get[K, V](key))

  override def put[K, V](key: K, value: V): IO[EclipseStoreError, Unit] =
    execute(Query.Put[K, V](key, value))

  override def putAll[K, V](entries: Iterable[(K, V)]): IO[EclipseStoreError, Unit] =
    withTimeout("putAll") {
      ZIO
        .attempt {
          entries.foreach { case (k, v) => kvStore.put(k, v) }
        }
        .mapError(e => EclipseStoreError.QueryError("Failed to execute putAll", Some(e))) *> persistRootState
    }

  override def delete[K](key: K): IO[EclipseStoreError, Unit] =
    execute(Query.Delete[K](key))

  override def getAll[V]: IO[EclipseStoreError, List[V]] =
    execute(Query.GetAllValues[V]())

  override def streamValues[V]: ZStream[Any, EclipseStoreError, V] =
    ZStream.fromZIO(getAll[V]).flatMap(list => ZStream.fromIterable(list))

  override def streamKeys[K]: ZStream[Any, EclipseStoreError, K] =
    ZStream.fromZIO(execute(Query.GetAllKeys[K]())).flatMap(keys => ZStream.fromIterable(keys))

  override def root[A](descriptor: RootDescriptor[A]): IO[EclipseStoreError, A] =
    for
      existed <- ZIO
                   .attempt(rootContainer.get(descriptor).isDefined)
                   .mapError(e => EclipseStoreError.ResourceError(s"Failed to obtain root: ${descriptor.id}", Some(e)))
      value   <- ZIO
                   .attempt(rootContainer.ensure(descriptor))
                   .mapError(e => EclipseStoreError.ResourceError(s"Failed to obtain root: ${descriptor.id}", Some(e)))
      _       <- ZIO.when(!existed)(persistRootState)
    yield value

  override def persist[A](value: A): IO[EclipseStoreError, Unit] =
    ZIO
      .attempt {
        storageManager.store(value)
        storageManager.storeRoot()
      }
      .unit
      .mapError(e => EclipseStoreError.StorageError("Failed to persist value", Some(e)))

  override def persistAll[A](values: Iterable[A]): IO[EclipseStoreError, Unit] =
    ZIO.foreachDiscard(values)(persist)

  override val reloadRoots: IO[EclipseStoreError, Unit] =
    (for
      _ <- ZIO.attempt {
             storageManager.root() match
               case container: RootContainer =>
                 rootContainer.replaceWith(container)
               case _                        => ()
           }
      _ <- ZIO.foreachDiscard(configuredDescriptors)(descriptor =>
             ZIO.attempt(rootContainer.ensure(descriptor.asInstanceOf[RootDescriptor[Any]]))
           )
    yield ()).mapError(e => EclipseStoreError.ResourceError("Failed to reload roots", Some(e)))

  override def maintenance(command: LifecycleCommand): IO[EclipseStoreError, LifecycleStatus] =
    command match
      case LifecycleCommand.Checkpoint        =>
        persistRootState *> status
      case LifecycleCommand.Backup(target, _) =>
        backupTo(target) *> status
      case LifecycleCommand.Export(target)    =>
        exportData(target) *> status
      case LifecycleCommand.Import(source)    =>
        importData(source) *> status
      case LifecycleCommand.Restart           =>
        restartManager *> status
      case LifecycleCommand.Shutdown          =>
        shutdownManager *> status

  override val status: UIO[LifecycleStatus] =
    statusRef.get

  override def exportData(target: Path): IO[EclipseStoreError, Unit] =
    for
      _         <- persistRootState
      _         <- wipeDirectory(target)
      exportMgr <- ZIO
                     .attempt(EmbeddedStorage.start(target))
                     .mapError(e => EclipseStoreError.InitializationError("Failed to open export storage", Some(e)))
      exportRoot = rootContainer
      _         <- ZIO
                     .attempt {
                       exportMgr.setRoot(exportRoot)
                       exportMgr.storeRoot()
                       exportMgr.shutdown()
                     }
                     .mapError(e => EclipseStoreError.StorageError("Failed to export store", Some(e)))
    yield ()

  override def importData(source: Path): IO[EclipseStoreError, Unit] =
    for
      imported     <- ZIO
                        .attempt(EmbeddedStorage.start(source))
                        .mapError(e => EclipseStoreError.InitializationError("Failed to open import storage", Some(e)))
      importedRoot <- ZIO
                        .attempt {
                          imported.root() match
                            case container: RootContainer     => container
                            case map: ConcurrentHashMap[?, ?] =>
                              val container = RootContainer.empty
                              val kvRoot    = RootDescriptor.concurrentMap[Any, Any]("kv-root")
                              val typed     = container.ensure(kvRoot)
                              typed.putAll(map.asInstanceOf[ConcurrentHashMap[Any, Any]])
                              container
                            case _                            =>
                              RootContainer.empty
                        }
                        .mapError(e => EclipseStoreError.ResourceError("Failed to read import root", Some(e)))
      _            <- ZIO
                        .attempt(imported.shutdown())
                        .ignore
      _            <- ZIO
                        .attempt(rootContainer.replaceWith(importedRoot))
                        .mapError(e => EclipseStoreError.ResourceError("Failed to replace root during import", Some(e)))
      _            <- persistRootState
    yield ()

  private def backupTo(target: Path): IO[EclipseStoreError, Unit] =
    config.storageTarget.storagePath match
      case Some(source) =>
        persistRootState *> copyDirectory(source, target)
      case None         =>
        ZIO.fail(EclipseStoreError.ResourceError("Cannot backup in-memory storage target", None))

  private def copyDirectory(source: Path, target: Path): IO[EclipseStoreError, Unit] =
    ZIO
      .attempt {
        if java.nio.file.Files.notExists(target) then java.nio.file.Files.createDirectories(target)
        val sourcePath = source
        Using.resource(java.nio.file.Files.walk(sourcePath)) { stream =>
          stream.iterator().asScala.foreach { path =>
            val dest = target.resolve(sourcePath.relativize(path))
            if java.nio.file.Files.isDirectory(path) then
              if java.nio.file.Files.notExists(dest) then java.nio.file.Files.createDirectories(dest)
            else java.nio.file.Files.copy(path, dest, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
          }
        }
      }
      .mapError(e => EclipseStoreError.ResourceError("Failed to copy storage directory", Some(e)))

  private def wipeDirectory(target: Path): IO[EclipseStoreError, Unit] =
    ZIO
      .attempt {
        if java.nio.file.Files.exists(target) then
          Using.resource(java.nio.file.Files.walk(target)) { stream =>
            stream
              .iterator()
              .asScala
              .toList
              .sortBy(_.toString.length)
              .reverse
              .foreach { path =>
                if path != target then java.nio.file.Files.deleteIfExists(path)
              }
          }
        if java.nio.file.Files.notExists(target) then java.nio.file.Files.createDirectories(target)
      }
      .mapError(e => EclipseStoreError.ResourceError("Failed to wipe target directory", Some(e)))

  private def restartManager: IO[EclipseStoreError, Unit] =
    for
      restartingAt <- Clock.instant
      _            <- statusRef.set(LifecycleStatus.Restarting(restartingAt))
      _            <- ZIO
                        .attempt(storageManager.shutdown())
                        .mapError(e => EclipseStoreError.ResourceError("Failed to shutdown during restart", Some(e)))
      _            <- ZIO
                        .attempt(storageManager.start())
                        .mapError(e => EclipseStoreError.InitializationError("Failed to restart storage manager", Some(e)))
      _            <- reloadRoots
      runningAt    <- Clock.instant
      _            <- statusRef.set(LifecycleStatus.Running(runningAt))
    yield ()

  private[service] def shutdownManager: IO[EclipseStoreError, Unit] =
    for
      shuttingDownAt <- Clock.instant
      _              <- statusRef.set(LifecycleStatus.ShuttingDown(shuttingDownAt))
      _              <- ZIO
                          .attempt(storageManager.shutdown())
                          .mapError(e => EclipseStoreError.ResourceError("Failed to shutdown storage manager", Some(e)))
                          .unit
                          .ensuring(statusRef.set(LifecycleStatus.Stopped).ignore)
    yield ()

  def healthMonitor: UIO[Unit] =
    (for
      _       <- ZIO.sleep(config.healthCheckInterval)
      running <- ZIO.attempt(isRunning).orElseSucceed(true)
      _       <- ZIO.unless(running) {
                   statusRef.set(LifecycleStatus.Failed("Storage manager reported unhealthy state"))
                 }
    yield ()).forever

  def autoCheckpoint(interval: Duration): UIO[Unit] =
    (maintenance(LifecycleCommand.Checkpoint).ignore *> ZIO.sleep(interval)).forever

  private def isRunning: Boolean =
    try
      val method = storageManager.getClass.getMethod("isRunning")
      method.invoke(storageManager).asInstanceOf[Boolean]
    catch case _: Throwable => true

object EclipseStoreService:
  /** Accessor method for getting a value */
  def get[K, V](key: K): ZIO[EclipseStoreService, EclipseStoreError, Option[V]] =
    ZIO.serviceWithZIO[EclipseStoreService](_.get(key))

  /** Accessor method for storing a value */
  def put[K, V](key: K, value: V): ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[EclipseStoreService](_.put(key, value))

  /** Accessor for storing multiple entries */
  def putAll[K, V](entries: Iterable[(K, V)]): ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[EclipseStoreService](_.putAll(entries))

  /** Accessor method for deleting a value */
  def delete[K](key: K): ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[EclipseStoreService](_.delete(key))

  /** Accessor method for getting all values */
  def getAll[V]: ZIO[EclipseStoreService, EclipseStoreError, List[V]] =
    ZIO.serviceWithZIO[EclipseStoreService](_.getAll)

  /** Accessor for streaming all values */
  def streamValues[V]: ZStream[EclipseStoreService, EclipseStoreError, V] =
    ZStream.serviceWithStream[EclipseStoreService](_.streamValues)

  /** Accessor for streaming all keys */
  def streamKeys[K]: ZStream[EclipseStoreService, EclipseStoreError, K] =
    ZStream.serviceWithStream[EclipseStoreService](_.streamKeys)

  /** Accessor for typed root retrieval */
  def root[A](descriptor: RootDescriptor[A]): ZIO[EclipseStoreService, EclipseStoreError, A] =
    ZIO.serviceWithZIO[EclipseStoreService](_.root(descriptor))

  /** Accessor for persisting a single value */
  def persist[A](value: A): ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[EclipseStoreService](_.persist(value))

  /** Accessor for persisting many values */
  def persistAll[A](values: Iterable[A]): ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[EclipseStoreService](_.persistAll(values))

  /** Accessor for reloading all configured roots */
  val reloadRoots: ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[EclipseStoreService](_.reloadRoots)

  /** Accessor for lifecycle management */
  def maintenance(command: LifecycleCommand): ZIO[EclipseStoreService, EclipseStoreError, LifecycleStatus] =
    ZIO.serviceWithZIO[EclipseStoreService](_.maintenance(command))

  /** Accessor for lifecycle status */
  val status: URIO[EclipseStoreService, LifecycleStatus] =
    ZIO.serviceWithZIO[EclipseStoreService](_.status)

  /** Accessor for exporting store contents */
  def exportData(target: Path): ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[EclipseStoreService](_.exportData(target))

  /** Accessor for importing store contents */
  def importData(source: Path): ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[EclipseStoreService](_.importData(source))

  /** Accessor method for executing a query */
  def execute[A](query: Query[A]): ZIO[EclipseStoreService, EclipseStoreError, A] =
    ZIO.serviceWithZIO[EclipseStoreService](_.execute(query))

  /** Accessor method for executing multiple queries */
  def executeMany[A](queries: List[Query[A]]): ZIO[EclipseStoreService, EclipseStoreError, List[A]] =
    ZIO.serviceWithZIO[EclipseStoreService](_.executeMany(queries))

  /** ZLayer that provides a live EclipseStoreService with resource-safe management */
  val live: ZLayer[EclipseStoreConfig, EclipseStoreError, EclipseStoreService] =
    ZLayer.scoped {
      for
        config            <- ZIO.service[EclipseStoreConfig]
        foundation        <-
          ZIO
            .attempt(config.storageTarget.buildFoundation())
            .mapError(e => EclipseStoreError.InitializationError("Failed to build storage foundation", Some(e)))
        _                 <- configureFoundation(foundation, config.performance, config)
        storageManager    <-
          ZIO
            .attempt {
              foundation.createEmbeddedStorageManager()
            }
            .mapError(e => EclipseStoreError.InitializationError("Failed to create storage manager", Some(e)))
        _                 <- ZIO
                               .attempt(storageManager.start())
                               .mapError(e => EclipseStoreError.InitializationError("Failed to start storage manager", Some(e)))
        storer             = Option(storageManager).collect { case s: Storer => s }
        persister          = Option(storageManager).collect { case p: Persister => p }
        rootContainer     <- ZIO
                               .attempt {
                                 storageManager.root() match
                                   case container: RootContainer     =>
                                     storageManager.setRoot(container)
                                     container
                                   case map: ConcurrentHashMap[?, ?] =>
                                     val container = RootContainer.empty
                                     val kvRoot    = RootDescriptor.concurrentMap[Any, Any]("kv-root")
                                     val typed     = container.ensure(kvRoot)
                                     typed.putAll(map.asInstanceOf[ConcurrentHashMap[Any, Any]])
                                     storageManager.setRoot(container)
                                     storageManager.storeRoot()
                                     container
                                   case _                            =>
                                     val created = RootContainer.empty
                                     storageManager.setRoot(created)
                                     storageManager.storeRoot()
                                     created
                               }
                               .mapError(e =>
                                 EclipseStoreError.InitializationError("Failed to initialize root container", Some(e))
                               )
        keyValueDescriptor = RootDescriptor.concurrentMap[Any, Any]("kv-root")
        descriptorChunk    =
          if config.rootDescriptors.exists(_.id == keyValueDescriptor.id) then config.rootDescriptors
          else config.rootDescriptors :+ keyValueDescriptor
        _                 <- ZIO.foreachDiscard(descriptorChunk)(descriptor =>
                               ZIO
                                 .attempt(rootContainer.ensure(descriptor.asInstanceOf[RootDescriptor[Any]]))
                                 .mapError(e =>
                                   EclipseStoreError.InitializationError(s"Failed to initialize root ${descriptor.id}", Some(e))
                                 )
                             )
        startingAt        <- Clock.instant
        statusRef         <- Ref.make[LifecycleStatus](LifecycleStatus.Starting(startingAt))
        service            = EclipseStoreServiceLive(
                               config = config,
                               storageManager = storageManager,
                               rootContainer = rootContainer,
                               storer = storer,
                               persister = persister,
                               statusRef = statusRef,
                               configuredDescriptors = descriptorChunk,
                               keyValueDescriptor = keyValueDescriptor,
                             )
        _                 <- statusRef.set(LifecycleStatus.Running(service.startedAt))
        _                 <- ZIO.addFinalizer(
                               service
                                 .shutdownManager
                                 .tapError(e => ZIO.logError(s"Error shutting down storage manager: $e"))
                                 .orElseSucceed(())
                             )
        _                 <- service.healthMonitor.forkScoped
        _                 <- config.autoCheckpointInterval match
                               case Some(interval) => service.autoCheckpoint(interval).forkScoped
                               case None           => ZIO.unit
      yield service
    }

  private[eclipsestore] def configureFoundation(
    foundation: AnyRef,
    performance: StoragePerformanceConfig,
    config: EclipseStoreConfig,
  ): IO[EclipseStoreError, Unit] =
    ZIO
      .attempt {
        def invokeIfExists(method: String, args: Seq[AnyRef]): Unit =
          foundation
            .getClass
            .getMethods
            .find(m => m.getName == method && m.getParameterCount == args.length)
            .foreach(_.invoke(foundation, args*))

        invokeIfExists("setChannelCount", Seq(Int.box(performance.channelCount)))
        performance.pageCacheSizeBytes.foreach(size => invokeIfExists("setPageCacheSizeBytes", Seq(Long.box(size))))
        performance.objectCacheSizeBytes.foreach(size => invokeIfExists("setObjectCacheSizeBytes", Seq(Long.box(size))))
        if performance.useOffHeapPageStore then invokeIfExists("enableOffHeapPageStore", Seq.empty)
        performance.compression match
          case CompressionSetting.Disabled => ()
          case other                       => invokeIfExists("setCompression", Seq(other.toString))
        performance.encryptionKey.foreach(key => invokeIfExists("setEncryptionKey", Seq(key)))
        config.backupDirectory.foreach(dir => invokeIfExists("setBackupDirectory", Seq(dir.toString)))
        config
          .backupTruncationDirectory
          .foreach(dir => invokeIfExists("setBackupTruncationDirectory", Seq(dir.toString)))
        config.backupDeletionDirectory.foreach(dir => invokeIfExists("setBackupDeletionDirectory", Seq(dir.toString)))
        val backupProps          =
          config.backupExternalProperties ++ config.backupTarget.map(_.toProperties).getOrElse(Map.empty)
        applyBackupConfiguration(foundation, backupProps)
        val connectionFoundation =
          foundation
            .getClass
            .getMethods
            .find(m => m.getName == "onConnectionFoundation" && m.getParameterCount == 1)

        connectionFoundation.foreach { method =>
          val consumer = new java.util.function.Consumer[AnyRef]:
            override def accept(cf: AnyRef): Unit =
              def invokeCf(name: String, args: Seq[AnyRef]): Unit =
                cf.getClass.getMethods.find(m => m.getName == name && m.getParameterCount == args.length).foreach { m =>
                  try
                    val finalArgs =
                      if m.isVarArgs && args.length == 1 && m.getParameterTypes.head.isArray then
                        val arrayType = m.getParameterTypes.head.getComponentType
                        val arr       = java.lang.reflect.Array.newInstance(arrayType, 1).asInstanceOf[Array[AnyRef]]
                        arr(0) = args.head
                        Seq(arr)
                      else args
                    m.invoke(cf, finalArgs*)
                  catch case _: Throwable => () // best-effort registration
                }

              config.customTypeHandlers.foreach(handler => invokeCf("registerCustomTypeHandlers", Seq(handler)))
              config.eagerStoringEvaluator.foreach(eval => invokeCf("setReferenceFieldEagerEvaluator", Seq(eval)))

          method.invoke(foundation, consumer)
        }
      }
      .mapError(e => EclipseStoreError.InitializationError("Failed to apply storage configuration", Some(e)))

  private[eclipsestore] def applyBackupConfiguration(
    target: AnyRef,
    properties: Map[String, String],
  ): Unit =
    def invoke(name: String, args: Seq[AnyRef]): Unit =
      target.getClass.getMethods.find(m => m.getName == name && m.getParameterCount == args.length).foreach { m =>
        m.invoke(target, args*)
      }

    properties.foreach {
      case (k, v) =>
        invoke("setBackupConfigurationProperty", Seq(k, v))
    }

  /** Test implementation that uses an in-memory map */
  def inMemory: ULayer[EclipseStoreService] =
    ZLayer.fromZIO {
      for statusRef <- Ref.make[LifecycleStatus](LifecycleStatus.Running(Instant.EPOCH))
      yield new EclipseStoreService:
        private val container          = RootContainer.empty
        private val keyValueDescriptor = RootDescriptor.concurrentMap[Any, Any]("kv-root")
        private def kv                 = container.ensure(keyValueDescriptor)

        override def execute[A](query: Query[A]): IO[EclipseStoreError, A] =
          query match
            case Query.Get(key, _)                  =>
              ZIO.succeed(Option(kv.get(key)).asInstanceOf[A])
            case Query.Put(key, value, _)           =>
              ZIO.succeed(kv.put(key, value)).as(().asInstanceOf[A])
            case Query.Delete(key, _)               =>
              ZIO.succeed(kv.remove(key)).as(().asInstanceOf[A])
            case Query.GetAllKeys(_)                =>
              ZIO.succeed(kv.keySet().asScala.toSet.asInstanceOf[A])
            case Query.GetAllValues(_)              =>
              ZIO.succeed(kv.values().asScala.toList.asInstanceOf[A])
            case Query.Custom(operation, run, _, _) =>
              ZIO
                .attempt(run(RootContext(container, None, None, None)))
                .mapError(e => EclipseStoreError.QueryError(s"Custom query failed: $operation", Some(e)))

        override def executeMany[A](queries: List[Query[A]]): IO[EclipseStoreError, List[A]] =
          ZIO.foreach(queries)(execute)

        override def get[K, V](key: K): IO[EclipseStoreError, Option[V]] =
          execute(Query.Get[K, V](key))

        override def put[K, V](key: K, value: V): IO[EclipseStoreError, Unit] =
          execute(Query.Put[K, V](key, value))

        override def putAll[K, V](entries: Iterable[(K, V)]): IO[EclipseStoreError, Unit] =
          ZIO.succeed(entries.foreach { case (k, v) => kv.put(k, v) })

        override def delete[K](key: K): IO[EclipseStoreError, Unit] =
          execute(Query.Delete[K](key))

        override def getAll[V]: IO[EclipseStoreError, List[V]] =
          execute(Query.GetAllValues[V]())

        override def streamValues[V]: ZStream[Any, EclipseStoreError, V] =
          ZStream.fromIterable(kv.values().asScala.toList.map(_.asInstanceOf[V]))

        override def streamKeys[K]: ZStream[Any, EclipseStoreError, K] =
          ZStream.fromIterable(kv.keySet().asScala.toList.map(_.asInstanceOf[K]))

        override def root[A](descriptor: RootDescriptor[A]): IO[EclipseStoreError, A] =
          ZIO.succeed(container.ensure(descriptor))

        override def persist[A](value: A): IO[EclipseStoreError, Unit] =
          ZIO.unit

        override def persistAll[A](values: Iterable[A]): IO[EclipseStoreError, Unit] =
          ZIO.foreachDiscard(values)(_ => ZIO.unit)

        override val reloadRoots: IO[EclipseStoreError, Unit] =
          ZIO.unit

        override def maintenance(command: LifecycleCommand): IO[EclipseStoreError, LifecycleStatus] =
          command match
            case LifecycleCommand.Shutdown =>
              statusRef.set(LifecycleStatus.Stopped) *> statusRef.get
            case _                         => statusRef.get

        override def exportData(target: Path): IO[EclipseStoreError, Unit] =
          ZIO.fail(EclipseStoreError.ResourceError("Cannot export in-memory store", None))

        override def importData(source: Path): IO[EclipseStoreError, Unit] =
          ZIO.fail(EclipseStoreError.ResourceError("Cannot import into in-memory store", None))

        override val status: UIO[LifecycleStatus] =
          statusRef.get
    }
