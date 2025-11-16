package io.github.riccardomerolla.zio.eclipsestore.service

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.domain.Query
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import org.eclipse.serializer.persistence.types.Persister
import org.eclipse.serializer.persistence.types.Storer
import org.eclipse.store.storage.embedded.types.EmbeddedStorage
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager
import zio.*

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

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
  
  /** Deletes a value by key */
  def delete[K](key: K): IO[EclipseStoreError, Unit]
  
  /** Retrieves all stored values */
  def getAll[V]: IO[EclipseStoreError, List[V]]

/** Live implementation of EclipseStoreService backed by EclipseStore */
final case class EclipseStoreServiceLive(
  config: EclipseStoreConfig,
  storageManager: EmbeddedStorageManager,
  root: ConcurrentHashMap[Any, Any]
) extends EclipseStoreService:
  
  override def execute[A](query: Query[A]): IO[EclipseStoreError, A] =
    query match
      case Query.Get(key, _) =>
        ZIO.attempt {
          Option(root.get(key)).asInstanceOf[A]
        }.mapError(e => EclipseStoreError.QueryError(s"Failed to get value for key: $key", Some(e)))
      
      case Query.Put(key, value, _) =>
        ZIO.attempt {
          root.put(key, value)
          storageManager.store(root)
          ().asInstanceOf[A]
        }.mapError(e => EclipseStoreError.QueryError(s"Failed to put value for key: $key", Some(e)))
      
      case Query.Delete(key, _) =>
        ZIO.attempt {
          root.remove(key)
          storageManager.store(root)
          ().asInstanceOf[A]
        }.mapError(e => EclipseStoreError.QueryError(s"Failed to delete value for key: $key", Some(e)))
      
      case Query.GetAllKeys(_) =>
        ZIO.attempt {
          root.keySet().asScala.toSet.asInstanceOf[A]
        }.mapError(e => EclipseStoreError.QueryError("Failed to get all keys", Some(e)))
      
      case Query.GetAllValues(_) =>
        ZIO.attempt {
          root.values().asScala.toList.asInstanceOf[A]
        }.mapError(e => EclipseStoreError.QueryError("Failed to get all values", Some(e)))
      
      case Query.Custom(operation, _, _) =>
        ZIO.fail(EclipseStoreError.QueryError(s"Custom query not implemented: $operation", None))
  
  override def executeMany[A](queries: List[Query[A]]): IO[EclipseStoreError, List[A]] =
    if queries.isEmpty then
      ZIO.succeed(List.empty)
    else
      // Group queries by batchability
      val (batchable, nonBatchable) = queries.partition(_.batchable)
      
      for
        // Execute batchable queries in sequence (they're already optimized by being batched)
        batchResults <- ZIO.foreach(batchable)(execute)
        
        // Execute non-batchable queries in parallel with controlled parallelism
        nonBatchResults <- ZIO.foreachPar(nonBatchable)(execute).withParallelism(config.maxParallelism)
        
        // Combine results maintaining original order
        allResults = (batchable.map((_, true)) ++ nonBatchable.map((_, false)))
          .zip(batchResults ++ nonBatchResults)
          .sortBy { case ((query, _), _) => queries.indexOf(query) }
          .map(_._2)
      yield allResults
  
  override def get[K, V](key: K): IO[EclipseStoreError, Option[V]] =
    execute(Query.Get[K, V](key))
  
  override def put[K, V](key: K, value: V): IO[EclipseStoreError, Unit] =
    execute(Query.Put[K, V](key, value))
  
  override def delete[K](key: K): IO[EclipseStoreError, Unit] =
    execute(Query.Delete[K](key))
  
  override def getAll[V]: IO[EclipseStoreError, List[V]] =
    execute(Query.GetAllValues[V]())

object EclipseStoreService:
  /** Accessor method for getting a value */
  def get[K, V](key: K): ZIO[EclipseStoreService, EclipseStoreError, Option[V]] =
    ZIO.serviceWithZIO[EclipseStoreService](_.get(key))
  
  /** Accessor method for storing a value */
  def put[K, V](key: K, value: V): ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[EclipseStoreService](_.put(key, value))
  
  /** Accessor method for deleting a value */
  def delete[K](key: K): ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[EclipseStoreService](_.delete(key))
  
  /** Accessor method for getting all values */
  def getAll[V]: ZIO[EclipseStoreService, EclipseStoreError, List[V]] =
    ZIO.serviceWithZIO[EclipseStoreService](_.getAll)
  
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
        config <- ZIO.service[EclipseStoreConfig]
        
        // Initialize EclipseStore
        storageManager <- ZIO.attempt {
          EmbeddedStorage.Foundation(config.storagePath)
            .createEmbeddedStorageManager()
        }.mapError(e => EclipseStoreError.InitializationError("Failed to create storage manager", Some(e)))
        
        // Start storage manager
        _ <- ZIO.attempt(storageManager.start())
          .mapError(e => EclipseStoreError.InitializationError("Failed to start storage manager", Some(e)))
        
        // Get or initialize root object
        root <- ZIO.attempt {
          Option(storageManager.root()) match
            case Some(r: ConcurrentHashMap[?, ?]) => r.asInstanceOf[ConcurrentHashMap[Any, Any]]
            case _ =>
              val newRoot = new ConcurrentHashMap[Any, Any]()
              storageManager.setRoot(newRoot)
              storageManager.storeRoot()
              newRoot
        }.mapError(e => EclipseStoreError.InitializationError("Failed to initialize root object", Some(e)))
        
        // Register finalizer to shutdown storage manager
        _ <- ZIO.addFinalizer(
          ZIO.attempt(storageManager.shutdown())
            .tapError(e => ZIO.logError(s"Error shutting down storage manager: ${e.getMessage}"))
            .ignore
        )
        
        service = EclipseStoreServiceLive(config, storageManager, root)
      yield service
    }
  
  /** Test implementation that uses an in-memory map */
  def inMemory: ULayer[EclipseStoreService] =
    ZLayer.succeed(new EclipseStoreService:
      private val storage = new ConcurrentHashMap[Any, Any]()
      
      override def execute[A](query: Query[A]): IO[EclipseStoreError, A] =
        query match
          case Query.Get(key, _) =>
            ZIO.succeed(Option(storage.get(key)).asInstanceOf[A])
          case Query.Put(key, value, _) =>
            ZIO.succeed(storage.put(key, value)).as(().asInstanceOf[A])
          case Query.Delete(key, _) =>
            ZIO.succeed(storage.remove(key)).as(().asInstanceOf[A])
          case Query.GetAllKeys(_) =>
            ZIO.succeed(storage.keySet().asScala.toSet.asInstanceOf[A])
          case Query.GetAllValues(_) =>
            ZIO.succeed(storage.values().asScala.toList.asInstanceOf[A])
          case Query.Custom(operation, _, _) =>
            ZIO.fail(EclipseStoreError.QueryError(s"Custom query not supported in test mode: $operation", None))
      
      override def executeMany[A](queries: List[Query[A]]): IO[EclipseStoreError, List[A]] =
        ZIO.foreach(queries)(execute)
      
      override def get[K, V](key: K): IO[EclipseStoreError, Option[V]] =
        execute(Query.Get[K, V](key))
      
      override def put[K, V](key: K, value: V): IO[EclipseStoreError, Unit] =
        execute(Query.Put[K, V](key, value))
      
      override def delete[K](key: K): IO[EclipseStoreError, Unit] =
        execute(Query.Delete[K](key))
      
      override def getAll[V]: IO[EclipseStoreError, List[V]] =
        execute(Query.GetAllValues[V]())
    )
