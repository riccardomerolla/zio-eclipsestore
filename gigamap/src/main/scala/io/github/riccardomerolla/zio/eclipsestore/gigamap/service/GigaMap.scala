package io.github.riccardomerolla.zio.eclipsestore.gigamap.service

import zio.*

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentHashMap.KeySetView

import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.*
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.*
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError.*
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import scala.jdk.CollectionConverters.*

trait GigaMap[K, V]:
  def definition: GigaMapDefinition[K, V]
  def put(key: K, value: V): IO[GigaMapError, Unit]
  def putAll(values: Iterable[(K, V)]): IO[GigaMapError, Unit]
  def get(key: K): IO[GigaMapError, Option[V]]
  def remove(key: K): IO[GigaMapError, Option[V]]
  def clear: IO[GigaMapError, Unit]
  def size: IO[GigaMapError, Int]
  def query[A](query: GigaMapQuery[V, A]): IO[GigaMapError, A]
  def entries: IO[GigaMapError, Chunk[(K, V)]]
  def keys: IO[GigaMapError, Chunk[K]]
  def persist: IO[GigaMapError, Unit]

object GigaMap:
  def make[K: Tag, V: Tag](definition: GigaMapDefinition[K, V])
      : ZLayer[EclipseStoreService, GigaMapError, GigaMap[K, V]] =
    ZLayer.fromZIO {
      for
        service    <- ZIO.service[EclipseStoreService]
        persistSem <- Semaphore.make(1)
      yield GigaMapLive(definition, service, persistSem)
    }

  given [K: Tag, V: Tag]: Tag[GigaMap[K, V]] = Tag.derived

final private class GigaMapLive[K, V: Tag](
    initialDefinition: GigaMapDefinition[K, V],
    store: EclipseStoreService,
    persistSem: Semaphore,
  ) extends GigaMap[K, V]:
  override val definition: GigaMapDefinition[K, V] = initialDefinition

  private val registry: GigaMapRegistry =
    Unsafe.unsafe { implicit unsafe =>
      val runtime = Runtime.default
      runtime
        .unsafe
        .run(store.root(GigaMapRegistry.descriptor))
        .getOrThrowFiberFailure()
    }

  private val map: ConcurrentHashMap[Any, Any] =
    registry.mapFor(definition.name)

  private val indexState: ConcurrentHashMap[String, ConcurrentHashMap[Any, KeySetView[Any, java.lang.Boolean]]] =
    registry.indexesFor(definition.name)

  private val indexes = definition.anyIndexes

  private val vectorIndexes = definition.vectorIndexes

  // Store vector embeddings for similarity search
  private val vectorStore: ConcurrentHashMap[String, ConcurrentHashMap[Any, Array[Float]]] =
    new ConcurrentHashMap()

  override def put(key: K, value: V): IO[GigaMapError, Unit] =
    persistSem.withPermit {
      for
        previous <- attempt {
                      Option(map.put(key, value)).map(_.asInstanceOf[V])
                    }
        _        <- updateIndexes(key, previous, Some(value))
        _        <- updateVectorIndexes(key, previous, Some(value))
        _        <- persistIfNeeded
      yield ()
    }

  override def putAll(values: Iterable[(K, V)]): IO[GigaMapError, Unit] =
    for _ <- ZIO.foreachDiscard(values) { case (key, value) => put(key, value) } yield ()

  override def get(key: K): IO[GigaMapError, Option[V]] =
    attempt(Option(map.get(key)).map(_.asInstanceOf[V]))

  override def remove(key: K): IO[GigaMapError, Option[V]] =
    persistSem.withPermit {
      for
        removed <- attempt(Option(map.remove(key)).map(_.asInstanceOf[V]))
        _       <- updateIndexes(key, removed, None)
        _       <- updateVectorIndexes(key, removed, None)
        _       <- persistIfNeeded
      yield removed
    }

  override def clear: IO[GigaMapError, Unit] =
    persistSem.withPermit {
      attempt {
        map.clear()
        indexState.values().asScala.foreach(_.clear())
        vectorStore.values().asScala.foreach(_.clear())
      } *> persistIfNeeded
    }

  override def size: IO[GigaMapError, Int] =
    attempt(map.size())

  override def entries: IO[GigaMapError, Chunk[(K, V)]] =
    attempt {
      val iter = map.entrySet().iterator().asScala
      Chunk.fromIterator(iter.map(e => (e.getKey.asInstanceOf[K], e.getValue.asInstanceOf[V])))
    }

  override def keys: IO[GigaMapError, Chunk[K]] =
    attempt {
      val iter = map.keySet().iterator().asScala
      Chunk.fromIterator(iter.map(_.asInstanceOf[K]))
    }

  override def query[A](query: GigaMapQuery[V, A]): IO[GigaMapError, A] =
    query match
      case GigaMapQuery.All()                     =>
        entries.map(_.map(_._2)).asInstanceOf[IO[GigaMapError, A]]
      case GigaMapQuery.Filter(predicate)         =>
        attempt {
          val matched = map
            .values()
            .asScala
            .map(_.asInstanceOf[V])
            .filter(predicate)
          Chunk.fromIterable(matched)
        }.asInstanceOf[IO[GigaMapError, A]]
      case GigaMapQuery.ByIndex(indexName, value) =>
        for result <- byIndex(indexName, value)
        yield result.asInstanceOf[A]
      case query: GigaMapQuery.VectorSimilarity[V] =>
        for result <- vectorSimilaritySearch(query.indexName, query.vector, query.limit, query.threshold)
        yield result.asInstanceOf[A]
      case GigaMapQuery.Count()                   =>
        size.map(_.toLong).asInstanceOf[IO[GigaMapError, A]]

  override def persist: IO[GigaMapError, Unit] =
    persistState

  private def byIndex(indexName: String, value: Any): IO[GigaMapError, Chunk[V]] =
    for
      state   <- ZIO.fromOption(Option(indexState.get(indexName))).orElseFail(IndexNotDefined(indexName))
      keysOpt  = Option(state.get(value))
      entries <- attempt {
                   val data = keysOpt
                     .map(_.asScala.toList.flatMap(key => Option(map.get(key)).map(_.asInstanceOf[V])))
                     .getOrElse(Nil)
                   Chunk.fromIterable(data)
                 }
    yield entries

  private def updateVectorIndexes(key: K, oldValue: Option[V], newValue: Option[V]): IO[GigaMapError, Unit] =
    attempt {
      vectorIndexes.foreach { vectorIndex =>
        val name  = vectorIndex.name
        val store =
          vectorStore.computeIfAbsent(name, _ => new ConcurrentHashMap[Any, Array[Float]]())
        oldValue.foreach { value =>
          store.remove(key)
        }
        newValue.foreach { value =>
          val vector = vectorIndex.extract(value)
          store.put(key, vector)
        }
      }
    }

  private def vectorSimilaritySearch(
      indexName: String,
      queryVector: Array[Float],
      limit: Int,
      threshold: Option[Float]
  ): IO[GigaMapError, Chunk[V]] =
    for
      vectorIndexOpt <- attempt { vectorIndexes.find(_.name == indexName) }
      vectorIndex    <- ZIO.fromOption(vectorIndexOpt).orElseFail(IndexNotDefined(indexName))
      store          <- attempt { vectorStore.computeIfAbsent(indexName, _ => new ConcurrentHashMap[Any, Array[Float]]()) }
      results <- attempt {
        val scores = store.entrySet().asScala
          .map { entry =>
            val key    = entry.getKey
            val vector = entry.getValue
            val distance = cosineSimilarity(queryVector, vector)
            (key, distance)
          }
          .filter { case (_, distance) =>
            threshold.forall(t => distance >= t)
          }
          .toList
          .sortBy { case (_, distance) => -distance } // Sort by distance descending
          .take(limit)
          .map { case (key, _) =>
            Option(map.get(key)).map(_.asInstanceOf[V])
          }
          .flatten
        Chunk.fromIterable(scores)
      }
    yield results

  private def cosineSimilarity(vecA: Array[Float], vecB: Array[Float]): Float =
    if (vecA.length != vecB.length) return 0f
    val dotProduct = (vecA zip vecB).foldLeft(0f) { case (sum, (a, b)) => sum + (a * b) }
    val magnitudeA = Math.sqrt(vecA.foldLeft(0f) { case (sum, a) => sum + (a * a) })
    val magnitudeB = Math.sqrt(vecB.foldLeft(0f) { case (sum, b) => sum + (b * b) })
    if (magnitudeA == 0 || magnitudeB == 0) 0f else (dotProduct / (magnitudeA * magnitudeB)).toFloat

  private def updateIndexes(key: K, oldValue: Option[V], newValue: Option[V]): IO[GigaMapError, Unit] =
    attempt {
      indexes.foreach { index =>
        val name  = index.name
        val state =
          indexState.computeIfAbsent(name, _ => new ConcurrentHashMap[Any, KeySetView[Any, java.lang.Boolean]]())
        oldValue.foreach { value =>
          index.extract(value).foreach { field =>
            Option(state.get(field)).foreach(_.remove(key))
          }
        }
        newValue.foreach { value =>
          index.extract(value).foreach { field =>
            val bucket = state.computeIfAbsent(field, _ => ConcurrentHashMap.newKeySet[Any]())
            bucket.add(key)
          }
        }
      }
    }

  // Called only when the caller already holds persistSem
  private def persistIfNeeded: IO[GigaMapError, Unit] =
    if definition.autoPersist then persistStateInternal else ZIO.unit

  // Public persist: acquires the semaphore to prevent concurrent mutation/persist
  private def persistState: IO[GigaMapError, Unit] =
    persistSem.withPermit(persistStateInternal)

  // Core persist logic — must only be called while persistSem is held
  private def persistStateInternal: IO[GigaMapError, Unit] =
    val registryMaps              = registry.maps
    val registryIndexes           = registry.indexes
    val indexMaps                 =
      indexState
        .values()
        .asScala
        .toList
    val indexBuckets              =
      indexMaps.flatMap(_.values().asScala.toList)
    val baseTargets: List[AnyRef] =
      List(registry, registryMaps, registryIndexes, map, indexState)
    val targets: List[AnyRef]     =
      baseTargets ++ indexMaps ++ indexBuckets
    store
      .persistAll[AnyRef](targets)
      .mapError(e => StorageFailure("Failed to persist GigaMap state", Some(new RuntimeException(e.toString))))

  private def attempt[A](thunk: => A): IO[GigaMapError, A] =
    ZIO.attempt(thunk).mapError(e => StorageFailure("GigaMap operation failed", Some(e)))
