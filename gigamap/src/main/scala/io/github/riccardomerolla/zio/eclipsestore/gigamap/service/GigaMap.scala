package io.github.riccardomerolla.zio.eclipsestore.gigamap.service

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentHashMap.KeySetView

import scala.jdk.CollectionConverters.*
import scala.math.{ atan2, cos, sin, sqrt }

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.*
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.*
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError.*
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

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

  private val spatialIndexes = definition.spatialIndexes

  // Store vector embeddings for similarity search
  private val vectorStore: ConcurrentHashMap[String, ConcurrentHashMap[Any, Array[Float]]] =
    new ConcurrentHashMap()

  private val spatialStore: ConcurrentHashMap[String, ConcurrentHashMap[Any, GeoPoint]] =
    new ConcurrentHashMap()

  override def put(key: K, value: V): IO[GigaMapError, Unit] =
    persistSem.withPermit {
      for
        previous <- attempt {
                      Option(map.put(key, value)).map(_.asInstanceOf[V])
                    }
        _        <- updateIndexes(key, previous, Some(value))
        _        <- updateVectorIndexes(key, previous, Some(value))
        _        <- updateSpatialIndexes(key, previous, Some(value))
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
        _       <- updateSpatialIndexes(key, removed, None)
        _       <- persistIfNeeded
      yield removed
    }

  override def clear: IO[GigaMapError, Unit] =
    persistSem.withPermit {
      attempt {
        map.clear()
        indexState.values().asScala.foreach(_.clear())
        vectorStore.values().asScala.foreach(_.clear())
        spatialStore.values().asScala.foreach(_.clear())
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
      case GigaMapQuery.All()                                              =>
        entries.map(_.map(_._2)).asInstanceOf[IO[GigaMapError, A]]
      case GigaMapQuery.Filter(predicate)                                  =>
        attempt {
          val matched = map
            .values()
            .asScala
            .map(_.asInstanceOf[V])
            .filter(predicate)
          Chunk.fromIterable(matched)
        }.asInstanceOf[IO[GigaMapError, A]]
      case GigaMapQuery.ByIndex(indexName, value)                          =>
        for result <- byIndex(indexName, value)
        yield result.asInstanceOf[A]
      case GigaMapQuery.VectorSearch(indexName, vector, limit, minScore)   =>
        vectorSearch(indexName, vector, limit, minScore).asInstanceOf[IO[GigaMapError, A]]
      case query: GigaMapQuery.VectorSimilarity[V]                         =>
        vectorSearch(query.indexName, Chunk.fromArray(query.vector), query.limit, query.threshold)
          .map(_.map(_.value))
          .asInstanceOf[IO[GigaMapError, A]]
      case GigaMapQuery.SpatialRadius(indexName, center, radiusKilometers) =>
        spatialRadius(indexName, center, radiusKilometers).asInstanceOf[IO[GigaMapError, A]]
      case GigaMapQuery.Composite(predicates)                              =>
        composite(predicates).asInstanceOf[IO[GigaMapError, A]]
      case GigaMapQuery.Count()                                            =>
        size.map(_.toLong).asInstanceOf[IO[GigaMapError, A]]

  override def persist: IO[GigaMapError, Unit] =
    persistState

  private def byIndexKeys(indexName: String, value: Any): IO[GigaMapError, Chunk[Any]] =
    for
      state   <- ZIO.fromOption(Option(indexState.get(indexName))).orElseFail(IndexNotDefined(indexName))
      keysOpt  = Option(state.get(value))
      entries <- attempt(Chunk.fromIterable(keysOpt.map(_.asScala.toList).getOrElse(Nil)))
    yield entries

  private def filterKeys(predicate: V => Boolean): IO[GigaMapError, Chunk[Any]] =
    attempt {
      Chunk.fromIterable(
        map.entrySet().asScala.collect {
          case entry if predicate(entry.getValue.asInstanceOf[V]) => entry.getKey
        }
      )
    }

  private def byIndex(indexName: String, value: Any): IO[GigaMapError, Chunk[V]] =
    for
      keys    <- byIndexKeys(indexName, value)
      entries <- attempt(keys.flatMap(key => Option(map.get(key)).map(_.asInstanceOf[V])))
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

  private def vectorSearchKeys(
    indexName: String,
    queryVector: Chunk[Float],
    limit: Int,
    minScore: Option[Float],
  ): IO[GigaMapError, Chunk[(Any, Float)]] =
    for
      vectorIndexOpt <- attempt(vectorIndexes.find(_.name == indexName))
      vectorIndex    <- ZIO.fromOption(vectorIndexOpt).orElseFail(IndexNotDefined(indexName))
      query           = queryVector.toArray
      _              <-
        if query.length == vectorIndex.dimension then ZIO.unit
        else ZIO.fail(InvalidVectorQuery(indexName, vectorIndex.dimension, query.length))
      store          <- attempt(vectorStore.computeIfAbsent(indexName, _ => new ConcurrentHashMap[Any, Array[Float]]()))
      results        <- attempt {
                          Chunk.fromIterable(
                            store.entrySet().asScala
                              .map { entry =>
                                val key    = entry.getKey
                                val vector = entry.getValue
                                val score  = similarity(vectorIndex.similarityFunction, query, vector)
                                (key, score)
                              }
                              .filter { case (_, score) => minScore.forall(score >= _) }
                              .toList
                              .sortBy { case (_, score) => -score }
                              .take(limit)
                          )
                        }
    yield results

  private def vectorSearch(
    indexName: String,
    queryVector: Chunk[Float],
    limit: Int,
    minScore: Option[Float],
  ): IO[GigaMapError, Chunk[GigaMapScored[V]]] =
    for
      ranked <- vectorSearchKeys(indexName, queryVector, limit, minScore)
      values <- attempt {
                  ranked.flatMap {
                    case (key, score) =>
                      Option(map.get(key)).map(value => GigaMapScored(value.asInstanceOf[V], score))
                  }
                }
    yield values

  private def spatialRadiusKeys(indexName: String, center: GeoPoint, radiusKilometers: Double)
    : IO[GigaMapError, Chunk[Any]] =
    if radiusKilometers < 0 then ZIO.fail(InvalidSpatialQuery(indexName, radiusKilometers))
    else
      for
        store <- ZIO.fromOption(Option(spatialStore.get(indexName))).orElseFail(IndexNotDefined(indexName))
        keys  <- attempt {
                   Chunk.fromIterable(
                     store.entrySet().asScala.collect {
                       case entry if haversineKilometers(center, entry.getValue) <= radiusKilometers => entry.getKey
                     }
                   )
                 }
      yield keys

  private def spatialRadius(indexName: String, center: GeoPoint, radiusKilometers: Double): IO[GigaMapError, Chunk[V]] =
    for
      keys   <- spatialRadiusKeys(indexName, center, radiusKilometers)
      values <- attempt(keys.flatMap(key => Option(map.get(key)).map(_.asInstanceOf[V])))
    yield values

  private def composite(predicates: Chunk[GigaMapPredicate[V]]): IO[GigaMapError, Chunk[V]] =
    if predicates.isEmpty then query(GigaMapQuery.All[V]())
    else
      for
        matched     <- ZIO.foreach(predicates)(predicateKeys)
        intersection = matched.map(_.toSet).reduce(_ intersect _)
        values      <- attempt {
                         Chunk.fromIterable(
                           map.entrySet().asScala.collect {
                             case entry if intersection.contains(entry.getKey) => entry.getValue.asInstanceOf[V]
                           }
                         )
                       }
      yield values

  private def predicateKeys(predicate: GigaMapPredicate[V]): IO[GigaMapError, Chunk[Any]] =
    predicate match
      case GigaMapPredicate.Filter(pred)                            => filterKeys(pred)
      case GigaMapPredicate.ByIndex(index, value)                   => byIndexKeys(index, value)
      case GigaMapPredicate.VectorSearch(index, vector, limit, min) =>
        vectorSearchKeys(index, vector, limit, min).map(_.map(_._1))
      case GigaMapPredicate.SpatialRadius(index, center, radius)    => spatialRadiusKeys(index, center, radius)

  private def updateSpatialIndexes(key: K, oldValue: Option[V], newValue: Option[V]): IO[GigaMapError, Unit] =
    attempt {
      spatialIndexes.foreach { spatialIndex =>
        val name  = spatialIndex.name
        val store =
          spatialStore.computeIfAbsent(name, _ => new ConcurrentHashMap[Any, GeoPoint]())
        oldValue.foreach(_ => store.remove(key))
        newValue.foreach(value => store.put(key, spatialIndex.extract(value)))
      }
    }

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
    val vectorMaps                =
      vectorStore.values().asScala.toList
    val spatialMaps               =
      spatialStore.values().asScala.toList
    val baseTargets: List[AnyRef] =
      List(registry, registryMaps, registryIndexes, map, indexState, vectorStore, spatialStore)
    val targets: List[AnyRef]     =
      baseTargets ++ indexMaps ++ indexBuckets ++ vectorMaps ++ spatialMaps
    store
      .persistAll[AnyRef](targets)
      .mapError(e => StorageFailure("Failed to persist GigaMap state", Some(new RuntimeException(e.toString))))

  private def similarity(
    function: io.github.riccardomerolla.zio.eclipsestore.gigamap.vector.SimilarityFunction,
    a: Array[Float],
    b: Array[Float],
  ): Float =
    function match
      case io.github.riccardomerolla.zio.eclipsestore.gigamap.vector.SimilarityFunction.Cosine     =>
        val dot  = a.indices.foldLeft(0f)((acc, index) => acc + a(index) * b(index))
        val magA = sqrt(a.foldLeft(0f)((acc, value) => acc + value * value).toDouble).toFloat
        val magB = sqrt(b.foldLeft(0f)((acc, value) => acc + value * value).toDouble).toFloat
        if magA == 0f || magB == 0f then 0f else dot / (magA * magB)
      case io.github.riccardomerolla.zio.eclipsestore.gigamap.vector.SimilarityFunction.DotProduct =>
        a.indices.foldLeft(0f)((acc, index) => acc + a(index) * b(index))
      case io.github.riccardomerolla.zio.eclipsestore.gigamap.vector.SimilarityFunction.Euclidean  =>
        -a.indices.foldLeft(0f) { (acc, index) =>
          val delta = a(index) - b(index)
          acc + (delta * delta)
        }

  private def haversineKilometers(a: GeoPoint, b: GeoPoint): Double =
    val earthRadiusKm = 6371.0
    val dLat          = Math.toRadians(b.latitude - a.latitude)
    val dLon          = Math.toRadians(b.longitude - a.longitude)
    val lat1          = Math.toRadians(a.latitude)
    val lat2          = Math.toRadians(b.latitude)
    val x             =
      sin(dLat / 2) * sin(dLat / 2) +
        cos(lat1) * cos(lat2) * sin(dLon / 2) * sin(dLon / 2)
    val c             = 2 * atan2(sqrt(x), sqrt(1 - x))
    earthRadiusKm * c

  private def attempt[A](thunk: => A): IO[GigaMapError, A] =
    ZIO.attempt(thunk).mapError(e => StorageFailure("GigaMap operation failed", Some(e)))
