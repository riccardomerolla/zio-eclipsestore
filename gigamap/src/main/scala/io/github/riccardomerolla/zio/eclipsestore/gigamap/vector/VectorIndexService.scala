package io.github.riccardomerolla.zio.eclipsestore.gigamap.vector

import scala.math.sqrt

import zio.*
import zio.stream.*

/** Top-level service for creating and querying named vector indexes.
  *
  * Acquire it via [[VectorIndexService.live]] and inject with `VectorIndexService` in the ZIO environment.
  *
  * Typical workflow:
  * {{{
  *   for
  *     idx <- VectorIndexService.createIndex[Product](
  *              "products", "similarity",
  *              VectorIndexConfig(768, SimilarityFunction.Cosine),
  *              ProductVectorizer
  *            )
  *     _   <- idx.add(product.id, product)
  *     res <- VectorIndexService.search[Product]("similarity", queryVec, k = 10)
  *   yield res
  * }}}
  */
trait VectorIndexService:

  /** Create a new named vector index or fail with [[VectorError.IndexAlreadyExists]].
    *
    * @param mapName
    *   Logical map / collection this index is associated with (for namespacing).
    * @param indexName
    *   Unique name for the index within this service instance.
    * @param config
    *   Index configuration (dimension, similarity function, persistence settings).
    * @param vectorizer
    *   Strategy for extracting embeddings from entities of type `A`.
    * @return
    *   A handle to the created [[VectorIndex]], ready for `add` / `search` / `persist`.
    */
  def createIndex[A](
    mapName: String,
    indexName: String,
    config: VectorIndexConfig,
    vectorizer: Vectorizer[A],
  ): IO[VectorError, VectorIndex[A]]

  /** Return the existing [[VectorIndex]] with the given name, or fail with [[VectorError.IndexNotFound]].
    */
  def getIndex[A](indexName: String): IO[VectorError, VectorIndex[A]]

  /** Remove an index and its stored data.  Fails with [[VectorError.IndexNotFound]]. */
  def deleteIndex(indexName: String): IO[VectorError, Unit]

  /** Convenience: delegate search to the named index (see [[VectorIndex.search]]). */
  def search[A](
    indexName: String,
    queryEmbedding: Chunk[Float],
    k: Int,
    minScore: Option[Float] = None,
  ): IO[VectorError, List[VectorSearchResult[A]]]

  /** Convenience: delegate streaming search to the named index (see [[VectorIndex.searchStream]]).
    */
  def searchStream[A](
    indexName: String,
    queryEmbedding: Chunk[Float],
    k: Int,
  ): ZStream[Any, VectorError, VectorSearchResult[A]]

object VectorIndexService:

  /** In-memory [[VectorIndexService]] layer. No external dependencies required.
    *
    * Background persistence fibers (if `persistenceIntervalMs` is configured) are started as daemon fibers and run
    * until the ZIO runtime shuts down.
    */
  val live: ULayer[VectorIndexService] =
    ZLayer.fromZIO(
      Ref
        .make(Map.empty[String, VectorIndex[Any]])
        .map(VectorIndexServiceLive.apply)
    )

  // ---- Accessor helpers ------------------------------------------------

  def createIndex[A](
    mapName: String,
    indexName: String,
    config: VectorIndexConfig,
    vectorizer: Vectorizer[A],
  ): ZIO[VectorIndexService, VectorError, VectorIndex[A]] =
    ZIO.serviceWithZIO[VectorIndexService](_.createIndex(mapName, indexName, config, vectorizer))

  def getIndex[A](indexName: String): ZIO[VectorIndexService, VectorError, VectorIndex[A]] =
    ZIO.serviceWithZIO[VectorIndexService](_.getIndex(indexName))

  def deleteIndex(indexName: String): ZIO[VectorIndexService, VectorError, Unit] =
    ZIO.serviceWithZIO[VectorIndexService](_.deleteIndex(indexName))

  def search[A](
    indexName: String,
    queryEmbedding: Chunk[Float],
    k: Int,
    minScore: Option[Float] = None,
  ): ZIO[VectorIndexService, VectorError, List[VectorSearchResult[A]]] =
    ZIO.serviceWithZIO[VectorIndexService](_.search(indexName, queryEmbedding, k, minScore))

  def searchStream[A](
    indexName: String,
    queryEmbedding: Chunk[Float],
    k: Int,
  ): ZStream[VectorIndexService, VectorError, VectorSearchResult[A]] =
    ZStream.serviceWithStream[VectorIndexService](_.searchStream(indexName, queryEmbedding, k))

// ---- Live service -------------------------------------------------------

final private class VectorIndexServiceLive(
  state: Ref[Map[String, VectorIndex[Any]]]
) extends VectorIndexService:

  override def createIndex[A](
    mapName: String,
    indexName: String,
    config: VectorIndexConfig,
    vectorizer: Vectorizer[A],
  ): IO[VectorError, VectorIndex[A]] =
    for
      current  <- state.get
      _        <- ZIO.when(current.contains(indexName))(
                    ZIO.fail(VectorError.IndexAlreadyExists(indexName))
                  )
      indexRef <- Ref.make(Map.empty[Long, (Array[Float], A)])
      index     = VectorIndexLive(config, vectorizer, indexRef)
      _        <- state.update(_ + (indexName -> index.asInstanceOf[VectorIndex[Any]]))
      _        <- ZIO.logInfo(
                    s"Created vector index '$indexName' [map=$mapName, " +
                      s"dim=${config.dimension}, fn=${config.similarityFunction}]"
                  )
      // Schedule optional background persistence as a daemon fiber.
      _        <- config.persistenceIntervalMs.fold(ZIO.unit) { intervalMs =>
                    index.persist.ignore
                      .repeat(Schedule.fixed(intervalMs.millis))
                      .forkDaemon
                      .unit
                  }
    yield index

  override def getIndex[A](indexName: String): IO[VectorError, VectorIndex[A]] =
    state.get.flatMap { current =>
      ZIO
        .fromOption(current.get(indexName))
        .orElseFail(VectorError.IndexNotFound(indexName))
        .map(_.asInstanceOf[VectorIndex[A]])
    }

  override def deleteIndex(indexName: String): IO[VectorError, Unit] =
    for
      current <- state.get
      _       <- ZIO.when(!current.contains(indexName))(
                   ZIO.fail(VectorError.IndexNotFound(indexName))
                 )
      _       <- state.update(_ - indexName)
      _       <- ZIO.logInfo(s"Deleted vector index '$indexName'")
    yield ()

  override def search[A](
    indexName: String,
    queryEmbedding: Chunk[Float],
    k: Int,
    minScore: Option[Float] = None,
  ): IO[VectorError, List[VectorSearchResult[A]]] =
    getIndex[A](indexName).flatMap(_.search(queryEmbedding, k, minScore))

  override def searchStream[A](
    indexName: String,
    queryEmbedding: Chunk[Float],
    k: Int,
  ): ZStream[Any, VectorError, VectorSearchResult[A]] =
    ZStream
      .fromZIO(getIndex[A](indexName))
      .flatMap(_.searchStream(queryEmbedding, k))

// ---- Live index ---------------------------------------------------------

final private class VectorIndexLive[A](
  val config: VectorIndexConfig,
  vectorizer: Vectorizer[A],
  state: Ref[Map[Long, (Array[Float], A)]],
) extends VectorIndex[A]:

  override def add(id: Long, entity: A): IO[VectorError, Unit] =
    val embedding = vectorizer.vectorize(entity).toArray
    ZIO.when(embedding.length != config.dimension)(
      ZIO.fail(VectorError.DimensionMismatch(config.dimension, embedding.length))
    ) *>
      state.update(_ + (id -> (embedding, entity)))

  override def remove(id: Long): IO[VectorError, Unit] =
    state.update(_ - id)

  override def size: IO[VectorError, Int] =
    state.get.map(_.size)

  override def search(
    queryEmbedding: Chunk[Float],
    k: Int,
    minScore: Option[Float] = None,
  ): IO[VectorError, List[VectorSearchResult[A]]] =
    val qv = queryEmbedding.toArray
    ZIO.when(qv.length != config.dimension)(
      ZIO.fail(VectorError.DimensionMismatch(config.dimension, qv.length))
    ) *>
      state.get.flatMap { entries =>
        ZIO
          .attempt {
            val fn = config.similarityFunction
            entries.iterator
              .map {
                case (id, (embedding, entity)) =>
                  VectorSearchResult(entity, id, score(fn, qv, embedding))
              }
              .filter(r => minScore.forall(r.score >= _))
              .toList
              .sortBy(-_.score)
              .take(k)
          }
          .mapError(e => VectorError.StorageFailure("Vector search failed", Some(e)))
      }

  override def searchStream(
    queryEmbedding: Chunk[Float],
    k: Int,
  ): ZStream[Any, VectorError, VectorSearchResult[A]] =
    ZStream
      .fromZIO(search(queryEmbedding, k))
      .flatMap(ZStream.fromIterable(_))

  override def persist: IO[VectorError, Unit] =
    ZIO.logInfo(
      s"[VectorIndex] persist checkpoint (dim=${config.dimension}, fn=${config.similarityFunction})"
      // NOTE: When the EclipseStore JVector API is available on Maven Central,
      // this is where the native index flush will be invoked.
    )

  // ---- Pure similarity maths ------------------------------------------

  private def score(fn: SimilarityFunction, a: Array[Float], b: Array[Float]): Float =
    fn match
      case SimilarityFunction.Cosine =>
        val dot  = dotProduct(a, b)
        val magA = magnitude(a)
        val magB = magnitude(b)
        if magA == 0f || magB == 0f then 0f
        else (dot / (magA * magB)).toFloat

      case SimilarityFunction.DotProduct =>
        dotProduct(a, b)

      case SimilarityFunction.Euclidean =>
        -euclideanDistanceSq(a, b)

  private def dotProduct(a: Array[Float], b: Array[Float]): Float =
    a.indices.foldLeft(0f)((acc, i) => acc + a(i) * b(i))

  private def magnitude(a: Array[Float]): Float =
    sqrt(a.foldLeft(0f)((acc, v) => acc + v * v).toDouble).toFloat

  private def euclideanDistanceSq(a: Array[Float], b: Array[Float]): Float =
    a.indices.foldLeft(0f) { (acc, i) =>
      val d = a(i) - b(i)
      acc + d * d
    }
