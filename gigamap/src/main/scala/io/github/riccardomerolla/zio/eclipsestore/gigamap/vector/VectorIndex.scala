package io.github.riccardomerolla.zio.eclipsestore.gigamap.vector

import zio.*
import zio.stream.*

/** A handle to a single named vector index obtained from [[VectorIndexService.createIndex]].
  *
  * All operations are effect-typed and composable within ZIO.
  *
  * @tparam A
  *   The entity type stored in this index.
  */
trait VectorIndex[A]:

  /** The configuration snapshot this index was created with. */
  def config: VectorIndexConfig

  /** Add or update an entity by its numeric id.
    *
    * The embedding is extracted automatically via the [[Vectorizer]] supplied at index creation. Fails with
    * [[VectorError.DimensionMismatch]] if the extracted embedding length does not match
    * [[VectorIndexConfig.dimension]].
    */
  def add(id: Long, entity: A): IO[VectorError, Unit]

  /** Remove an entity by its numeric id. No-op if the id does not exist. */
  def remove(id: Long): IO[VectorError, Unit]

  /** Current number of indexed entities. */
  def size: IO[VectorError, Int]

  /** Return the `k` nearest neighbours to `queryEmbedding`, ranked by similarity score (highest first).
    *
    * @param queryEmbedding
    *   The query vector; must have exactly [[VectorIndexConfig.dimension]] components.
    * @param k
    *   Maximum number of results to return.
    * @param minScore
    *   When provided, only results with `score >= minScore` are included.
    */
  def search(
    queryEmbedding: Chunk[Float],
    k: Int,
    minScore: Option[Float] = None,
  ): IO[VectorError, List[VectorSearchResult[A]]]

  /** Stream search results one element at a time; useful for large `k` or downstream processing.
    *
    * Equivalent to `ZStream.fromZIO(search(...)).flatMap(ZStream.fromIterable(_))`.
    */
  def searchStream(
    queryEmbedding: Chunk[Float],
    k: Int,
  ): ZStream[Any, VectorError, VectorSearchResult[A]]

  /** Flush in-memory state to the configured on-disk store.
    *
    * No-op when `VectorIndexConfig.onDisk = false`. Background flushes are scheduled automatically if
    * [[VectorIndexConfig.persistenceIntervalMs]] is set.
    */
  def persist: IO[VectorError, Unit]
