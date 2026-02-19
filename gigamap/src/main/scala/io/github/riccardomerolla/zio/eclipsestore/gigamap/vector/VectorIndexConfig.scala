package io.github.riccardomerolla.zio.eclipsestore.gigamap.vector

import java.nio.file.Path

/** Configuration for a named vector index, mirroring EclipseStore's `VectorIndexConfiguration` builder API.
  *
  * @param dimension
  *   Number of dimensions in every embedding vector. Must match the output dimension of your embedding model.
  * @param similarityFunction
  *   Distance function used during search. See [[SimilarityFunction]].
  * @param maxDegree
  *   Maximum number of graph edges per HNSW node. Higher values improve recall at the cost of memory and build time.
  *   Typical range: 8–64 (default 16).
  * @param beamWidth
  *   Beam width used during HNSW graph construction. Higher values produce a better-quality graph but are slower to
  *   build. Typical range: 50–500 (default 100).
  * @param onDisk
  *   When `true`, the index is persisted to [[indexDirectory]].
  * @param indexDirectory
  *   Directory for on-disk persistence; required when `onDisk = true`.
  * @param enablePqCompression
  *   Enable product-quantisation (PQ) compression to reduce on-disk size.
  * @param pqSubspaces
  *   Number of PQ subspaces. Must evenly divide `dimension`. Only used when `enablePqCompression = true`.
  * @param persistenceIntervalMs
  *   If set, a background fiber will flush the index to disk every `n` milliseconds.
  * @param minChangesBetweenPersists
  *   Minimum number of add/remove operations that must accumulate before a background flush is triggered.
  */
final case class VectorIndexConfig(
  dimension: Int,
  similarityFunction: SimilarityFunction,
  maxDegree: Int = 16,
  beamWidth: Int = 100,
  onDisk: Boolean = false,
  indexDirectory: Option[Path] = None,
  enablePqCompression: Boolean = false,
  pqSubspaces: Option[Int] = None,
  persistenceIntervalMs: Option[Long] = None,
  minChangesBetweenPersists: Option[Int] = None,
)
