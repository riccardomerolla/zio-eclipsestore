package io.github.riccardomerolla.zio.eclipsestore.gigamap.vector

/** A single result returned from a vector similarity search.
  *
  * @param entity
  *   The matched entity, in its original type.
  * @param entityId
  *   The `Long` key that was supplied when the entity was added to the index.
  * @param score
  *   Similarity score. Higher = more similar. The range depends on the [[SimilarityFunction]] configured for the index:
  *   - `Cosine` → [–1, 1]
  *   - `DotProduct` → (–∞, +∞) (unbounded; normalised embeddings give [–1, 1])
  *   - `Euclidean` → (–∞, 0] (negated squared L2 distance)
  */
final case class VectorSearchResult[+A](entity: A, entityId: Long, score: Float)
