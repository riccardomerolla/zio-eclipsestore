package io.github.riccardomerolla.zio.eclipsestore.gigamap.vector

import zio.Chunk

/** Converts an entity of type `A` into a fixed-length float embedding vector.
  *
  * Implement this trait for each entity type you want to index for vector similarity search.
  *
  * Example:
  * {{{
  *   case class Article(id: Long, title: String, embedding: Chunk[Float])
  *
  *   object ArticleVectorizer extends Vectorizer[Article]:
  *     def vectorize(a: Article): Chunk[Float] = a.embedding
  *     def isEmbedded: Boolean = true
  * }}}
  */
trait Vectorizer[A]:

  /** Extract the embedding vector from an entity of type `A`.
    *
    * The returned `Chunk` must have exactly `dimension` floats as specified in [[VectorIndexConfig]].
    */
  def vectorize(entity: A): Chunk[Float]

  /** Whether entities of type `A` already carry pre-computed embeddings.
    *
    * Set to `false` when embeddings must be computed externally (e.g. via an embedding model API) before being added to
    * the index.
    */
  def isEmbedded: Boolean
