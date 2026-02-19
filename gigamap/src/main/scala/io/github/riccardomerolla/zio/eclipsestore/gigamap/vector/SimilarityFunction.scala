package io.github.riccardomerolla.zio.eclipsestore.gigamap.vector

/** Similarity function used when comparing embedding vectors.
  *
  * Choosing the right function depends on your embedding model:
  *
  *   - '''Cosine''' is the safest default for text/semantic embeddings produced by most transformer
  *     models (OpenAI, Sentence-Transformers, etc.). It measures the angle between vectors and is
  *     invariant to vector magnitude.
  *
  *   - '''DotProduct''' equals cosine when embeddings are L2-normalised but avoids the division;
  *     use it when your model guarantees unit-length outputs (e.g. some OpenAI models with
  *     "text-embedding-3-*" normalised).
  *
  *   - '''Euclidean''' measures geometric distance; useful for models that encode semantic distance
  *     as spatial proximity (e.g. some image and audio embeddings). Scores are reported as the
  *     negated squared L2 distance so that higher = more similar.
  */
enum SimilarityFunction:
  /** Cosine similarity: cos(θ) = (A·B) / (|A||B|). Range [–1, 1]. */
  case Cosine

  /** Dot-product similarity: A·B (unnormalised). Unbounded range. */
  case DotProduct

  /** Euclidean similarity: –||A–B||². Range (–∞, 0]. */
  case Euclidean
