package io.github.riccardomerolla.zio.eclipsestore.gigamap.vector

/** Domain errors for vector index operations. */
sealed trait VectorError extends Throwable

object VectorError:

  /** No index with the given name has been registered in [[VectorIndexService]]. */
  final case class IndexNotFound(name: String) extends VectorError:
    override def getMessage: String = s"Vector index '$name' not found"

  /** An index with this name was already created. */
  final case class IndexAlreadyExists(name: String) extends VectorError:
    override def getMessage: String = s"Vector index '$name' already exists"

  /** The supplied embedding has a different dimensionality than the index was configured with. */
  final case class DimensionMismatch(expected: Int, actual: Int) extends VectorError:
    override def getMessage: String =
      s"Embedding dimension mismatch: expected $expected, got $actual"

  /** A low-level storage or computation failure. */
  final case class StorageFailure(message: String, cause: Option[Throwable] = None)
      extends VectorError:
    override def getMessage: String         = message
    override def getCause: Throwable | Null = cause.orNull
