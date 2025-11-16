package io.github.riccardomerolla.zio.eclipsestore.domain

import io.github.riccardomerolla.zio.eclipsestore.domain.RootContext

/** Represents a query operation that can be executed against EclipseStore */
sealed trait Query[+A]:
  /** Unique identifier for this query, used for batching similar queries */
  def queryId: String

  /** Indicates whether this query can be batched with other queries */
  def batchable: Boolean

object Query:
  /** A query that retrieves data by a key */
  final case class Get[K, V](key: K, queryId: String = "get") extends Query[Option[V]]:
    override def batchable: Boolean = true

  /** A query that stores data with a key */
  final case class Put[K, V](key: K, value: V, queryId: String = "put") extends Query[Unit]:
    override def batchable: Boolean = true

  /** A query that deletes data by a key */
  final case class Delete[K](key: K, queryId: String = "delete") extends Query[Unit]:
    override def batchable: Boolean = true

  /** A query that retrieves all keys */
  final case class GetAllKeys[K](queryId: String = "getAllKeys") extends Query[Set[K]]:
    override def batchable: Boolean = false

  /** A query that retrieves all values */
  final case class GetAllValues[V](queryId: String = "getAllValues") extends Query[List[V]]:
    override def batchable: Boolean = false

  /** A custom query that may not be batchable */
  final case class Custom[A](
      operation: String,
      run: RootContext => A,
      batchable: Boolean = false,
      queryId: String = "custom",
    ) extends Query[A]
