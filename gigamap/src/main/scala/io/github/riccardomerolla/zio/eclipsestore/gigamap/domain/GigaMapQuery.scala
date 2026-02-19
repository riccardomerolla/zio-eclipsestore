package io.github.riccardomerolla.zio.eclipsestore.gigamap.domain

import zio.Chunk

sealed trait GigaMapQuery[V, +A]

object GigaMapQuery:
  final case class All[V]()                               extends GigaMapQuery[V, Chunk[V]]
  final case class Filter[V](predicate: V => Boolean)     extends GigaMapQuery[V, Chunk[V]]
  final case class ByIndex[V, A](index: String, value: A) extends GigaMapQuery[V, Chunk[V]]
  final case class VectorSimilarity[V](
    indexName: String,
    vector: Array[Float],
    limit: Int = 10,
    threshold: Option[Float] = None,
  ) extends GigaMapQuery[V, Chunk[V]]
  final case class Count[V]()                             extends GigaMapQuery[V, Long]
