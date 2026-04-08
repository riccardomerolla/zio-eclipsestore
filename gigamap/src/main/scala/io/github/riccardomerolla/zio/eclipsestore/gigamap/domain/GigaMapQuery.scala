package io.github.riccardomerolla.zio.eclipsestore.gigamap.domain

import zio.Chunk

import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.GeoPoint

sealed trait GigaMapQuery[V, +A]

sealed trait GigaMapPredicate[V]

final case class GigaMapScored[V](value: V, score: Float)

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
  final case class VectorSearch[V](
    indexName: String,
    vector: Chunk[Float],
    limit: Int = 10,
    minScore: Option[Float] = None,
  ) extends GigaMapQuery[V, Chunk[GigaMapScored[V]]]
  final case class SpatialRadius[V](
    indexName: String,
    center: GeoPoint,
    radiusKilometers: Double,
  ) extends GigaMapQuery[V, Chunk[V]]
  final case class Composite[V](
    predicates: Chunk[GigaMapPredicate[V]]
  ) extends GigaMapQuery[V, Chunk[V]]
  final case class Count[V]()                             extends GigaMapQuery[V, Long]

object GigaMapPredicate:
  final case class Filter[V](predicate: V => Boolean)     extends GigaMapPredicate[V]
  final case class ByIndex[V, A](index: String, value: A) extends GigaMapPredicate[V]
  final case class VectorSearch[V](
    indexName: String,
    vector: Chunk[Float],
    limit: Int = 10,
    minScore: Option[Float] = None,
  ) extends GigaMapPredicate[V]
  final case class SpatialRadius[V](
    indexName: String,
    center: GeoPoint,
    radiusKilometers: Double,
  ) extends GigaMapPredicate[V]
