package io.github.riccardomerolla.zio.eclipsestore.gigamap.config

import zio.Chunk

/** Describes a named GigaMap instance together with optional indexes. */
final case class GigaMapDefinition[K, V](
    name: String,
    indexes: Chunk[GigaMapIndex[V, ?]] = Chunk.empty,
    autoPersist: Boolean = true,
  ):
  private[gigamap] val anyIndexes: Chunk[GigaMapIndex[V, Any]] =
    indexes.map(_.toAny)

final case class GigaMapIndex[V, A](name: String, extract: V => A):
  private[gigamap] def toAny: GigaMapIndex[V, Any] =
    GigaMapIndex(name, (value: V) => extract(value).asInstanceOf[Any])
