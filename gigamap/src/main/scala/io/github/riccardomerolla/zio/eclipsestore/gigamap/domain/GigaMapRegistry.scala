package io.github.riccardomerolla.zio.eclipsestore.gigamap.domain

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentHashMap.KeySetView

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor

/** Root container that stores all GigaMap instances and their indexes. */
final case class GigaMapRegistry(
    maps: ConcurrentHashMap[String, ConcurrentHashMap[Any, Any]],
    indexes: ConcurrentHashMap[
      String,
      ConcurrentHashMap[String, ConcurrentHashMap[Any, KeySetView[Any, java.lang.Boolean]]],
    ],
  ) extends Serializable:

  def mapFor(name: String): ConcurrentHashMap[Any, Any] =
    maps.computeIfAbsent(name, _ => new ConcurrentHashMap[Any, Any]())

  def indexesFor(name: String): ConcurrentHashMap[String, ConcurrentHashMap[Any, KeySetView[Any, java.lang.Boolean]]] =
    indexes.computeIfAbsent(
      name,
      _ => new ConcurrentHashMap[String, ConcurrentHashMap[Any, KeySetView[Any, java.lang.Boolean]]](),
    )

object GigaMapRegistry:
  val descriptor: RootDescriptor[GigaMapRegistry] =
    RootDescriptor(
      id = "gigamap-registry",
      initializer = () =>
        GigaMapRegistry(
          new ConcurrentHashMap[String, ConcurrentHashMap[Any, Any]](),
          new ConcurrentHashMap[
            String,
            ConcurrentHashMap[String, ConcurrentHashMap[Any, KeySetView[Any, java.lang.Boolean]]],
          ](),
        ),
    )
