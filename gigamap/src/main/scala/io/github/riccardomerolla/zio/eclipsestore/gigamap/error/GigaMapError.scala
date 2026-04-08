package io.github.riccardomerolla.zio.eclipsestore.gigamap.error

import io.github.riccardomerolla.zio.eclipsestore.error.PersistenceError

sealed trait GigaMapError extends Throwable with PersistenceError

object GigaMapError:
  final case class StorageFailure(message: String, cause: Option[Throwable] = None) extends GigaMapError:
    override def getMessage: String         = message
    override def getCause: Throwable | Null = cause.orNull

  final case class IndexNotDefined(index: String) extends GigaMapError:
    override def getMessage: String = s"Index '$index' is not defined for this GigaMap"

  final case class InvalidVectorQuery(index: String, expectedDimension: Int, actualDimension: Int) extends GigaMapError:
    override def getMessage: String =
      s"Vector query for index '$index' expected dimension $expectedDimension but received $actualDimension"

  final case class InvalidSpatialQuery(index: String, radiusKilometers: Double) extends GigaMapError:
    override def getMessage: String =
      s"Spatial query for index '$index' requires a non-negative radius, received $radiusKilometers"

  case object EmptyRegistry extends GigaMapError:
    override def getMessage: String = "GigaMap registry is not initialized"
