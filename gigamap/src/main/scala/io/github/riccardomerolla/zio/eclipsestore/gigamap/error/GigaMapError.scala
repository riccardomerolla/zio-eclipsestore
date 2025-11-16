package io.github.riccardomerolla.zio.eclipsestore.gigamap.error

sealed trait GigaMapError extends Throwable

object GigaMapError:
  final case class StorageFailure(message: String, cause: Option[Throwable] = None) extends GigaMapError:
    override def getMessage: String         = message
    override def getCause: Throwable | Null = cause.orNull

  final case class IndexNotDefined(index: String) extends GigaMapError:
    override def getMessage: String = s"Index '$index' is not defined for this GigaMap"

  case object EmptyRegistry extends GigaMapError:
    override def getMessage: String = "GigaMap registry is not initialized"
