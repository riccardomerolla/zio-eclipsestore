package io.github.riccardomerolla.zio.eclipsestore.schema

import org.eclipse.serializer.persistence.binary.types.{ Binary, BinaryTypeHandler }
import org.eclipse.serializer.persistence.types.PersistenceTypeHandler

/** Scala-native wrapper around an EclipseStore binary type handler. */
final case class TypeHandler[+A](
  runtimeClass: Class[?],
  typeId: Long,
  underlying: BinaryTypeHandler[?],
):
  def persistenceHandler: PersistenceTypeHandler[Binary, ?] =
    underlying

object TypeHandler:
  private[schema] def fromBinary[A](handler: BinaryTypeHandler[A]): TypeHandler[A] =
    TypeHandler(
      runtimeClass = handler.`type`(),
      typeId = handler.typeId(),
      underlying = handler,
    )

  private[schema] def fromUntyped(handler: BinaryTypeHandler[?]): TypeHandler[?] =
    TypeHandler(
      runtimeClass = handler.`type`(),
      typeId = handler.typeId(),
      underlying = handler,
    )
