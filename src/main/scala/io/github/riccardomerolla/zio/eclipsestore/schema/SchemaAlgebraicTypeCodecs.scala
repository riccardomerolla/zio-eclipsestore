package io.github.riccardomerolla.zio.eclipsestore.schema

import zio.schema.Schema

import org.eclipse.serializer.persistence.binary.types.BinaryTypeHandler

private[schema] object SchemaAlgebraicTypeCodecs:
  def handlerFor[A](runtimeClass: Class[A], schema: Schema[A], typeId: Long): Option[BinaryTypeHandler[A]] =
    schema match
      case _: Schema.Optional[?] => Some(SchemaBinaryCodec.jsonPayloadHandler(runtimeClass, schema, typeId))
      case _: Schema.Enum[?]     => Some(SchemaBinaryCodec.jsonPayloadHandler(runtimeClass, schema, typeId))
      case _                     => None
