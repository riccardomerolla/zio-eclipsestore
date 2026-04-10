package io.github.riccardomerolla.zio.eclipsestore.schema

import scala.util.hashing.MurmurHash3

import zio.schema.Schema

object SchemaIntrospection:
  def topLevelFields[A](schema: Schema[A]): Map[String, String] =
    schema match
      case record: Schema.Record[A] =>
        record.fields.map(field => field.name -> stableShape(field.schema.asInstanceOf[Schema[?]])).toMap
      case _                        =>
        Map.empty

  def stableShape[A](schema: Schema[A]): String =
    schema.ast.toString

  def fingerprint[A](schema: Schema[A]): String =
    java.lang.Integer.toUnsignedString(MurmurHash3.stringHash(stableShape(schema)), 16)
