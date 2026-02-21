package io.github.riccardomerolla.zio.eclipsestore.schema

import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

import zio.Chunk
import zio.schema.Schema
import zio.schema.codec.JsonCodec

import org.eclipse.serializer.persistence.binary.types.{
  AbstractBinaryHandlerCustom,
  AbstractBinaryHandlerCustomValueVariableLength,
  Binary,
  BinaryTypeHandler,
}
import org.eclipse.serializer.persistence.types.{ PersistenceLoadHandler, PersistenceStoreHandler }

/** Derives EclipseStore binary type handlers from ZIO Schema. */
object SchemaBinaryCodec:
  private val PayloadField = "schemaPayload"

  /** Derives a `BinaryTypeHandler[A]` from `Schema[A]` using an explicit type id. */
  def handler[A: ClassTag](schema: Schema[A], typeId: Long): BinaryTypeHandler[A] =
    val runtimeClass = boxedClass(summon[ClassTag[A]].runtimeClass).asInstanceOf[Class[A]]
    SchemaStandardTypeCodecs
      .handlerFor(runtimeClass, schema, typeId)
      .getOrElse(jsonPayloadHandler(runtimeClass, schema, typeId))

  /** Derives a `BinaryTypeHandler[A]` from `Schema[A]` using a deterministic type id. */
  def handler[A: ClassTag](schema: Schema[A]): BinaryTypeHandler[A] =
    handler(schema, stableTypeId(schema))

  private def stableTypeId[A](schema: Schema[A]): Long =
    val hash = MurmurHash3.stringHash(schema.ast.toString)
    val id   = java.lang.Integer.toUnsignedLong(hash)
    if id == 0L then 1L else id

  private[schema] def jsonPayloadHandler[A](runtimeClass: Class[A], schema: Schema[A], typeId: Long): BinaryTypeHandler[A] =
    new SchemaBackedBinaryTypeHandler[A](runtimeClass, schema).initialize(typeId).asInstanceOf[BinaryTypeHandler[A]]

  private def boxedClass(cls: Class[?]): Class[?] =
    if !cls.isPrimitive then cls
    else if cls == java.lang.Boolean.TYPE then classOf[java.lang.Boolean]
    else if cls == java.lang.Byte.TYPE then classOf[java.lang.Byte]
    else if cls == java.lang.Short.TYPE then classOf[java.lang.Short]
    else if cls == java.lang.Character.TYPE then classOf[java.lang.Character]
    else if cls == java.lang.Integer.TYPE then classOf[java.lang.Integer]
    else if cls == java.lang.Long.TYPE then classOf[java.lang.Long]
    else if cls == java.lang.Float.TYPE then classOf[java.lang.Float]
    else if cls == java.lang.Double.TYPE then classOf[java.lang.Double]
    else if cls == java.lang.Void.TYPE then classOf[scala.runtime.BoxedUnit]
    else cls

  private final class SchemaBackedBinaryTypeHandler[A](
    runtimeClass: Class[A],
    schema: Schema[A],
  ) extends AbstractBinaryHandlerCustomValueVariableLength[A, Int](
        runtimeClass,
        AbstractBinaryHandlerCustom.CustomFields(AbstractBinaryHandlerCustom.bytes(PayloadField)),
      ):
    private val codec = JsonCodec.schemaBasedBinaryCodec(schema)

    override def store(
      data: Binary,
      instance: A,
      objectId: Long,
      handler: PersistenceStoreHandler[Binary],
    ): Unit =
      val encoded = encode(instance)
      data.store_bytes(typeId(), objectId, encoded)

    override def create(data: Binary, handler: PersistenceLoadHandler): A =
      decode(data.create_bytes())

    override def getValidationStateFromInstance(instance: A): Int =
      MurmurHash3.bytesHash(encode(instance))

    override def getValidationStateFromBinary(data: Binary): Int =
      MurmurHash3.bytesHash(data.build_bytes())

    private def encode(value: A): Array[Byte] =
      codec.encode(value).asInstanceOf[Chunk[Byte]].toArray

    private def decode(bytes: Array[Byte]): A =
      codec.decode(Chunk.fromArray(bytes)) match
        case Right(value) => value
        case Left(error)  =>
          throw new IllegalStateException(s"Failed to decode schema payload for ${runtimeClass.getName}: $error")
