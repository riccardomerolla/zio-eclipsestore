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
  private val PayloadField = "schemaJsonPayload"

  /** Derives a `BinaryTypeHandler[A]` from `Schema[A]` using an explicit type id. */
  def handler[A: ClassTag](schema: Schema[A], typeId: Long): BinaryTypeHandler[A] =
    val runtimeClass = boxedClass(summon[ClassTag[A]].runtimeClass).asInstanceOf[Class[A]]
    handler(schema, runtimeClass, typeId)

  /** Derives a `BinaryTypeHandler[A]` from `Schema[A]` with explicit runtime class and type id. */
  def handler[A](schema: Schema[A], runtimeClass: Class[A], typeId: Long): BinaryTypeHandler[A] =
    val boxedRuntimeClass = boxedClass(runtimeClass).asInstanceOf[Class[A]]
    SchemaAlgebraicTypeCodecs
      .handlerFor(boxedRuntimeClass, schema, typeId)
      .orElse(SchemaStandardTypeCodecs.handlerFor(boxedRuntimeClass, schema, typeId))
      .getOrElse(jsonPayloadHandler(boxedRuntimeClass, schema, typeId))

  /** Derives a `BinaryTypeHandler[A]` from `Schema[A]` with explicit runtime class and deterministic type id. */
  def handler[A](schema: Schema[A], runtimeClass: Class[A]): BinaryTypeHandler[A] =
    handler(schema, runtimeClass, stableTypeId(schema))

  /** Derives all handlers needed for a schema and runtime class, including enum case subtype handlers. */
  def handlers[A](schema: Schema[A], runtimeClass: Class[A]): Chunk[BinaryTypeHandler[?]] =
    val primary = handler(schema, runtimeClass)
    val extras  = enumCaseSubtypeHandlers(schema, runtimeClass)
    Chunk.single(primary) ++ extras.filterNot(_.`type`() == primary.`type`())

  /** Derives a `BinaryTypeHandler[A]` from `Schema[A]` using an explicit type id. */
  private def handlerFromTag[A: ClassTag](schema: Schema[A], typeId: Long): BinaryTypeHandler[A] =
    val runtimeClass = boxedClass(summon[ClassTag[A]].runtimeClass).asInstanceOf[Class[A]]
    SchemaAlgebraicTypeCodecs
      .handlerFor(runtimeClass, schema, typeId)
      .orElse(SchemaStandardTypeCodecs.handlerFor(runtimeClass, schema, typeId))
      .getOrElse(jsonPayloadHandler(runtimeClass, schema, typeId))

  /** Derives a `BinaryTypeHandler[A]` from `Schema[A]` using a deterministic type id. */
  def handler[A: ClassTag](schema: Schema[A]): BinaryTypeHandler[A] =
    handlerFromTag(schema, stableTypeId(schema))

  /** Derives all handlers needed for a schema. For enums, this includes case runtime subtype handlers. */
  def handlers[A: ClassTag](schema: Schema[A]): Chunk[BinaryTypeHandler[?]] =
    val primary      = handler(schema)
    val runtimeClass = boxedClass(summon[ClassTag[A]].runtimeClass).asInstanceOf[Class[A]]
    val extras       = enumCaseSubtypeHandlers(schema, runtimeClass)
    Chunk.single(primary) ++ extras.filterNot(_.`type`() == primary.`type`())

  private def stableTypeId[A](schema: Schema[A]): Long =
    val hash = MurmurHash3.stringHash(schema.ast.toString)
    val id   = java.lang.Integer.toUnsignedLong(hash)
    if id == 0L then 1L else id

  private def stableTypeIdForClass[A](schema: Schema[A], className: String): Long =
    val hash = MurmurHash3.stringHash(s"${schema.ast}|$className")
    val id   = java.lang.Integer.toUnsignedLong(hash)
    if id == 0L then 1L else id

  private[schema] def jsonPayloadHandler[A](runtimeClass: Class[A], schema: Schema[A], typeId: Long)
    : BinaryTypeHandler[A] =
    new SchemaBackedBinaryTypeHandler[A](runtimeClass, schema).initialize(typeId).asInstanceOf[BinaryTypeHandler[A]]

  private def enumCaseSubtypeHandlers[A](schema: Schema[A], outerClass: Class[A]): Chunk[BinaryTypeHandler[?]] =
    schema match
      case enumSchema: Schema.Enum[A @unchecked] =>
        enumSchema.cases.foldLeft(Chunk.empty[BinaryTypeHandler[?]]) { (acc, enumCase) =>
          val caseClassOpt: Option[Class[A]] =
            enumCase.schema.defaultValue.toOption match
              case Some(defaultCase) =>
                val instance = enumCase.construct(defaultCase).asInstanceOf[AnyRef]
                Some(instance.getClass.asInstanceOf[Class[A]])
              case None              =>
                // Fallback for cases with no defaultValue (e.g. fields using transformOrFail schemas
                // that reject the ZIO Schema-generated empty-string / zero default).
                // Scala 3 enums encode sub-cases as OuterClass$CaseName on the JVM;
                // enumCase.id is the unqualified case name (e.g. "Assigned").
                // This heuristic works for Scala 3 enum cases nested inside the outer enum class.
                // For sealed-trait subtypes defined at the top level the lookup will fail (ClassNotFoundException)
                // and the case will still be skipped — the same pre-existing behaviour.
                scala.util.Try(
                  Class.forName(s"${outerClass.getName}$$${enumCase.id}").asInstanceOf[Class[A]]
                ).toOption
          caseClassOpt match
            case None            => acc
            case Some(caseClass) =>
              if caseClass.getName.contains("$$anon$") then acc
              else
                val caseTypeId = stableTypeIdForClass(schema, caseClass.getName)
                acc :+ jsonPayloadHandler(caseClass, schema, caseTypeId)
        }
      case _                                     => Chunk.empty

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

  final private class SchemaBackedBinaryTypeHandler[A](
    runtimeClass: Class[A],
    schema: Schema[A],
  ) extends AbstractBinaryHandlerCustomValueVariableLength[A, Int](
      runtimeClass,
      AbstractBinaryHandlerCustom.CustomFields(AbstractBinaryHandlerCustom.chars(PayloadField)),
    ):
    private val codec = JsonCodec.jsonCodec(schema)

    override def store(
      data: Binary,
      instance: A,
      objectId: Long,
      handler: PersistenceStoreHandler[Binary],
    ): Unit =
      data.storeStringSingleValue(typeId(), objectId, encode(instance))

    override def create(data: Binary, handler: PersistenceLoadHandler): A =
      decode(data.buildString())

    override def getValidationStateFromInstance(instance: A): Int =
      MurmurHash3.stringHash(encode(instance))

    override def getValidationStateFromBinary(data: Binary): Int =
      MurmurHash3.stringHash(data.buildString())

    override def guaranteeSpecificInstanceViablity(): Unit = ()

    override def isSpecificInstanceViable(): Boolean = true

    override def guaranteeSubTypeInstanceViablity(): Unit = ()

    override def isSubTypeInstanceViable(): Boolean = true

    private def encode(value: A): String =
      codec.encodeJson(value, None).toString

    private def decode(json: String): A =
      codec.decodeJson(json).fold(
        error => scala.sys.error(s"Failed to decode schema payload for ${runtimeClass.getName}: $error"),
        identity,
      )
