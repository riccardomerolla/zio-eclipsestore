package io.github.riccardomerolla.zio.eclipsestore.schema

import java.net.{ URI, URL }
import java.time.*
import java.util.UUID

import scala.math.{ BigDecimal, BigInt }

import zio.schema.Schema

import org.eclipse.serializer.persistence.binary.java.lang.*
import org.eclipse.serializer.persistence.binary.java.net.{ BinaryHandlerURI, BinaryHandlerURL }
import org.eclipse.serializer.persistence.binary.java.time.{ BinaryHandlerLocalDate, BinaryHandlerPeriod }
import org.eclipse.serializer.persistence.binary.types.{
  AbstractBinaryHandlerCustom,
  AbstractBinaryHandlerCustomValueFixedLength,
  AbstractBinaryHandlerCustomValueVariableLength,
  Binary,
  BinaryTypeHandler,
}
import org.eclipse.serializer.persistence.types.{ PersistenceLoadHandler, PersistenceStoreHandler }

private[schema] object SchemaStandardTypeCodecs:
  def handlerFor[A](runtimeClass: Class[A], schema: Schema[A], typeId: Long): Option[BinaryTypeHandler[A]] =
    val cls = runtimeClass
    if cls == classOf[String] then Some(BinaryHandlerString.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[java.lang.Boolean] then Some(BinaryHandlerBoolean.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[java.lang.Byte] then Some(BinaryHandlerByte.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[java.lang.Short] then Some(BinaryHandlerShort.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[java.lang.Integer] then Some(BinaryHandlerInteger.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[java.lang.Long] then Some(BinaryHandlerLong.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[java.lang.Float] then Some(BinaryHandlerFloat.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[java.lang.Double] then Some(BinaryHandlerDouble.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[java.lang.Character] then
      Some(BinaryHandlerCharacter.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[java.math.BigDecimal] then
      Some(initCast[A](
        stringEncoded(classOf[java.math.BigDecimal], _.toString, s => new java.math.BigDecimal(s)),
        typeId,
      ))
    else if cls == classOf[java.math.BigInteger] then
      Some(initCast[A](
        stringEncoded(classOf[java.math.BigInteger], _.toString, s => new java.math.BigInteger(s)),
        typeId,
      ))
    else if cls == classOf[URI] then Some(BinaryHandlerURI.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[URL] then Some(BinaryHandlerURL.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[LocalDate] then Some(BinaryHandlerLocalDate.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[Period] then Some(BinaryHandlerPeriod.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls == classOf[scala.runtime.BoxedUnit] then
      Some(SchemaBinaryCodec.jsonPayloadHandler(runtimeClass, schema, typeId))
    else if cls == classOf[Instant] then Some(initCast[A](new InstantHandler(), typeId))
    else if cls == classOf[BigDecimal] then Some(initCast[A](new ScalaBigDecimalHandler(), typeId))
    else if cls == classOf[BigInt] then Some(initCast[A](new ScalaBigIntHandler(), typeId))
    else if cls == classOf[UUID] then Some(initCast[A](new UuidHandler(), typeId))
    else if cls == classOf[LocalDateTime] then
      Some(initCast[A](stringEncoded(classOf[LocalDateTime], _.toString, LocalDateTime.parse), typeId))
    else if cls == classOf[LocalTime] then
      Some(initCast[A](stringEncoded(classOf[LocalTime], _.toString, LocalTime.parse), typeId))
    else if cls == classOf[ZonedDateTime] then
      Some(initCast[A](stringEncoded(classOf[ZonedDateTime], _.toString, ZonedDateTime.parse), typeId))
    else if cls == classOf[OffsetDateTime] then
      Some(initCast[A](stringEncoded(classOf[OffsetDateTime], _.toString, OffsetDateTime.parse), typeId))
    else if cls == classOf[java.time.Duration] then
      Some(initCast[A](stringEncoded(classOf[java.time.Duration], _.toString, java.time.Duration.parse), typeId))
    else if cls == classOf[Array[Byte]] then
      Some(BinaryHandlerNativeArray_byte.New().asInstanceOf[BinaryTypeHandler[A]])
    else if cls.getName == "zio.Chunk" || cls.getName == "zio.NonEmptyChunk" then
      Some(SchemaBinaryCodec.jsonPayloadHandler(runtimeClass, schema, typeId))
    else None

  private def initCast[A](handler: BinaryTypeHandler[?], typeId: Long): BinaryTypeHandler[A] =
    handler.initialize(typeId).asInstanceOf[BinaryTypeHandler[A]]

  private def stringEncoded[A](runtimeClass: Class[A], encode: A => String, decode: String => A): BinaryTypeHandler[A] =
    new AbstractBinaryHandlerCustomValueVariableLength[A, String](
      runtimeClass,
      AbstractBinaryHandlerCustom.CustomFields(AbstractBinaryHandlerCustom.chars("value")),
    ):
      override def store(data: Binary, instance: A, objectId: Long, handler: PersistenceStoreHandler[Binary]): Unit =
        data.storeStringSingleValue(typeId(), objectId, encode(instance))

      override def create(data: Binary, handler: PersistenceLoadHandler): A =
        decode(data.buildString())

      override def getValidationStateFromInstance(instance: A): String =
        encode(instance)

      override def getValidationStateFromBinary(data: Binary): String =
        data.buildString()

  final private class InstantHandler
    extends AbstractBinaryHandlerCustomValueFixedLength[Instant, java.lang.Long](
      classOf[Instant],
      AbstractBinaryHandlerCustom.defineValueType(java.lang.Long.TYPE),
    ):
    override def store(
      data: Binary,
      instance: Instant,
      objectId: Long,
      handler: PersistenceStoreHandler[Binary],
    ): Unit =
      data.storeLong(typeId(), objectId, instance.toEpochMilli)

    override def create(data: Binary, handler: PersistenceLoadHandler): Instant =
      Instant.ofEpochMilli(data.buildLong().longValue())

    override def getValidationStateFromInstance(instance: Instant): java.lang.Long =
      java.lang.Long.valueOf(instance.toEpochMilli)

    override def getValidationStateFromBinary(data: Binary): java.lang.Long =
      data.buildLong()

  final private class ScalaBigDecimalHandler
    extends AbstractBinaryHandlerCustomValueVariableLength[BigDecimal, String](
      classOf[BigDecimal],
      AbstractBinaryHandlerCustom.CustomFields(AbstractBinaryHandlerCustom.chars("value")),
    ):
    override def store(
      data: Binary,
      instance: BigDecimal,
      objectId: Long,
      handler: PersistenceStoreHandler[Binary],
    ): Unit =
      data.storeStringSingleValue(typeId(), objectId, instance.toString())

    override def create(data: Binary, handler: PersistenceLoadHandler): BigDecimal =
      BigDecimal(data.buildString())

    override def getValidationStateFromInstance(instance: BigDecimal): String =
      instance.toString()

    override def getValidationStateFromBinary(data: Binary): String =
      data.buildString()

  final private class ScalaBigIntHandler
    extends AbstractBinaryHandlerCustomValueVariableLength[BigInt, String](
      classOf[BigInt],
      AbstractBinaryHandlerCustom.CustomFields(AbstractBinaryHandlerCustom.chars("value")),
    ):
    override def store(
      data: Binary,
      instance: BigInt,
      objectId: Long,
      handler: PersistenceStoreHandler[Binary],
    ): Unit =
      data.storeStringSingleValue(typeId(), objectId, instance.toString())

    override def create(data: Binary, handler: PersistenceLoadHandler): BigInt =
      BigInt(data.buildString())

    override def getValidationStateFromInstance(instance: BigInt): String =
      instance.toString()

    override def getValidationStateFromBinary(data: Binary): String =
      data.buildString()

  final private class UuidHandler
    extends AbstractBinaryHandlerCustomValueFixedLength[UUID, String](
      classOf[UUID],
      AbstractBinaryHandlerCustom.CustomFields(
        AbstractBinaryHandlerCustom.CustomField(java.lang.Long.TYPE, "mostSignificantBits"),
        AbstractBinaryHandlerCustom.CustomField(java.lang.Long.TYPE, "leastSignificantBits"),
      ),
    ):
    private val MsbOffset   = 0L
    private val LsbOffset   = 8L
    private val BinaryBytes = 16L

    override def store(
      data: Binary,
      instance: UUID,
      objectId: Long,
      handler: PersistenceStoreHandler[Binary],
    ): Unit =
      data.storeEntityHeader(BinaryBytes, typeId(), objectId)
      data.store_long(MsbOffset, instance.getMostSignificantBits)
      data.store_long(LsbOffset, instance.getLeastSignificantBits)

    override def create(data: Binary, handler: PersistenceLoadHandler): UUID =
      new UUID(data.read_long(MsbOffset), data.read_long(LsbOffset))

    override def getValidationStateFromInstance(instance: UUID): String =
      instance.toString

    override def getValidationStateFromBinary(data: Binary): String =
      new UUID(data.read_long(MsbOffset), data.read_long(LsbOffset)).toString
