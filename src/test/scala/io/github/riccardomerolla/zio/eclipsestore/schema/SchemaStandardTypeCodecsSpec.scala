package io.github.riccardomerolla.zio.eclipsestore.schema

import java.net.{ URI, URL }
import java.nio.file.{ Files, Path }
import java.time.*
import java.util.UUID

import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

import zio.*
import zio.schema.Schema
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object SchemaStandardTypeCodecsSpec extends ZIOSpecDefault:
  private def withService[A](dir: Path, schemaHandler: org.eclipse.serializer.persistence.binary.types.BinaryTypeHandler[?])(
    use: EclipseStoreService => ZIO[Any, EclipseStoreError, A]
  ) =
    val cfg = EclipseStoreConfig(
      storageTarget = StorageTarget.FileSystem(dir),
      customTypeHandlers = Chunk.single(schemaHandler),
    )
    ZIO.scoped {
      val layer = ZLayer.succeed(cfg) >>> EclipseStoreService.live
      for
        env <- layer.build
        svc  = env.get[EclipseStoreService]
        out <- use(svc)
      yield out
    }

  private def roundtrip[A: ClassTag](
    key: String,
    value: A,
    schema: Schema[A],
  ): ZIO[Any, EclipseStoreError, Option[A]] =
    ZIO.scoped {
      for
        dir <- ZIO.acquireRelease(
                 ZIO.attempt(Files.createTempDirectory("schema-standard-codecs"))
                   .mapError(e => EclipseStoreError.InitializationError("Failed to create temp directory", Some(e)))
               )(path =>
                 ZIO.attempt {
                   if Files.exists(path) then Files.walk(path).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
                 }.orDie
               )
        out <- withService(dir, SchemaBinaryCodec.handler(schema)) { svc =>
                 for
                   _      <- svc.put(key, value)
                   loaded <- svc.get[String, A](key)
                 yield loaded
               }
      yield out
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Schema standard codecs")(
      test("String") {
        roundtrip("string", "hello", Schema[String]).map(out => assertTrue(out.contains("hello")))
      },
      test("Boolean") {
        roundtrip("bool", true, Schema[Boolean]).map(out => assertTrue(out.contains(true)))
      },
      test("Byte") {
        roundtrip("byte", 42.toByte, Schema[Byte]).map(out => assertTrue(out.contains(42.toByte)))
      },
      test("Short") {
        roundtrip("short", 1024.toShort, Schema[Short]).map(out => assertTrue(out.contains(1024.toShort)))
      },
      test("Int") {
        roundtrip("int", 123456, Schema[Int]).map(out => assertTrue(out.contains(123456)))
      },
      test("Long") {
        roundtrip("long", 9876543210L, Schema[Long]).map(out => assertTrue(out.contains(9876543210L)))
      },
      test("Float") {
        roundtrip("float", 12.5f, Schema[Float]).map(out => assertTrue(out.contains(12.5f)))
      },
      test("Double") {
        roundtrip("double", 33.125d, Schema[Double]).map(out => assertTrue(out.contains(33.125d)))
      },
      test("Char") {
        roundtrip("char", 'z', Schema[Char]).map(out => assertTrue(out.contains('z')))
      },
      test("Scala BigDecimal preserves precision") {
        val v = BigDecimal("12345678901234567890.12345678901234567890")
        roundtrip("bigdecimal", v, Schema[BigDecimal]).map(out => assertTrue(out.contains(v)))
      },
      test("Scala BigInt") {
        val v = BigInt("123456789012345678901234567890")
        roundtrip("bigint", v, Schema[BigInt]).map(out => assertTrue(out.contains(v)))
      },
      test("UUID") {
        val v = UUID.fromString("123e4567-e89b-12d3-a456-426614174000")
        roundtrip("uuid", v, Schema[UUID]).map(out => assertTrue(out.contains(v)))
      },
      test("Unit") {
        roundtrip("unit", (), Schema[Unit]).map(out => assertTrue(out.contains(())))
      },
      test("Instant uses epoch-millis representation") {
        val v = Instant.ofEpochMilli(1_723_456_789_123L)
        roundtrip("instant", v, Schema[Instant]).map(out => assertTrue(out.contains(v)))
      },
      test("LocalDate") {
        val v = LocalDate.parse("2026-02-21")
        roundtrip("localdate", v, Schema[LocalDate]).map(out => assertTrue(out.contains(v)))
      },
      test("LocalDateTime") {
        val v = LocalDateTime.parse("2026-02-21T10:11:12")
        roundtrip("localdatetime", v, Schema[LocalDateTime]).map(out => assertTrue(out.contains(v)))
      },
      test("LocalTime") {
        val v = LocalTime.parse("10:11:12")
        roundtrip("localtime", v, Schema[LocalTime]).map(out => assertTrue(out.contains(v)))
      },
      test("ZonedDateTime") {
        val v = ZonedDateTime.parse("2026-02-21T10:11:12+01:00[Europe/Rome]")
        roundtrip("zoned", v, Schema[ZonedDateTime]).map(out => assertTrue(out.contains(v)))
      },
      test("OffsetDateTime") {
        val v = OffsetDateTime.parse("2026-02-21T10:11:12+01:00")
        roundtrip("offset", v, Schema[OffsetDateTime]).map(out => assertTrue(out.contains(v)))
      },
      test("Duration") {
        val v = java.time.Duration.ofMillis(123456789L)
        roundtrip("duration", v, Schema[java.time.Duration]).map(out => assertTrue(out.contains(v)))
      },
      test("Period") {
        val v = Period.of(1, 2, 3)
        roundtrip("period", v, Schema[Period]).map(out => assertTrue(out.contains(v)))
      },
      test("URI") {
        val v = URI.create("https://example.com/books/1?q=test")
        roundtrip("uri", v, Schema[URI]).map(out => assertTrue(out.contains(v)))
      },
      test("URL") {
        val v = new URL("https://example.com/assets/logo.png")
        roundtrip("url", v, Schema[URL]).map(out => assertTrue(out.exists(_.toExternalForm == v.toExternalForm)))
      },
      test("Array[Byte]") {
        val v      = Array[Byte](1, 2, 3, 4, 5)
        val schema = Schema.chunk(Schema[Byte]).transform(_.toArray, Chunk.fromArray(_))
        roundtrip("bytes", v, schema).map(out => assertTrue(out.exists(_.sameElements(v))))
      },
      test("Chunk[Int]") {
        val schema = Schema.chunk(Schema[Int])
        val v      = Chunk(1, 2, 3, 4)
        roundtrip("chunk", v, schema).map(out => assertTrue(out.contains(v)))
      },
      test("NonEmptyChunk[Int]") {
        val schema = Schema.nonEmptyChunk(Schema[Int])
        val v      = NonEmptyChunk(1, 2, 3)
        roundtrip("nonemptychunk", v, schema).map(out => assertTrue(out.contains(v)))
      },
    )
