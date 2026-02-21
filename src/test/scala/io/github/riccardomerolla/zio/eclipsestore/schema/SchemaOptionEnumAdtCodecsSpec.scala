package io.github.riccardomerolla.zio.eclipsestore.schema

import java.nio.file.{ Files, Path }
import java.time.Instant

import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

import zio.*
import zio.schema.Schema
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object SchemaOptionEnumAdtCodecsSpec extends ZIOSpecDefault:
  private def withService[A](
    dir: Path,
    handlers: Chunk[org.eclipse.serializer.persistence.binary.types.BinaryTypeHandler[?]],
  )(
    use: EclipseStoreService => ZIO[Any, EclipseStoreError, A]
  ) =
    val cfg = EclipseStoreConfig(
      storageTarget = StorageTarget.FileSystem(dir),
      customTypeHandlers = handlers,
    )
    ZIO.scoped {
      val layer = ZLayer.succeed(cfg) >>> EclipseStoreService.live
      for
        env <- layer.build
        svc  = env.get[EclipseStoreService]
        out <- use(svc)
      yield out
    }

  private def restartRoundtrip[A: ClassTag](key: String, value: A, schema: Schema[A])
    : ZIO[Any, EclipseStoreError, Option[A]] =
    ZIO.scoped {
      for
        dir     <- ZIO.acquireRelease(
                     ZIO.attempt(Files.createTempDirectory("schema-adt-codecs"))
                       .mapError(e => EclipseStoreError.InitializationError("Failed to create temp directory", Some(e)))
                   )(path =>
                     ZIO.attempt {
                       if Files.exists(path) then
                         Files.walk(path).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
                     }.orDie
                   )
        handlers = SchemaBinaryCodec.handlers(schema)
        _       <- withService(dir, handlers)(_.put(key, value))
        out     <- withService(dir, handlers)(_.get[String, A](key))
      yield out
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Schema Option / Enum / ADT codecs")(
      test("Option[String] roundtrips across restart for Some and None") {
        for
          some <- restartRoundtrip("opt-s", OptionStringBox(Some("x")), Schema[OptionStringBox])
          none <- restartRoundtrip("opt-n", OptionStringBox(None), Schema[OptionStringBox])
        yield assertTrue(some.exists(_.value.toString == "Some(x)"), none.exists(_.value.toString.startsWith("None")))
      },
      test("Option[Instant] roundtrips across restart") {
        val value = OptionInstantBox(Some(Instant.ofEpochMilli(1_700_000_000_123L)))
        restartRoundtrip("opt-instant", value, Schema[OptionInstantBox]).map(out => assertTrue(out.contains(value)))
      },
      test("Option[CaseClass] roundtrips across restart") {
        val value = OptionNoteBox(Some(Note("hello", 3)))
        restartRoundtrip("opt-note", value, Schema[OptionNoteBox]).map(out => assertTrue(out.contains(value)))
      },
      test("nested Option[Option[Int]] roundtrips across restart") {
        for
          a <- restartRoundtrip("opt-opt-a", NestedOptionBox(Some(Some(42))), Schema[NestedOptionBox])
          b <- restartRoundtrip("opt-opt-b", NestedOptionBox(Some(None)), Schema[NestedOptionBox])
          c <- restartRoundtrip("opt-opt-c", NestedOptionBox(None), Schema[NestedOptionBox])
        yield assertTrue(
          a.exists(_.value.toString == "Some(Some(42))"),
          b.exists(v => v.value.isEmpty || v.value.toString.contains("Some(None")),
          c.exists(_.value.toString.startsWith("None")),
        )
      },
      test("simple Scala enum survives restart") {
        val value = MessageType.Code()
        restartRoundtrip("enum-simple", value, Schema[MessageType]).map(out =>
          assertTrue(out.contains(value))
        )
      },
      test("enum with fields survives restart") {
        val value = DeliveryMethod.Push("device-1", priority = 2)
        restartRoundtrip("enum-fields", value, Schema[DeliveryMethod]).map(out => assertTrue(out.contains(value)))
      },
      test("sealed trait ADT with different case shapes survives restart") {
        import Shape.*
        val schema = Schema[Shape]
        for
          a <- restartRoundtrip("shape-a", Circle(2.5), schema)
          b <- restartRoundtrip("shape-b", Rectangle(4.0, 5.0), schema)
          c <- restartRoundtrip("shape-c", Origin(), schema)
        yield assertTrue(a.contains(Circle(2.5)), b.contains(Rectangle(4.0, 5.0)), c.contains(Origin()))
      },
      test("ConversationEntry model with options + enums survives restart") {
        val entry = ConversationEntry(
          id = Some("entry-1"),
          sender = "Riccardo",
          senderType = SenderType.User(),
          messageType = MessageType.Text(),
          metadata = None,
          createdAt = Instant.ofEpochMilli(1_723_456_789_123L),
        )
        restartRoundtrip("conversation-entry", entry, Schema[ConversationEntry]).map(out =>
          assertTrue(
            out.exists(v =>
              v.id.toString == "Some(entry-1)" &&
              v.sender == "Riccardo" &&
              v.senderType == SenderType.User() &&
              v.messageType == MessageType.Text() &&
              v.metadata.toString.startsWith("None") &&
              v.createdAt == Instant.ofEpochMilli(1_723_456_789_123L)
            )
          )
        )
      },
    )
