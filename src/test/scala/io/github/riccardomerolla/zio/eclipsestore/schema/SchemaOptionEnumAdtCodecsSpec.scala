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
      test("enum case with transformOrFail field (no defaultValue) survives restart") {
        // Regression test for issue #25: enumCaseSubtypeHandlers skipped cases whose Schema had no
        // defaultValue (e.g. opaque types with transformOrFail validation). The Assigned case here
        // uses ValidatedId, whose Schema.transformOrFail rejects the empty-string default, causing
        // defaultValue to be Left. The fix falls back to Class.forName to find the case class.
        val value = AssignmentState.Assigned(ValidatedId("agent-007"), Instant.ofEpochMilli(1_700_000_000_000L))
        restartRoundtrip("assignment-assigned", value, Schema[AssignmentState]).map(out =>
          assertTrue(out.contains(value))
        )
      },
      test("enum case without transformOrFail field alongside transformOrFail case survives restart") {
        val value = AssignmentState.Unassigned()
        restartRoundtrip("assignment-unassigned", value, Schema[AssignmentState]).map(out =>
          assertTrue(out.contains(value))
        )
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
      // Regression tests for issue #31: enum with case class variants (not just case objects).
      // These tests verify that each case class variant pattern-matches correctly after restart,
      // catching the category of bug documented in #25 before it reaches production.
      suite("Enum case class variants — restart regression (#31)")(
        test("Confirmed variant pattern-matches correctly after restart") {
          val confirmedAt = Instant.ofEpochMilli(1_700_000_000_000L)
          val payment     = Payment("pay-1", PaymentState.Confirmed(BigDecimal("99.99"), confirmedAt))
          restartRoundtrip("payment:pay-1", payment, Schema[Payment]).map { out =>
            assertTrue(
              out.isDefined,
              out.exists(p =>
                p.state match
                  case PaymentState.Confirmed(amount, _) => amount == BigDecimal("99.99")
                  case _                                 => false
              ),
            )
          }
        },
        test("Pending variant (single-field) pattern-matches correctly after restart") {
          val createdAt = Instant.ofEpochMilli(1_700_000_001_000L)
          val payment   = Payment("pay-2", PaymentState.Pending(createdAt))
          restartRoundtrip("payment:pay-2", payment, Schema[Payment]).map { out =>
            assertTrue(
              out.isDefined,
              out.exists(p =>
                p.state match
                  case PaymentState.Pending(ts) => ts == createdAt
                  case _                        => false
              ),
            )
          }
        },
        test("Failed variant (two-field) pattern-matches correctly after restart") {
          val failedAt = Instant.ofEpochMilli(1_700_000_002_000L)
          val payment  = Payment("pay-3", PaymentState.Failed("insufficient funds", failedAt))
          restartRoundtrip("payment:pay-3", payment, Schema[Payment]).map { out =>
            assertTrue(
              out.isDefined,
              out.exists(p =>
                p.state match
                  case PaymentState.Failed(reason, _) => reason == "insufficient funds"
                  case _                              => false
              ),
            )
          }
        },
        test("all PaymentState variants round-trip with correct equality after restart") {
          val confirmedAt = Instant.ofEpochMilli(1_700_000_000_000L)
          val createdAt   = Instant.ofEpochMilli(1_700_000_001_000L)
          val failedAt    = Instant.ofEpochMilli(1_700_000_002_000L)
          for
            confirmed <- restartRoundtrip(
                           "pay-all-confirmed",
                           Payment("c", PaymentState.Confirmed(BigDecimal("1.00"), confirmedAt)),
                           Schema[Payment],
                         )
            pending   <- restartRoundtrip(
                           "pay-all-pending",
                           Payment("p", PaymentState.Pending(createdAt)),
                           Schema[Payment],
                         )
            failed    <- restartRoundtrip(
                           "pay-all-failed",
                           Payment("f", PaymentState.Failed("timeout", failedAt)),
                           Schema[Payment],
                         )
          yield assertTrue(
            confirmed.contains(Payment("c", PaymentState.Confirmed(BigDecimal("1.00"), confirmedAt))),
            pending.contains(Payment("p", PaymentState.Pending(createdAt))),
            failed.contains(Payment("f", PaymentState.Failed("timeout", failedAt))),
          )
        },
        test("sealed trait hierarchy pattern-matches correctly after restart") {
          import Shape.*
          val schema = Schema[Shape]
          for
            circle <- restartRoundtrip("shape-circle", Circle(3.14), schema)
            rect   <- restartRoundtrip("shape-rect", Rectangle(2.0, 5.0), schema)
            origin <- restartRoundtrip("shape-origin", Origin(), schema)
          yield assertTrue(
            circle.exists(s =>
              s match
                case Circle(r) => r == 3.14
                case _         => false
            ),
            rect.exists(s =>
              s match
                case Rectangle(w, h) => w == 2.0 && h == 5.0
                case _               => false
            ),
            origin.exists(s =>
              s match
                case Origin() => true
                case _        => false
            ),
          )
        },
      ),
    )
