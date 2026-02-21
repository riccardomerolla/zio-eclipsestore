package io.github.riccardomerolla.zio.eclipsestore.schema

import java.nio.file.{ Files, Path }
import java.time.Instant

import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

import zio.*
import zio.schema.{ Schema, derived }
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, LifecycleCommand }

object SchemaComprehensiveIntegrationSpec extends ZIOSpecDefault:
  final case class BulkRecord(id: Int, payload: String, createdAt: Instant) derives Schema
  final case class BulkEnvelope(records: Chunk[BulkRecord]) derives Schema

  final case class TwentyFieldRecord(
    f01: String,
    f02: Int,
    f03: Long,
    f04: Double,
    f05: Boolean,
    f06: Option[String],
    f07: Option[Int],
    f08: BigDecimal,
    f09: Chunk[Int],
    f10: String,
    f11: Int,
    f12: Long,
    f13: Double,
    f14: Boolean,
    f15: Option[String],
    f16: Option[Int],
    f17: BigDecimal,
    f18: Chunk[Int],
    f19: Instant,
    f20: String,
  ) derives Schema

  private def withService[A](cfg: EclipseStoreConfig)(use: EclipseStoreService => ZIO[Any, EclipseStoreError, A]) =
    ZIO.scoped {
      val layer = ZLayer.succeed(cfg) >>> EclipseStoreService.live
      for
        env <- layer.build
        svc  = env.get[EclipseStoreService]
        out <- use(svc)
      yield out
    }

  private def tempDir: ZIO[Scope, EclipseStoreError, Path] =
    ZIO.acquireRelease(
      ZIO
        .attempt(Files.createTempDirectory("schema-comprehensive"))
        .mapError(e => EclipseStoreError.InitializationError("Failed to create temp directory", Some(e)))
    )(path =>
      ZIO.attempt {
        if Files.exists(path) then Files.walk(path).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
      }.orDie
    )

  private def restartRoundtrip[A: Schema: ClassTag](
    key: String,
    value: A,
    rootId: String,
    useAutoRegistration: Boolean = true,
  ): ZIO[Any, EclipseStoreError, Option[A]] =
    ZIO.scoped {
      for
        dir       <- tempDir
        descriptor = RootDescriptor.fromSchema[A](rootId, () => value)
        cfg        = EclipseStoreConfig(
                       storageTarget = StorageTarget.FileSystem(dir),
                       rootDescriptors = Chunk.single(descriptor),
                       customTypeHandlers =
                         if useAutoRegistration then Chunk.empty
                         else SchemaBinaryCodec.handlers(Schema[A]),
                       autoRegisterSchemaHandlers = useAutoRegistration,
                     )
        _         <- withService(cfg)(_.put(key, value))
        reloaded  <- withService(cfg)(_.get[String, A](key))
      yield reloaded
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Schema comprehensive integration")(
      suite("Store-restart-retrieve")(
        test("ChatConversation with nested messages survives restart") {
          val value = ChatConversation(
            id = "c-1",
            title = Some("chat"),
            messages = List(
              ConversationEntry(
                id = Some("m-1"),
                sender = "user",
                senderType = SenderType.User(),
                messageType = MessageType.Text(),
                metadata = None,
                createdAt = Instant.ofEpochMilli(1_723_456_789_123L),
              ),
              ConversationEntry(
                id = Some("m-2"),
                sender = "assistant",
                senderType = SenderType.Assistant(),
                messageType = MessageType.Code(),
                metadata = Some("tool output"),
                createdAt = Instant.ofEpochMilli(1_723_456_790_123L),
              ),
            ),
            archivedAt = None,
          )
          restartRoundtrip("chat:c-1", value, "chat-root").map(out =>
            assertTrue(
              out.exists(_.id == "c-1"),
              out.exists(_.title.contains("chat")),
              out.exists(_.messages.nonEmpty),
            )
          )
        },
        test("AgentIssue with Option/enum fields survives restart") {
          val issue = AgentIssue(
            id = "iss-1",
            conversationId = Some("c-1"),
            state = IssueState.InProgress(),
            severity = IssueSeverity.High(),
            summary = "Schema integration validation",
            owner = Some("platform"),
            labels = Chunk("schema", "restart", "typed"),
            dueAt = Some(Instant.ofEpochMilli(1_723_500_000_000L)),
          )
          restartRoundtrip("issue:1", issue, "issue-root").map(out => assertTrue(out.contains(issue)))
        },
        test("bulk envelope survives restart") {
          val envelope = BulkEnvelope(
            records =
              Chunk.fromIterable((0 until 50).map(n => BulkRecord(n, s"payload-$n", Instant.ofEpochMilli(n.toLong))))
          )
          restartRoundtrip("bulk:all", envelope, "bulk-root", useAutoRegistration = false).map(out =>
            assertTrue(out.exists(_.records.size == 50), out.exists(_.records(37).id == 37))
          )
        },
      ),
      suite("Auto-registration & compatibility")(
        test("nested schema type ids are stable across repeated derivation") {
          val first  =
            SchemaBinaryCodec.handlers(Schema[ChatConversation]).map(h => (h.`type`().getName, h.typeId())).toSet
          val second =
            SchemaBinaryCodec.handlers(Schema[ChatConversation]).map(h => (h.`type`().getName, h.typeId())).toSet
          assertTrue(first == second, first.nonEmpty)
        },
        test("mixed manual + schema handlers work together") {
          val descriptor = RootDescriptor.fromSchema[ConversationEntry](
            "conversation-root",
            () =>
              ConversationEntry(
                id = None,
                sender = "system",
                senderType = SenderType.System(),
                messageType = MessageType.Status(),
                metadata = None,
                createdAt = Instant.EPOCH,
              ),
          )
          val cfg        = EclipseStoreConfig(
            storageTarget = StorageTarget.InMemory("mixed-handlers"),
            rootDescriptors = Chunk.single(descriptor),
            customTypeHandlers =
              Chunk.single(org.eclipse.serializer.persistence.binary.java.lang.BinaryHandlerString.New()),
          )
          val handlers   = EclipseStoreService.mergedTypeHandlers(cfg)
          assertTrue(
            handlers.exists(_.`type`() == classOf[String]),
            handlers.exists(_.`type`() == classOf[ConversationEntry]),
          )
        },
      ),
      suite("Edge cases & performance sanity")(
        test("empty Chunk roundtrips across restart") {
          given Schema[Chunk[Int]] = Schema.chunk(Schema[Int])
          restartRoundtrip("chunk:empty", Chunk.empty[Int], "chunk-root").map(out =>
            assertTrue(out.contains(Chunk.empty))
          )
        },
        test("very large String (>64KB) roundtrips across restart") {
          val huge = "x" * 100_000
          restartRoundtrip("large-string", huge, "large-string-root").map(out => assertTrue(out.contains(huge)))
        },
        test("case class with 20+ fields survives restart") {
          val rec = TwentyFieldRecord(
            "a",
            2,
            3L,
            4.5,
            true,
            Some("f6"),
            Some(7),
            BigDecimal("8.123"),
            Chunk(9, 10),
            "b",
            12,
            13L,
            14.5,
            false,
            Some("f15"),
            Some(16),
            BigDecimal("17.321"),
            Chunk(18, 19),
            Instant.ofEpochMilli(20L),
            "z",
          )
          restartRoundtrip("twenty", rec, "twenty-root").map(out => assertTrue(out.contains(rec)))
        },
        test("concurrent writes either complete without corruption or fail with a storage error") {
          ZIO.scoped {
            for
              dir     <- tempDir
              cfg      = EclipseStoreConfig(
                           storageTarget = StorageTarget.FileSystem(dir),
                           customTypeHandlers = SchemaBinaryCodec.handlers(Schema[BulkRecord]),
                         )
              outcome <-
                withService(cfg) { svc =>
                  ZIO
                    .foreachPar((0 until 50).grouped(10).toList) { group =>
                      svc.putAll(group.map(n => s"cw:$n" -> BulkRecord(n, s"v-$n", Instant.ofEpochMilli(n.toLong))))
                    }
                    .withParallelism(4) *> svc.maintenance(LifecycleCommand.Checkpoint).unit *> svc.getAll[BulkRecord]
                }.either
            yield outcome match
              case Right(all)                              => assertTrue(all.map(_.id).distinct.size == all.size)
              case Left(_: EclipseStoreError.StorageError) => assertTrue(true)
              case Left(_)                                 => assertTrue(false)
          }
        },
        test("1000 writes/reads complete within reasonable time") {
          ZIO.scoped {
            for
              dir   <- tempDir
              cfg    = EclipseStoreConfig(
                         storageTarget = StorageTarget.FileSystem(dir),
                         customTypeHandlers = SchemaBinaryCodec.handlers(Schema[BulkRecord]),
                       )
              start <- Clock.nanoTime
              _     <- withService(cfg) { svc =>
                         for
                           _ <- ZIO.foreachDiscard(0 until 1000)(n =>
                                  svc.put(s"perf:$n", BulkRecord(n, s"payload-$n", Instant.ofEpochMilli(n.toLong)))
                                )
                           _ <- ZIO.foreachDiscard(0 until 1000)(n => svc.get[String, BulkRecord](s"perf:$n"))
                         yield ()
                       }
              end   <- Clock.nanoTime
              tookMs = (end - start) / 1_000_000L
            yield assertTrue(tookMs < 20_000L)
          }
        },
      ),
    )
