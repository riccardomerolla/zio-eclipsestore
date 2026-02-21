package io.github.riccardomerolla.zio.eclipsestore.schema

import java.nio.file.{ Files, Path }
import java.time.Instant

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import org.eclipse.serializer.persistence.binary.java.lang.BinaryHandlerString

object SchemaRootDescriptorIntegrationSpec extends ZIOSpecDefault:
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
      ZIO.attempt(Files.createTempDirectory("schema-root-descriptor"))
        .mapError(e => EclipseStoreError.InitializationError("Failed to create temp directory", Some(e)))
    )(path =>
      ZIO.attempt {
        if Files.exists(path) then Files.walk(path).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
      }.orDie
    )

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Schema-aware RootDescriptor integration")(
      test("RootDescriptor.fromSchema captures schema and runtime class") {
        val descriptor = RootDescriptor.fromSchema[ConversationEntry](
          id = "conversation-root",
          initializer = () =>
            ConversationEntry(
              id = None,
              sender = "system",
              senderType = SenderType.System(),
              messageType = MessageType.Status(),
              metadata = None,
              createdAt = Instant.EPOCH,
            ),
        )

        assertTrue(descriptor.schema.nonEmpty, descriptor.schemaClass.contains(classOf[ConversationEntry]))
      },
      test("schema-aware root descriptors auto-register handlers for persisted values across restart") {
        val descriptor = RootDescriptor.fromSchema[ConversationEntry](
          id = "conversation-root",
          initializer = () =>
            ConversationEntry(
              id = None,
              sender = "system",
              senderType = SenderType.System(),
              messageType = MessageType.Status(),
              metadata = None,
              createdAt = Instant.EPOCH,
            ),
        )
        val entry      = ConversationEntry(
          id = Some("entry-10"),
          sender = "Riccardo",
          senderType = SenderType.User(),
          messageType = MessageType.Text(),
          metadata = None,
          createdAt = Instant.ofEpochMilli(1_723_456_789_123L),
        )

        ZIO.scoped {
          for
            dir <- tempDir
            cfg  = EclipseStoreConfig(
                     storageTarget = StorageTarget.FileSystem(dir),
                     rootDescriptors = Chunk.single(descriptor),
                   )
            _   <- withService(cfg) { svc =>
                     for
                       _ <- svc.root(descriptor)
                       _ <- svc.put("entry", entry)
                     yield ()
                   }
            out <- withService(cfg)(_.get[String, ConversationEntry]("entry"))
          yield assertTrue(
            out.exists(v =>
              v.id.toString == "Some(entry-10)" &&
                v.sender == "Riccardo" &&
                v.senderType == SenderType.User() &&
                v.messageType == MessageType.Text() &&
                v.metadata.toString.startsWith("None") &&
                v.createdAt == Instant.ofEpochMilli(1_723_456_789_123L)
            )
          )
        }
      },
      test("manual customTypeHandlers are preserved and take precedence over schema-derived duplicates") {
        val descriptor = RootDescriptor.fromSchema[String]("string-root", () => "init")
        val config     = EclipseStoreConfig(
          storageTarget = StorageTarget.InMemory("schema-root-manual-precedence"),
          rootDescriptors = Chunk.single(descriptor),
          customTypeHandlers = Chunk.single(BinaryHandlerString.New()),
        )
        val merged     = EclipseStoreService.mergedTypeHandlers(config)
        val count      = merged.count(_.`type`() == classOf[String])

        assertTrue(count == 1)
      },
    )
