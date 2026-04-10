package io.github.riccardomerolla.zio.eclipsestore.schema

import java.nio.file.Files
import java.time.Instant

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.{ Schema, StandardType, derived }
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object SchemaSerializerSpec extends ZIOSpecDefault:
  opaque type UserId = String

  object UserId:
    def apply(value: String): UserId = value

    extension (userId: UserId) def value: String = userId

  given Schema[UserId] =
    Schema.primitive(StandardType.StringType).asInstanceOf[Schema[UserId]]

  enum DeliveryState derives Schema:
    case Pending
    case Delivered(at: Instant)

  final case class Shipment(
    id: UserId,
    aliases: Chunk[String],
    primaryContact: Either[String, Int],
    state: DeliveryState,
  ) derives Schema

  final case class RecursiveNode(
    id: String,
    children: List[RecursiveNode],
  ) derives Schema

  final case class TrackedIssue(
    id: String,
    state: IssueState,
    severity: IssueSeverity,
    summary: String,
  ) derives Schema

  private val serializer = SchemaSerializerLive()

  private def tempDirectory(prefix: String): ZIO[Scope, EclipseStoreError, java.nio.file.Path] =
    ZIO.acquireRelease(
      ZIO
        .attempt(Files.createTempDirectory(prefix))
        .mapError(cause =>
          EclipseStoreError.InitializationError(s"Failed to create $prefix temp directory", Some(cause))
        )
    )(path =>
      ZIO.attempt {
        if Files.exists(path) then
          Files.walk(path).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
      }.orDie
    )

  private def restartRoundTrip(
    config: EclipseStoreConfig,
    write: EclipseStoreService => IO[EclipseStoreError, Unit],
    read: EclipseStoreService => IO[EclipseStoreError, TrackedIssue],
  ): ZIO[Any, EclipseStoreError, TrackedIssue] =
    val layer = ZLayer.succeed(config) >>> EclipseStoreService.live
    for
      _   <- ZIO.scoped {
               for
                 writerEnv <- layer.build
                 writer     = writerEnv.get[EclipseStoreService]
                 _         <- write(writer)
               yield ()
             }
      out <- ZIO.scoped {
               for
                 readerEnv <- layer.build
                 reader     = readerEnv.get[EclipseStoreService]
                 value     <- read(reader)
               yield value
             }
    yield out

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("SchemaSerializer")(
      test("encodes and decodes schema-driven values without leaking Java serialization") {
        val shipment = Shipment(
          id = UserId("user-1"),
          aliases = Chunk("ops", "priority"),
          primaryContact = Right(42),
          state = DeliveryState.Delivered(Instant.ofEpochMilli(1_723_456_789_123L)),
        )

        for
          encoded <- serializer.encode(shipment)
          decoded <- serializer.decode[Shipment](encoded)
        yield assertTrue(
          encoded.contains("user-1"),
          encoded.contains("priority"),
          decoded == shipment,
        )
      },
      test("round-trips recursive values and opaque wrappers through the schema boundary") {
        val root = RecursiveNode(
          id = "root",
          children = List(
            RecursiveNode("left", Nil),
            RecursiveNode("right", List(RecursiveNode("right-leaf", Nil))),
          ),
        )

        for
          encoded <- serializer.encode(root)
          decoded <- serializer.decode[RecursiveNode](encoded)
        yield assertTrue(
          encoded.contains("right-leaf"),
          decoded == root,
        )
      },
      test("derives stable handlers and deduplicated config registration from Schema[A]") {
        val config = EclipseStoreConfig(StorageTarget.InMemory("schema-serializer-registration"))

        for
          firstHandler  <- serializer.typeHandler[Shipment]
          secondHandler <- serializer.typeHandler[Shipment]
          registered    <- serializer.register[Shipment](config)
          registered2   <- serializer.register[Shipment](registered)
          runtimeTypes   = registered2.customTypeHandlers.map(_.`type`()).toSet
        yield assertTrue(
          firstHandler.typeId == secondHandler.typeId,
          firstHandler.runtimeClass == secondHandler.runtimeClass,
          runtimeTypes.contains(firstHandler.runtimeClass),
          registered.customTypeHandlers.nonEmpty,
          registered2.customTypeHandlers.size == runtimeTypes.size,
        )
      },
      test("registered schema handlers preserve enum pattern matching after restart") {
        val issue = TrackedIssue(
          id = "issue-1",
          state = IssueState.Closed(reason = Some("resolved")),
          severity = IssueSeverity.High(),
          summary = "SchemaSerializer registration path",
        )

        for
          dir    <- tempDirectory("schema-serializer-restart")
          config <- serializer.register[TrackedIssue](
                      EclipseStoreConfig(storageTarget = StorageTarget.FileSystem(dir))
                    )
          out    <- restartRoundTrip(
                      config = config,
                      write = svc => svc.put("issue", issue),
                      read = svc =>
                        svc
                          .get[String, TrackedIssue]("issue")
                          .someOrFail(EclipseStoreError.QueryError("Missing issue after restart", None)),
                    )
        yield assertTrue(
          out == issue,
          out.state match
            case IssueState.Closed(Some("resolved")) => true
            case _                                   => false,
        )
      },
    )
