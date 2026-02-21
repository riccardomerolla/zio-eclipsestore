package io.github.riccardomerolla.zio.eclipsestore.examples.chatmodels

import java.nio.file.Files
import java.time.Instant

import zio.*
import zio.schema.{ Schema, derived }

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.schema.TypedStore
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

enum SenderType derives Schema:
  case User()
  case Assistant()
  case System()

enum MessageType derives Schema:
  case Text()
  case Status()
  case ToolCall(name: String)

enum IssueSeverity derives Schema:
  case Low()
  case Medium()
  case High()

enum IssueState derives Schema:
  case Open()
  case InProgress()
  case Closed(reason: Option[String])

final case class ConversationEntry(
  id: Option[String],
  sender: String,
  senderType: SenderType,
  messageType: MessageType,
  metadata: Option[String],
  createdAt: Instant,
) derives Schema

final case class ChatConversation(
  id: String,
  title: Option[String],
  entries: List[ConversationEntry],
  archivedAt: Option[Instant],
) derives Schema

final case class AgentIssue(
  id: String,
  conversationId: Option[String],
  state: IssueState,
  severity: IssueSeverity,
  summary: String,
  owner: Option[String],
  labels: List[String],
  dueAt: Option[Instant],
) derives Schema

final case class ChatRoot(
  conversations: List[ChatConversation],
  issues: List[AgentIssue],
) derives Schema

object ChatModelsSchemaApp extends ZIOAppDefault:
  private val chatRoot = RootDescriptor.fromSchema[ChatRoot](
    id = "chat-root",
    initializer = () => ChatRoot(Nil, Nil),
  )

  private def withStore[A](
    cfg: EclipseStoreConfig
  )(
    use: TypedStore => ZIO[Any, EclipseStoreError, A]
  ): ZIO[Scope, EclipseStoreError, A] =
    val layer = ZLayer.succeed(cfg) >>> EclipseStoreService.live >>> TypedStore.live
    for
      env <- layer.build
      out <- use(env.get[TypedStore])
    yield out

  override def run: ZIO[Any, Throwable, Unit] =
    val conversation = ChatConversation(
      id = "c-1",
      title = Some("Schema roundtrip"),
      entries = List(
        ConversationEntry(
          id = Some("m-1"),
          sender = "Riccardo",
          senderType = SenderType.User(),
          messageType = MessageType.Text(),
          metadata = Some("first message"),
          createdAt = Instant.ofEpochMilli(1_723_456_789_123L),
        ),
        ConversationEntry(
          id = Some("m-2"),
          sender = "assistant",
          senderType = SenderType.Assistant(),
          messageType = MessageType.ToolCall("search"),
          metadata = None,
          createdAt = Instant.ofEpochMilli(1_723_456_790_000L),
        ),
      ),
      archivedAt = None,
    )
    val issue        = AgentIssue(
      id = "iss-42",
      conversationId = Some("c-1"),
      state = IssueState.InProgress(),
      severity = IssueSeverity.Medium(),
      summary = "Support Option/enums/Instant end-to-end",
      owner = Some("platform"),
      labels = List("schema", "typed-store", "restart"),
      dueAt = None,
    )

    ZIO.scoped {
      for
        dir                  <- ZIO
                                  .attempt(Files.createTempDirectory("chat-models-schema"))
                                  .mapError(e => EclipseStoreError.InitializationError("Cannot create temporary directory", Some(e)))
        cfg                   = EclipseStoreConfig(
                                  storageTarget = StorageTarget.FileSystem(dir),
                                  rootDescriptors = Chunk.single(chatRoot),
                                )
        _                    <- withStore(cfg) { store =>
                                  for
                                    _ <- store.typedRoot(chatRoot)
                                    _ <- store.store("conversation:c-1", conversation)
                                    _ <- store.store("issue:iss-42", issue)
                                  yield ()
                                }
        reloadedConversation <- withStore(cfg)(_.fetch[String, ChatConversation]("conversation:c-1"))
        reloadedIssue        <- withStore(cfg)(_.fetch[String, AgentIssue]("issue:iss-42"))
        _                    <- ZIO.logInfo(s"Conversation after restart: $reloadedConversation")
        _                    <- ZIO.logInfo(s"Issue after restart: $reloadedIssue")
      yield ()
    }.mapError(e => new RuntimeException(e.toString))
