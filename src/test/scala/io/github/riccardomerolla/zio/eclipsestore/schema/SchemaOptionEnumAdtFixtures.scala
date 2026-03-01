package io.github.riccardomerolla.zio.eclipsestore.schema

import java.time.Instant

import zio.Chunk
import zio.schema.{ Schema, derived }

// Opaque type with a validating Schema (transformOrFail rejects the default empty string),
// reproducing the bug from issue #25: enumCaseSubtypeHandlers skips cases whose defaultValue is Left.
opaque type ValidatedId = String
object ValidatedId:
  def apply(s: String): ValidatedId = s
  given schema: Schema[ValidatedId] = Schema.primitive[String].transformOrFail(
    s => if s.nonEmpty then Right(s) else Left("ValidatedId cannot be empty"),
    Right(_),
  )

enum AssignmentState derives Schema:
  case Unassigned()
  case Assigned(agentId: ValidatedId, assignedAt: Instant)

enum MessageType derives Schema:
  case Text()
  case Code()
  case Error()
  case Status()

enum SenderType derives Schema:
  case User()
  case Assistant()
  case System()

enum DeliveryMethod derives Schema:
  case Email(address: String)
  case Sms(number: String)
  case Push(deviceId: String, priority: Int)

sealed trait Shape derives Schema
object Shape:
  final case class Circle(radius: Double)                   extends Shape derives Schema
  final case class Rectangle(width: Double, height: Double) extends Shape derives Schema
  final case class Origin()                                 extends Shape derives Schema

final case class Note(value: String, rank: Int) derives Schema

final case class OptionStringBox(value: Option[String]) derives Schema
final case class OptionInstantBox(value: Option[Instant]) derives Schema
final case class OptionNoteBox(value: Option[Note]) derives Schema
final case class NestedOptionBox(value: Option[Option[Int]]) derives Schema

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
  messages: List[ConversationEntry],
  archivedAt: Option[Instant],
) derives Schema

enum IssueState derives Schema:
  case Open()
  case InProgress()
  case Closed(reason: Option[String])

enum IssueSeverity derives Schema:
  case Low()
  case Medium()
  case High()

final case class AgentIssue(
  id: String,
  conversationId: Option[String],
  state: IssueState,
  severity: IssueSeverity,
  summary: String,
  owner: Option[String],
  labels: Chunk[String],
  dueAt: Option[Instant],
) derives Schema
