package io.github.riccardomerolla.zio.eclipsestore.schema

import java.time.Instant

import zio.schema.{ Schema, derived }

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
