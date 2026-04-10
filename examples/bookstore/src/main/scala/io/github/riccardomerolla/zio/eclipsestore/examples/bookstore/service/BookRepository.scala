package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service

import zio.*

import java.util.UUID

import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.*
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ ObjectStore, StorageOps, Transaction }
import scala.jdk.CollectionConverters.*

enum BookRepositoryError:
  case NotFound(id: BookId)
  case InvalidInput(message: String)
  case StorageFailure(message: String)

trait BookRepository:
  def create(payload: CreateBookRequest): IO[BookRepositoryError, Book]
  def update(id: BookId, payload: UpdateBookRequest): IO[BookRepositoryError, Book]
  def get(id: BookId): IO[BookRepositoryError, Option[Book]]
  def delete(id: BookId): IO[BookRepositoryError, Unit]
  def list: IO[BookRepositoryError, Chunk[Book]]

final case class BookRepositoryLive(store: ObjectStore[BookstoreRoot]) extends BookRepository:
  private def withRoot: IO[BookRepositoryError, BookstoreRoot] =
    store.load.mapError(err => BookRepositoryError.StorageFailure(err.toString))

  private def missing(id: BookId): EclipseStoreError =
    EclipseStoreError.QueryError(s"Book ${id.value} not found", None)

  override def create(payload: CreateBookRequest): IO[BookRepositoryError, Book] =
    store
      .transact(
        Transaction.effect { root =>
          ZIO.attempt {
            val id   = BookId(UUID.randomUUID())
            val book = Book(id, payload.title, payload.author, payload.price, payload.tags)
            root.storage.put(id.value, book)
            book
          }.mapError(cause => EclipseStoreError.StorageError("Failed to create book", Some(cause)))
        }
      )
      .mapError(err => BookRepositoryError.StorageFailure(err.toString))

  override def update(id: BookId, payload: UpdateBookRequest): IO[BookRepositoryError, Book] =
    store
      .transact(
        Transaction.effect { root =>
          Option(root.storage.get(id.value)) match
            case Some(existing) =>
              val recalculated = existing.copy(
                title = payload.title.getOrElse(existing.title),
                author = payload.author.getOrElse(existing.author),
                price = payload.price.getOrElse(existing.price),
                tags = payload.tags.getOrElse(existing.tags),
              )
              ZIO.succeed {
                root.storage.put(id.value, recalculated)
                recalculated
              }
            case None           =>
              ZIO.fail(missing(id))
        }
      )
      .mapError {
        case EclipseStoreError.QueryError(message, _) if message.contains(id.value.toString) =>
          BookRepositoryError.NotFound(id)
        case err                      => BookRepositoryError.StorageFailure(err.toString)
      }

  override def get(id: BookId): IO[BookRepositoryError, Option[Book]] =
    withRoot.map(root => Option(root.storage.get(id.value)))

  override def delete(id: BookId): IO[BookRepositoryError, Unit] =
    store
      .transact(
        Transaction.effect { root =>
          Option(root.storage.remove(id.value)) match
            case Some(_) => ZIO.unit
            case None    => ZIO.fail(missing(id))
        }
      )
      .mapError {
        case EclipseStoreError.QueryError(message, _) if message.contains(id.value.toString) =>
          BookRepositoryError.NotFound(id)
        case err                      => BookRepositoryError.StorageFailure(err.toString)
      }

  override def list: IO[BookRepositoryError, Chunk[Book]] =
    withRoot.map(root => Chunk.fromIterable(root.storage.values().asScala.toList))

final case class BookRepositoryEventSourcedLive(
  store: ObjectStore[BookstoreEventRoot],
  ops: StorageOps[BookstoreEventRoot],
) extends BookRepository:
  private val DecisionPrefix = "bookstore-decision:"

  private def encodeDecisionError(error: BookstoreDecisionError): EclipseStoreError =
    error match
      case BookstoreDecisionError.NotFound(id)      =>
        EclipseStoreError.QueryError(s"${DecisionPrefix}not-found:${id.value}", None)
      case BookstoreDecisionError.InvalidInput(msg) =>
        EclipseStoreError.QueryError(s"${DecisionPrefix}invalid-input:$msg", None)
      case BookstoreDecisionError.SnapshotDrift     =>
        EclipseStoreError.QueryError(s"${DecisionPrefix}snapshot-drift", None)

  private def decodeStoreError(error: EclipseStoreError): BookRepositoryError =
    error match
      case EclipseStoreError.QueryError(message, _) if message.startsWith(DecisionPrefix) =>
        decodeDecisionMessage(message.stripPrefix(DecisionPrefix))
      case other                                                                         =>
        BookRepositoryError.StorageFailure(other.toString)

  private def decodeDecisionMessage(message: String): BookRepositoryError =
    if message.startsWith("not-found:") then
      BookRepositoryError.NotFound(BookId(UUID.fromString(message.stripPrefix("not-found:"))))
    else if message.startsWith("invalid-input:") then
      BookRepositoryError.InvalidInput(message.stripPrefix("invalid-input:"))
    else if message == "snapshot-drift" then
      BookRepositoryError.StorageFailure("Stored bookstore snapshot drift detected")
    else
      BookRepositoryError.StorageFailure(s"Unrecognized bookstore decision failure: $message")

  private def process(command: BookstoreCommand): IO[BookRepositoryError, Chunk[BookstoreEventRecord]] =
    for
      records <- store.modify { root =>
                   ZIO.fromEither(BookstoreEventSourcing.runCommand(root, command)).mapError { error =>
                     encodeDecisionError(error)
                   }
                 }
                 .mapError(decodeStoreError)
      _       <- ops.checkpoint.mapError(err => BookRepositoryError.StorageFailure(err.toString))
    yield records

  private def projection: IO[BookRepositoryError, BookstoreProjection] =
    store.load
      .map(_.snapshot)
      .mapError(err => BookRepositoryError.StorageFailure(err.toString))

  override def create(payload: CreateBookRequest): IO[BookRepositoryError, Book] =
    for
      id      <- Random.nextUUID.map(BookId.apply)
      book     = Book(id, payload.title, payload.author, payload.price, payload.tags)
      records <- process(BookstoreCommand.Create(book))
      created <- ZIO
                   .fromOption(records.collectFirst { case BookstoreEventRecord(_, BookstoreEvent.BookCreated(created)) =>
                     created
                   })
                   .orElseFail(BookRepositoryError.StorageFailure("Expected BookCreated event after create command"))
    yield created

  override def update(id: BookId, payload: UpdateBookRequest): IO[BookRepositoryError, Book] =
    for
      records <- process(BookstoreCommand.Update(id, payload))
      updated <- ZIO
                   .fromOption(records.collectFirst { case BookstoreEventRecord(_, BookstoreEvent.BookUpdated(book)) =>
                     book
                   })
                   .orElseFail(BookRepositoryError.StorageFailure(s"Expected BookUpdated event for ${id.value}"))
    yield updated

  override def get(id: BookId): IO[BookRepositoryError, Option[Book]] =
    projection.map(_.find(id))

  override def delete(id: BookId): IO[BookRepositoryError, Unit] =
    process(BookstoreCommand.Delete(id)).unit

  override def list: IO[BookRepositoryError, Chunk[Book]] =
    projection.map(_.books)

object BookRepository:
  def create(payload: CreateBookRequest): ZIO[BookRepository, BookRepositoryError, Book] =
    ZIO.serviceWithZIO[BookRepository](_.create(payload))

  def update(id: BookId, payload: UpdateBookRequest): ZIO[BookRepository, BookRepositoryError, Book] =
    ZIO.serviceWithZIO[BookRepository](_.update(id, payload))

  def get(id: BookId): ZIO[BookRepository, BookRepositoryError, Option[Book]] =
    ZIO.serviceWithZIO[BookRepository](_.get(id))

  def delete(id: BookId): ZIO[BookRepository, BookRepositoryError, Unit] =
    ZIO.serviceWithZIO[BookRepository](_.delete(id))

  val list: ZIO[BookRepository, BookRepositoryError, Chunk[Book]] =
    ZIO.serviceWithZIO[BookRepository](_.list)

  val live: ZLayer[ObjectStore[BookstoreRoot], Nothing, BookRepository] =
    ZLayer.fromFunction(BookRepositoryLive.apply)

  val eventSourcedLive: ZLayer[ObjectStore[BookstoreEventRoot] & StorageOps[BookstoreEventRoot], Nothing, BookRepository] =
    ZLayer.fromFunction(BookRepositoryEventSourcedLive.apply)
