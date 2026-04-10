package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service

import zio.*

import java.util.UUID

import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.*
import io.github.riccardomerolla.zio.eclipsestore.error.{ EclipseStoreError, EventStoreError, SnapshotStoreError }
import io.github.riccardomerolla.zio.eclipsestore.service.{
  EventEnvelope,
  EventSourcedError,
  EventSourcedResult,
  EventSourcedRuntime,
  ExpectedVersion,
  ObjectStore,
  Transaction,
}
import scala.jdk.CollectionConverters.*

enum BookRepositoryError:
  case NotFound(id: BookId)
  case InvalidInput(message: String)
  case ConcurrentModification(message: String)
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
  runtime: EventSourcedRuntime[BookstoreProjection, BookstoreCommand, BookstoreEvent, BookstoreDecisionError]
) extends BookRepository:
  private def projection: IO[BookRepositoryError, BookstoreProjection] =
    runtime
      .load(BookstoreEventSourcing.streamId)
      .map(_.state)
      .mapError(decodeLoadError)

  private def process(command: BookstoreCommand): IO[BookRepositoryError, EventSourcedResult[BookstoreProjection, BookstoreEvent]] =
    for
      aggregate <- runtime.load(BookstoreEventSourcing.streamId).mapError(decodeLoadError)
      expected   = aggregate.revision.fold[ExpectedVersion](ExpectedVersion.NoStream)(ExpectedVersion.Exact.apply)
      result    <- runtime
                     .handle(BookstoreEventSourcing.streamId, expected, command)
                     .mapError(decodeHandleError)
    yield result

  private def createdBook(result: EventSourcedResult[BookstoreProjection, BookstoreEvent]): IO[BookRepositoryError, Book] =
    ZIO
      .fromOption(
        extractEvent(result.appendResult.envelopes) {
          case BookstoreEvent.BookCreated(created) => created
        }
      )
      .orElseFail(BookRepositoryError.StorageFailure("Expected BookCreated event after create command"))

  private def updatedBook(
    id: BookId,
    result: EventSourcedResult[BookstoreProjection, BookstoreEvent],
  ): IO[BookRepositoryError, Book] =
    ZIO
      .fromOption(
        extractEvent(result.appendResult.envelopes) {
          case BookstoreEvent.BookUpdated(updated) => updated
        }
      )
      .orElseFail(BookRepositoryError.StorageFailure(s"Expected BookUpdated event for ${id.value}"))

  private def extractEvent[A](
    envelopes: Chunk[EventEnvelope[BookstoreEvent]]
  )(
    project: PartialFunction[BookstoreEvent, A]
  ): Option[A] =
    envelopes.collectFirst { case EventEnvelope(_, _, _, event, _) if project.isDefinedAt(event) =>
      project(event)
    }

  private def decodeLoadError(error: EventSourcedError[Nothing]): BookRepositoryError =
    error match
      case EventSourcedError.DomainFailure(other)              =>
        BookRepositoryError.StorageFailure(s"Unexpected domain failure while loading bookstore state: $other")
      case EventSourcedError.EventStoreFailure(storeError)     => decodeEventStoreError(storeError)
      case EventSourcedError.SnapshotStoreFailure(snapshotErr) => decodeSnapshotStoreError(snapshotErr)

  private def decodeHandleError(error: EventSourcedError[BookstoreDecisionError]): BookRepositoryError =
    error match
      case EventSourcedError.DomainFailure(BookstoreDecisionError.NotFound(id)) =>
        BookRepositoryError.NotFound(id)
      case EventSourcedError.DomainFailure(BookstoreDecisionError.InvalidInput(message)) =>
        BookRepositoryError.InvalidInput(message)
      case EventSourcedError.EventStoreFailure(storeError)                           =>
        decodeEventStoreError(storeError)
      case EventSourcedError.SnapshotStoreFailure(snapshotErr)                      =>
        decodeSnapshotStoreError(snapshotErr)

  private def decodeEventStoreError(error: EventStoreError): BookRepositoryError =
    error match
      case EventStoreError.WrongExpectedVersion(_, expected, actual, _) =>
        BookRepositoryError.ConcurrentModification(
          s"Bookstore stream changed while processing command (expected=$expected, actual=$actual)"
        )
      case other                                                       =>
        BookRepositoryError.StorageFailure(other.toString)

  private def decodeSnapshotStoreError(error: SnapshotStoreError): BookRepositoryError =
    BookRepositoryError.StorageFailure(error.toString)

  override def create(payload: CreateBookRequest): IO[BookRepositoryError, Book] =
    for
      id      <- Random.nextUUID.map(BookId.apply)
      book     = Book(id, payload.title, payload.author, payload.price, payload.tags)
      result  <- process(BookstoreCommand.Create(book))
      created <- createdBook(result)
    yield created

  override def update(id: BookId, payload: UpdateBookRequest): IO[BookRepositoryError, Book] =
    for
      result  <- process(BookstoreCommand.Update(id, payload))
      updated <- updatedBook(id, result)
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

  val eventSourcedLive
    : ZLayer[EventSourcedRuntime[BookstoreProjection, BookstoreCommand, BookstoreEvent, BookstoreDecisionError], Nothing, BookRepository] =
    ZLayer.fromFunction(BookRepositoryEventSourcedLive.apply)
