package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain

import zio.*
import zio.schema.Schema
import zio.schema.derived

import io.github.riccardomerolla.zio.eclipsestore.service.{ EventEnvelope, EventSourcedBehavior, StreamId }

enum BookstoreCommand derives Schema:
  case Create(book: Book)
  case Update(id: BookId, payload: UpdateBookRequest)
  case Delete(id: BookId)

enum BookstoreEvent derives Schema:
  case BookCreated(book: Book)
  case BookUpdated(book: Book)
  case BookDeleted(id: BookId)

final case class BookstoreProjection(books: Chunk[Book]) derives Schema:
  def find(id: BookId): Option[Book] =
    books.find(_.id == id)

object BookstoreProjection:
  val empty: BookstoreProjection =
    BookstoreProjection(Chunk.empty)

enum BookstoreDecisionError:
  case NotFound(id: BookId)
  case InvalidInput(message: String)

object BookstoreEventSourcing extends EventSourcedBehavior[BookstoreProjection, BookstoreCommand, BookstoreEvent, BookstoreDecisionError]:
  val streamId: StreamId =
    StreamId("bookstore-catalog")

  override val zero: BookstoreProjection =
    BookstoreProjection.empty

  def replay(journal: Chunk[EventEnvelope[BookstoreEvent]]): BookstoreProjection =
    journal.foldLeft(zero) { (projection, envelope) =>
      evolve(projection, envelope.payload)
    }

  override def decide(
    state: BookstoreProjection,
    command: BookstoreCommand,
  ): IO[BookstoreDecisionError, Chunk[BookstoreEvent]] =
    ZIO.fromEither(decidePure(state, command))

  private def decidePure(
    state: BookstoreProjection,
    command: BookstoreCommand,
  ): Either[BookstoreDecisionError, Chunk[BookstoreEvent]] =
    command match
      case BookstoreCommand.Create(payload)     =>
        validateCreate(payload).map(valid => Chunk.single(BookstoreEvent.BookCreated(valid)))
      case BookstoreCommand.Update(id, payload) =>
        state.find(id) match
          case None           => Left(BookstoreDecisionError.NotFound(id))
          case Some(existing) =>
            validateUpdate(existing, payload).map { updated =>
              Chunk.single(BookstoreEvent.BookUpdated(updated))
            }
      case BookstoreCommand.Delete(id)          =>
        state.find(id) match
          case None    => Left(BookstoreDecisionError.NotFound(id))
          case Some(_) => Right(Chunk.single(BookstoreEvent.BookDeleted(id)))

  private def validateCreate(book: Book): Either[BookstoreDecisionError, Book] =
    for
      title  <- nonBlank(book.title, "Book title must not be empty")
      author <- nonBlank(book.author, "Book author must not be empty")
      _      <- nonNegativePrice(book.price)
    yield book.copy(
      title = title,
      author = author,
    )

  private def validateUpdate(existing: Book, payload: UpdateBookRequest): Either[BookstoreDecisionError, Book] =
    for
      title  <- payload.title.fold[Either[BookstoreDecisionError, String]](Right(existing.title))(nonBlank(_, "Book title must not be empty"))
      author <- payload.author.fold[Either[BookstoreDecisionError, String]](Right(existing.author))(nonBlank(_, "Book author must not be empty"))
      price   = payload.price.getOrElse(existing.price)
      _      <- nonNegativePrice(price)
    yield existing.copy(
      title = title,
      author = author,
      price = price,
      tags = payload.tags.getOrElse(existing.tags),
    )

  private def nonBlank(value: String, message: String): Either[BookstoreDecisionError, String] =
    Either.cond(value.trim.nonEmpty, value.trim, BookstoreDecisionError.InvalidInput(message))

  private def nonNegativePrice(value: BigDecimal): Either[BookstoreDecisionError, Unit] =
    Either.cond(value >= 0, (), BookstoreDecisionError.InvalidInput("Book price must be non-negative"))

  override def evolve(
    projection: BookstoreProjection,
    event: BookstoreEvent,
  ): BookstoreProjection =
    event match
      case BookstoreEvent.BookCreated(book) =>
        projection.copy(books = projection.books :+ book)
      case BookstoreEvent.BookUpdated(book) =>
        projection.copy(
          books = projection.books.map(existing => if existing.id == book.id then book else existing)
        )
      case BookstoreEvent.BookDeleted(id)   =>
        projection.copy(books = projection.books.filterNot(_.id == id))
