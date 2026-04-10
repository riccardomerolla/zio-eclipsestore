package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain

import zio.*
import zio.schema.Schema
import zio.schema.derived

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor

enum BookstoreCommand derives Schema:
  case Create(book: Book)
  case Update(id: BookId, payload: UpdateBookRequest)
  case Delete(id: BookId)

enum BookstoreEvent derives Schema:
  case BookCreated(book: Book)
  case BookUpdated(book: Book)
  case BookDeleted(id: BookId)

final case class BookstoreEventRecord(
  sequence: Long,
  event: BookstoreEvent,
) derives Schema

final case class BookstoreProjection(books: Chunk[Book]) derives Schema:
  def find(id: BookId): Option[Book] =
    books.find(_.id == id)

object BookstoreProjection:
  val empty: BookstoreProjection =
    BookstoreProjection(Chunk.empty)

final case class BookstoreEventRoot(
  snapshot: BookstoreProjection,
  journal: Chunk[BookstoreEventRecord],
  nextSequence: Long,
) derives Schema

object BookstoreEventRoot:
  val empty: BookstoreEventRoot =
    BookstoreEventRoot(
      snapshot = BookstoreProjection.empty,
      journal = Chunk.empty,
      nextSequence = 1L,
    )

  val descriptor: RootDescriptor[BookstoreEventRoot] =
    RootDescriptor.fromSchema(
      id = "bookstore-event-sourcing-root",
      initializer = () => empty,
    )

enum BookstoreDecisionError:
  case NotFound(id: BookId)
  case InvalidInput(message: String)
  case SnapshotDrift

object BookstoreEventSourcing:
  def replay(journal: Chunk[BookstoreEventRecord]): BookstoreProjection =
    journal.foldLeft(BookstoreProjection.empty) { (projection, record) =>
      evolve(projection, record.event)
    }

  def validate(root: BookstoreEventRoot): Either[BookstoreDecisionError, Unit] =
    Either.cond(
      replay(root.journal) == root.snapshot,
      (),
      BookstoreDecisionError.SnapshotDrift,
    )

  def decide(
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

  def runCommand(
    root: BookstoreEventRoot,
    command: BookstoreCommand,
  ): Either[BookstoreDecisionError, (Chunk[BookstoreEventRecord], BookstoreEventRoot)] =
    for
      _      <- validate(root)
      events <- decide(root.snapshot, command)
    yield
      val records =
        events.zipWithIndex.map { case (event, index) =>
          BookstoreEventRecord(root.nextSequence + index.toLong, event)
        }
      val nextSnapshot =
        records.foldLeft(root.snapshot) { (projection, record) =>
          evolve(projection, record.event)
        }
      (
        records,
        root.copy(
          snapshot = nextSnapshot,
          journal = root.journal ++ records,
          nextSequence = root.nextSequence + records.size.toLong,
        ),
      )

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

  private def evolve(
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
