package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service

import zio.*

import java.util.UUID

import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.*
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ ObjectStore, Transaction }
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
