package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service

import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.*
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import zio.*

import java.util.UUID
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

final case class BookRepositoryLive(store: EclipseStoreService) extends BookRepository:
  private def withRoot: IO[BookRepositoryError, BookstoreRoot] =
    store.root(BookstoreRoot.descriptor).mapError(err => BookRepositoryError.StorageFailure(err.toString))

  private def persist(root: BookstoreRoot): IO[BookRepositoryError, Unit] =
    store.persist(root).mapError(err => BookRepositoryError.StorageFailure(err.toString))

  override def create(payload: CreateBookRequest): IO[BookRepositoryError, Book] =
    for
      root <- withRoot
      id = BookId(UUID.randomUUID())
      book = Book(id, payload.title, payload.author, payload.price, payload.tags)
      _ <- ZIO.succeed(root.storage.put(id.value, book))
      _ <- persist(root)
    yield book

  override def update(id: BookId, payload: UpdateBookRequest): IO[BookRepositoryError, Book] =
    for
      root <- withRoot
      updated <- Option(root.storage.get(id.value)) match
        case Some(existing) =>
          val recalculated = existing.copy(
            title = payload.title.getOrElse(existing.title),
            author = payload.author.getOrElse(existing.author),
            price = payload.price.getOrElse(existing.price),
            tags = payload.tags.getOrElse(existing.tags)
          )
          root.storage.put(id.value, recalculated)
          ZIO.succeed(recalculated)
        case None =>
          ZIO.fail(BookRepositoryError.NotFound(id))
      _ <- persist(root)
    yield updated

  override def get(id: BookId): IO[BookRepositoryError, Option[Book]] =
    withRoot.map(root => Option(root.storage.get(id.value)))

  override def delete(id: BookId): IO[BookRepositoryError, Unit] =
    for
      root <- withRoot
      _ <- Option(root.storage.remove(id.value)) match
        case Some(_) => persist(root)
        case None    => ZIO.fail(BookRepositoryError.NotFound(id))
    yield ()

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

  val live: ZLayer[EclipseStoreService, Nothing, BookRepository] =
    ZLayer.fromFunction(BookRepositoryLive.apply)
