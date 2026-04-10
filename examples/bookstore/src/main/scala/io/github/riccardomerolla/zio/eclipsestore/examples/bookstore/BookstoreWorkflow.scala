package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import java.nio.file.Path
import java.util.UUID

import zio.*
import zio.json.ast.Json
import zio.schema.Schema
import zio.schema.derived

import io.github.riccardomerolla.zio.eclipsestore.config.BackendConfig
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.*
import io.github.riccardomerolla.zio.eclipsestore.schema.{ Migration, MigrationPlan }
import io.github.riccardomerolla.zio.eclipsestore.service.{ ObjectStore, StorageBackend, StorageOps, Transaction }

final case class BookstoreCatalog(books: Chunk[Book]):
  def sorted: Chunk[Book] =
    books.sortBy(_.title)

trait BookstoreWorkflow:
  def createBook(payload: CreateBookRequest): IO[EclipseStoreError, Book]
  def inventory: IO[EclipseStoreError, BookstoreCatalog]
  def backup(target: Path): IO[EclipseStoreError, Unit]
  def restartAndInventory: IO[EclipseStoreError, BookstoreCatalog]

final case class BookstoreWorkflowLive(
  objectStore: ObjectStore[BookstoreRoot],
  storageOps: StorageOps[BookstoreRoot],
) extends BookstoreWorkflow:
  override def createBook(payload: CreateBookRequest): IO[EclipseStoreError, Book] =
    for
      book <- objectStore.transact(
                Transaction.effect { root =>
                  ZIO.attempt {
                    val book = Book(
                      id = BookId(UUID.randomUUID()),
                      title = payload.title,
                      author = payload.author,
                      price = payload.price,
                      tags = payload.tags,
                    )
                    root.storage.put(book.id.value, book)
                    book
                  }.mapError(cause => EclipseStoreError.QueryError("Failed to create bookstore sample book", Some(cause)))
                }
              )
      root <- objectStore.load
      _    <- objectStore.storeSubgraph(root.storage)
    yield book

  override def inventory: IO[EclipseStoreError, BookstoreCatalog] =
    objectStore.load.map(root => BookstoreCatalog(Chunk.fromIterable(root.storage.values().toArray(Array.empty[Book]))))

  override def backup(target: Path): IO[EclipseStoreError, Unit] =
    storageOps.backup(target).unit

  override def restartAndInventory: IO[EclipseStoreError, BookstoreCatalog] =
    storageOps.restart *> inventory

object BookstoreWorkflow:
  def createBook(payload: CreateBookRequest): ZIO[BookstoreWorkflow, EclipseStoreError, Book] =
    ZIO.serviceWithZIO[BookstoreWorkflow](_.createBook(payload))

  val inventory: ZIO[BookstoreWorkflow, EclipseStoreError, BookstoreCatalog] =
    ZIO.serviceWithZIO[BookstoreWorkflow](_.inventory)

  def backup(target: Path): ZIO[BookstoreWorkflow, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[BookstoreWorkflow](_.backup(target))

  val restartAndInventory: ZIO[BookstoreWorkflow, EclipseStoreError, BookstoreCatalog] =
    ZIO.serviceWithZIO[BookstoreWorkflow](_.restartAndInventory)

  def layer(backendConfig: BackendConfig): ZLayer[Any, EclipseStoreError, BookstoreWorkflow] =
    ZLayer.make[BookstoreWorkflow](
      ZLayer.succeed(backendConfig),
      StorageBackend.rootServices(
        BookstoreRoot.descriptor,
        _.copy(rootDescriptors = Chunk.single(BookstoreRoot.descriptor)),
      ),
      ZLayer.fromFunction(BookstoreWorkflowLive.apply),
    )

final case class LegacyBookRecord(
  id: BookId,
  title: String,
  author: String,
  price: BigDecimal,
) derives Schema

final case class BookRecordV2(
  id: BookId,
  title: String,
  author: String,
  price: BigDecimal,
  tags: Chunk[String],
  shelf: String,
) derives Schema

object BookstoreMigrations:
  val legacyToV2: MigrationPlan[LegacyBookRecord, BookRecordV2] =
    Migration
      .define[LegacyBookRecord, BookRecordV2](
        Migration.addField("tags", Json.Arr()),
        Migration.addField("shelf", Json.Str("general")),
      )
      .plan

  def migrate(record: LegacyBookRecord): IO[EclipseStoreError, BookRecordV2] =
    legacyToV2.migrate(record)
