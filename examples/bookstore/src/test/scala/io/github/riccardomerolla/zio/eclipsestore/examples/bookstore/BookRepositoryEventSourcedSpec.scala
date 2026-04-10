package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import java.nio.file.{ Files, Path }

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.BackendConfig
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.*
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.{ BookRepository, BookRepositoryError }
import io.github.riccardomerolla.zio.eclipsestore.service.StorageBackend

object BookRepositoryEventSourcedSpec extends ZIOSpecDefault:

  private def withTempDirectory[A](prefix: String)(use: Path => ZIO[Any, Throwable, A]): ZIO[Any, Throwable, A] =
    ZIO.scoped {
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory(prefix))) { path =>
        ZIO.attemptBlocking {
          if Files.exists(path) then
            Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
        }.ignore
      }.flatMap(use)
    }

  private def repoLayer(snapshotPath: Path): ZLayer[Any, Nothing, BookRepository] =
    ZLayer.succeed(BackendConfig.NativeLocal(snapshotPath)) >>>
      StorageBackend
        .rootServices(BookstoreEventRoot.descriptor)
        .mapError(err => new RuntimeException(err.toString))
        .orDie >>>
      BookRepository.eventSourcedLive

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("BookRepositoryEventSourced")(
      test("creates, updates, and reloads books from the NativeLocal journal-backed root") {
        withTempDirectory("bookstore-event-sourced-repo") { dir =>
          val snapshotPath = dir.resolve("bookstore-events.snapshot.json")
          val layer        = repoLayer(snapshotPath)

          (for
            created <- (for
                         created <- BookRepository.create(CreateBookRequest("Event Sourcing", "Pierre", BigDecimal(42)))
                         _       <- BookRepository.update(
                                      created.id,
                                      UpdateBookRequest(
                                        title = Some("Event Sourcing in Practice"),
                                        author = None,
                                        price = Some(BigDecimal(45)),
                                        tags = Some(Chunk("event-sourcing", "nativelocal")),
                                      ),
                                    )
                       yield created).provideLayer(layer)
            reopened <- (for
                          fetched <- BookRepository.get(created.id)
                          listed  <- BookRepository.list
                        yield (fetched, listed)).provideLayer(layer.fresh)
          yield assertTrue(
            reopened._1.exists(_.title == "Event Sourcing in Practice"),
            reopened._1.exists(_.price == BigDecimal(45)),
            reopened._2.size == 1,
            reopened._2.head.tags == Chunk("event-sourcing", "nativelocal"),
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
      test("rejects invalid commands and preserves domain-specific errors") {
        withTempDirectory("bookstore-event-sourced-errors") { dir =>
          val snapshotPath = dir.resolve("bookstore-events.snapshot.json")

          BookRepository
            .create(CreateBookRequest("  ", "Author", BigDecimal(10)))
            .provideLayer(repoLayer(snapshotPath))
            .either
            .map(result =>
              assertTrue(
                result == Left(BookRepositoryError.InvalidInput("Book title must not be empty"))
              )
            )
        }
      },
      test("deletes books durably across a fresh reopen") {
        withTempDirectory("bookstore-event-sourced-delete") { dir =>
          val snapshotPath = dir.resolve("bookstore-events.snapshot.json")
          val layer        = repoLayer(snapshotPath)

          (for
            created <- BookRepository.create(CreateBookRequest("Transient Book", "Author", BigDecimal(10))).provideLayer(layer)
            _       <- BookRepository.delete(created.id).provideLayer(layer)
            reopened <- (for
                           fetched <- BookRepository.get(created.id)
                           listed  <- BookRepository.list
                         yield (fetched, listed)).provideLayer(layer.fresh)
          yield assertTrue(
            reopened._1.isEmpty,
            reopened._2.isEmpty,
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
    )
