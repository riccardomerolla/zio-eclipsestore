package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import java.nio.file.{ Files, Path }

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.BackendConfig
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.{ BookId, CreateBookRequest }

object BookstoreWorkflowSpec extends ZIOSpecDefault:

  private def withTempDirectory[A](prefix: String)(use: Path => ZIO[Any, Throwable, A]): ZIO[Any, Throwable, A] =
    ZIO.scoped {
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory(prefix))) { path =>
        ZIO.attemptBlocking {
          if Files.exists(path) then
            Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
        }.ignore
      }.flatMap(use)
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("BookstoreWorkflow")(
      test("creates, queries, backs up, and reloads data after restart") {
        withTempDirectory("bookstore-workflow") { dataPath =>
          withTempDirectory("bookstore-backup") { backupPath =>
            val layer    = BookstoreWorkflow.layer(BackendConfig.FileSystem(dataPath))
            val firstRun =
              for
                _ <- BookstoreWorkflow.createBook(
                       CreateBookRequest("Zionomicon", "John de Goes", BigDecimal(49), Chunk("zio", "scala"))
                     )
                _ <- BookstoreWorkflow.createBook(
                       CreateBookRequest("Inside EclipseStore", "Riccardo", BigDecimal(39), Chunk("storage"))
                     )
                beforeRestart <- BookstoreWorkflow.inventory
                _             <- BookstoreWorkflow.backup(backupPath)
              yield beforeRestart

            val secondRun = BookstoreWorkflow.inventory.provideLayer(layer)

            for
              initial <- firstRun.provideLayer(layer).either
              reloaded <- secondRun.either
              backupFiles <- ZIO.attemptBlocking {
                               Files.walk(backupPath).iterator().asScala.count(Files.isRegularFile(_))
                             }
            yield (initial, reloaded) match
              case (Right(beforeRestart), Right(afterRestart)) =>
                assertTrue(
                  beforeRestart.sorted.map(_.title) == Chunk("Inside EclipseStore", "Zionomicon"),
                  afterRestart.sorted.map(_.title) == Chunk("Inside EclipseStore", "Zionomicon"),
                  backupFiles > 0,
                )
              case _                                       =>
                assertTrue(false)
          }
        }
      },
      test("legacy sample records receive v2 defaults during migration") {
        val legacy = LegacyBookRecord(
          id = BookId(java.util.UUID.randomUUID()),
          title = "Legacy Scala Book",
          author = "Vintage Author",
          price = BigDecimal(19),
        )

        assertZIO(
          BookstoreMigrations.migrate(legacy).either.map {
            case Right(migrated) =>
              migrated.id == legacy.id &&
              migrated.title == legacy.title &&
              migrated.author == legacy.author &&
              migrated.price == legacy.price &&
              migrated.tags.isEmpty &&
              migrated.shelf == "general"
            case Left(_)         =>
              false
          }
        )(Assertion.isTrue)
      },
      test("legacy migration preserves common fields for arbitrary samples") {
        check(
          Gen.uuid,
          Gen.alphaNumericStringBounded(1, 24),
          Gen.alphaNumericStringBounded(1, 24),
          Gen.bigDecimal(BigDecimal(-1000), BigDecimal(1000)),
        ) { (id, title, author, price) =>
          val legacy = LegacyBookRecord(BookId(id), title, author, price)

          assertZIO(
            BookstoreMigrations.migrate(legacy).either.map {
              case Right(record) =>
                record.id == legacy.id &&
                record.title == title &&
                record.author == author &&
                record.price == price &&
                record.tags == Chunk.empty &&
                record.shelf == "general"
              case Left(_)       =>
                false
            }
          )(Assertion.isTrue)
        }
      },
    )
