package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import java.nio.file.{ Files, Path }

import scala.jdk.CollectionConverters.*

import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalEventingConfig
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.*
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.http.BookRoutes
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.BookRepository
import io.github.riccardomerolla.zio.eclipsestore.service.{ EventSourcedRuntimeLive, NativeLocalEventStore, NativeLocalSnapshotStore, SnapshotPolicy }

object BookRoutesEventSourcedSpec extends ZIOSpecDefault:

  final case class JsonDecodeError(message: String) extends Exception(message)

  private def withTempDirectory[A](prefix: String)(use: Path => ZIO[Any, Throwable, A]): ZIO[Any, Throwable, A] =
    ZIO.scoped {
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory(prefix))) { path =>
        ZIO.attemptBlocking {
          if Files.exists(path) then
            Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
        }.ignore
      }.flatMap(use)
    }

  private def repoLayer(baseDir: Path): ULayer[BookRepository] =
    val config = NativeLocalEventingConfig(baseDir)
    ZLayer.make[BookRepository](
      NativeLocalEventStore.live[BookstoreEvent](config),
      NativeLocalSnapshotStore.live[BookstoreProjection](config),
      EventSourcedRuntimeLive.layer(
        BookstoreEventSourcing,
        SnapshotPolicy.everyNEvents[BookstoreProjection, BookstoreEvent](100),
      ),
      BookRepository.eventSourcedLive,
    )

  private val routes = BookRoutes.routes

  private def run(request: Request): ZIO[BookRepository & Scope, Nothing, Response] =
    routes.runZIO(request)

  private def bodyText(response: Response): Task[String] =
    response.body.asArray.map(bytes => new String(bytes, java.nio.charset.StandardCharsets.UTF_8))

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("BookRoutesEventSourced")(
      test("creates and lists books via the event-sourced NativeLocal repository") {
        withTempDirectory("book-routes-event-sourced") { dir =>
          val baseDir = dir.resolve("bookstore-eventing")
          val payload = CreateBookRequest("HTTP NativeLocal", "Author", BigDecimal(25)).toJson

          (for
            createResponse <- run(Request.post("/books", Body.fromString(payload))).provide(
                                Scope.default,
                                repoLayer(baseDir),
                              )
            listResponse   <- run(Request.get("/books")).provide(
                                Scope.default,
                                repoLayer(baseDir).fresh,
                              )
            body           <- bodyText(listResponse)
            books          <- ZIO.fromEither(body.fromJson[List[Book]].left.map(JsonDecodeError.apply))
          yield assertTrue(
            createResponse.status == Status.Ok,
            books.map(_.title) == List("HTTP NativeLocal"),
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
      test("returns 404 for a missing book via the event-sourced repository") {
        withTempDirectory("book-routes-event-sourced-404") { dir =>
          val baseDir = dir.resolve("bookstore-eventing")

          run(Request.get("/books/00000000-0000-0000-0000-000000000000"))
            .provide(
              Scope.default,
              repoLayer(baseDir),
            )
            .map(response => assertTrue(response.status == Status.NotFound))
        }
      },
    )
