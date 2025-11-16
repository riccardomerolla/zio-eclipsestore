package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.*
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.http.BookRoutes
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.BookRepository
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object BookRoutesSpec extends ZIOSpecDefault:

  private val repoLayer: ULayer[BookRepository] =
    EclipseStoreService.inMemory >>> BookRepository.live

  private val routes = BookRoutes.routes

  private def run(request: Request): ZIO[BookRepository & Scope, Nothing, Response] =
    routes.runZIO(request)

  private def bodyText(response: Response): Task[String] =
    response.body.asArray.map(bytes => new String(bytes, java.nio.charset.StandardCharsets.UTF_8))

  override def spec =
    suite("BookRoutes")(
      test("creates and lists books via HTTP") {
        val payload = CreateBookRequest("HTTP ZIO", "Author", BigDecimal(25)).toJson
        for
          createResponse <- run(Request.post("/books", Body.fromString(payload)))
          listResponse   <- run(Request.get("/books"))
          body           <- bodyText(listResponse)
          books          <- ZIO.fromEither(body.fromJson[List[Book]].left.map(new RuntimeException(_)))
        yield assertTrue(
          createResponse.status == Status.Ok,
          books.nonEmpty,
        )
      },
      test("returns 404 when book is missing") {
        for response <- run(Request.get("/books/00000000-0000-0000-0000-000000000000"))
        yield assertTrue(response.status == Status.NotFound)
      },
    ).provideLayer(Scope.default ++ repoLayer)
