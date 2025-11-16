package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.http

import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.*
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.{BookRepository, BookRepositoryError}
import zio.*
import zio.http.*
import zio.json.*

import java.nio.charset.StandardCharsets
import java.util.UUID

object BookRoutes:
  private val BooksSegment = "books"
  private val emptyHeaders = Headers.empty
  private val notFoundResponse = textResponse(Status.NotFound, "Not found")

  private def readBody[A: JsonDecoder](request: Request): IO[BookRepositoryError, A] =
    for
      bytes <- request.body.asArray.mapError(err => BookRepositoryError.InvalidInput(err.getMessage))
      json = new String(bytes, StandardCharsets.UTF_8)
      decoded <- ZIO.fromEither(json.fromJson[A].left.map(BookRepositoryError.InvalidInput.apply))
    yield decoded

  private def jsonResponse[A: JsonEncoder](value: A): Response =
    Response.json(value.toJson)

  private def textResponse(status: Status, body: String): Response =
    Response(status, emptyHeaders, Body.fromString(body))

  private def encodeError(error: BookRepositoryError): Response =
    error match
      case BookRepositoryError.NotFound(id) =>
        textResponse(Status.NotFound, s"Book ${id.value} not found")
      case BookRepositoryError.InvalidInput(message) =>
        textResponse(Status.BadRequest, message)
      case BookRepositoryError.StorageFailure(message) =>
        textResponse(Status.InternalServerError, message)

  private def parseId(segment: String): IO[BookRepositoryError, BookId] =
    ZIO
      .attempt(BookId(UUID.fromString(segment)))
      .mapError(_ => BookRepositoryError.InvalidInput(s"Invalid book id: $segment"))

  private def routeRequest(request: Request): ZIO[BookRepository, Nothing, Response] =
    val segments = request.url.path.segments.toList
    (request.method, segments) match
      case (Method.GET, BooksSegment :: Nil) =>
        BookRepository
          .list
          .map(books => jsonResponse(books.toList))
          .catchAll(err => ZIO.succeed(encodeError(err)))
      case (Method.POST, BooksSegment :: Nil) =>
        (for
          payload <- readBody[CreateBookRequest](request)
          created <- BookRepository.create(payload)
        yield jsonResponse(created))
          .catchAll(err => ZIO.succeed(encodeError(err)))
      case (Method.GET, BooksSegment :: bookId :: Nil) =>
        (for
          id <- parseId(bookId)
          result <- BookRepository.get(id)
        yield result match
          case Some(book) => jsonResponse(book)
          case None       => notFoundResponse
        ).catchAll(err => ZIO.succeed(encodeError(err)))
      case (Method.PUT, BooksSegment :: bookId :: Nil) =>
        (for
          id <- parseId(bookId)
          payload <- readBody[UpdateBookRequest](request)
          updated <- BookRepository.update(id, payload)
        yield jsonResponse(updated))
          .catchAll(err => ZIO.succeed(encodeError(err)))
      case (Method.DELETE, BooksSegment :: bookId :: Nil) =>
        (for
          id <- parseId(bookId)
          _ <- BookRepository.delete(id)
        yield Response(Status.NoContent, emptyHeaders, Body.empty))
          .catchAll(err => ZIO.succeed(encodeError(err)))
      case _ =>
        ZIO.succeed(notFoundResponse)

  val routes: Routes[BookRepository, Response] =
    Routes(
      RoutePattern.any -> Handler.fromFunctionZIO(routeRequest)
    )
