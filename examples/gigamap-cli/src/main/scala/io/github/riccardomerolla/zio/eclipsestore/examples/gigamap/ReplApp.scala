package io.github.riccardomerolla.zio.eclipsestore.examples.gigamap

import zio.*
import zio.Console.{ printLine, printLineError }
import zio.cli.*
import zio.cli.CliError
import zio.cli.HelpDoc
import zio.cli.HelpDoc.Span.text

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.{ GigaMapDefinition, GigaMapIndex }
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.GigaMapQuery
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError
import io.github.riccardomerolla.zio.eclipsestore.gigamap.service.GigaMap
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import scala.util.Try

final case class Book(id: Int, title: String, authors: List[String], year: Int)

object Book:
  def fromInput(fields: List[String]): Either[String, Book] =
    fields match
      case id :: title :: authors :: year :: Nil =>
        for
          idValue   <- Try(id.toInt).toEither.left.map(_ => s"Invalid id: $id")
          yearValue <- Try(year.toInt).toEither.left.map(_ => s"Invalid year: $year")
        yield Book(idValue, title, authors.split(",").map(_.trim).toList, yearValue)
      case _                                     =>
        Left("Expected format: <id>;<title>;<authors comma separated>;<year>")

object ReplApp extends ZIOAppDefault:

  sealed trait Action
  object Action:
    case class Insert(payload: String)      extends Action
    case object ListBooks                   extends Action
    case class FindByTitle(title: String)   extends Action
    case class FindByAuthor(author: String) extends Action
    case class Delete(id: Int)              extends Action
    case object Count                       extends Action

  val bookMapDefinition =
    GigaMapDefinition[Int, Book](
      name = "gigamap-books",
      indexes = Chunk(
        GigaMapIndex.single("title", _.title),
        GigaMapIndex.single("year", _.year),
        GigaMapIndex("author", _.authors),
      ),
    )

  private val insertCommand =
    Command("insert", Args.text("payload"))
      .withHelp(HelpDoc.p(text("Insert book: id;title;author1,author2;year")))
      .map(Action.Insert.apply)

  private val listCommand =
    Command("list")
      .withHelp("List all books")
      .map(_ => Action.ListBooks)

  private val findByTitleCommand =
    Command("findByTitle", Args.text("title"))
      .withHelp("Find books by title index")
      .map(Action.FindByTitle.apply)

  private val findByAuthorCommand =
    Command("findByAuthor", Args.text("author"))
      .withHelp("Find books by author index")
      .map(Action.FindByAuthor.apply)

  private val deleteCommand =
    Command("delete", Args.integer("id"))
      .withHelp("Delete book by id")
      .map(id => Action.Delete(id.toInt))

  private val countCommand =
    Command("count")
      .withHelp("Count books")
      .map(_ => Action.Count)

  private val rootCommand: Command[Action] =
    insertCommand | listCommand | findByTitleCommand | findByAuthorCommand | deleteCommand | countCommand

  private val layer =
    EclipseStoreConfig.temporaryLayer >>>
      EclipseStoreService.live.mapError(e => new RuntimeException(e.toString)) >>>
      GigaMap.make(bookMapDefinition).orDie

  private def pretty(book: Book): String =
    s"${book.id}: ${book.title} by ${book.authors.mkString(", ")} (${book.year})"

  private def withMap[A](f: GigaMap[Int, Book] => ZIO[Any, GigaMapError, A]): ZIO[GigaMap[Int, Book], Nothing, A] =
    ZIO.serviceWithZIO[GigaMap[Int, Book]](f).orDie

  def runAction(action: Action): ZIO[GigaMap[Int, Book], Nothing, Chunk[String]] =
    action match
      case Action.Insert(payload)      =>
        Book.fromInput(payload.split(";").toList) match
          case Right(book) =>
            for _ <- withMap(_.put(book.id, book))
            yield Chunk(s"Inserted ${pretty(book)}")
          case Left(error) =>
            ZIO.succeed(Chunk(error))
      case Action.ListBooks            =>
        withMap(_.entries).map(entries => entries.map { case (_, book) => pretty(book) })
      case Action.FindByTitle(title)   =>
        withMap(_.query(GigaMapQuery.ByIndex("title", title))).map(_.map(pretty))
      case Action.FindByAuthor(author) =>
        withMap(_.query(GigaMapQuery.ByIndex("author", author))).map(_.map(pretty))
      case Action.Delete(id)           =>
        withMap(_.remove(id)).map {
          case Some(book) => Chunk(s"Removed ${pretty(book)}")
          case None       => Chunk(s"No book found with id $id")
        }
      case Action.Count                =>
        withMap(_.query(GigaMapQuery.Count[Book]())).map(count => Chunk(s"Total books: $count"))

  val cliApp =
    CliApp.make(
      name = "gigamap-repl",
      version = "0.1.0",
      summary = text("GigaMap Bookstore REPL"),
      command = rootCommand,
    )(action =>
      runAction(action)
        .flatMap(lines => ZIO.foreachDiscard(lines)(line => printLine(line).orDie))
        .as(ExitCode.success)
    )

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    for
      args <- getArgs
      exit <- cliApp
                .run(args.toList)
                .provideSomeLayer[Scope](layer)
                .foldZIO(
                  err => printLineError(err.toString).orDie.as(Some(ExitCode.failure)),
                  success => ZIO.succeed(success),
                )
    yield exit.getOrElse(ExitCode.success)
