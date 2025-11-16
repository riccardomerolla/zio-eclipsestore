package io.github.riccardomerolla.zio.eclipsestore.examples.gigamap

import zio.*
import zio.Console
import zio.Console.{ printLine, printLineError }
import zio.cli.*
import zio.cli.HelpDoc.Span.text

import java.nio.file.{ Files, Paths }

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.{ GigaMapDefinition, GigaMapIndex }
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.GigaMapQuery
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError
import io.github.riccardomerolla.zio.eclipsestore.gigamap.service.GigaMap
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import scala.jdk.CollectionConverters.*
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

  private val helpCommands = Set("help", "?")
  private val exitCommands = Set("exit", "quit")
  private val prompt       = "gigamap> "

  val bookMapDefinition =
    GigaMapDefinition[Int, Book](
      name = "gigamap-books",
      indexes = Chunk(
        GigaMapIndex.single("title", _.title),
        GigaMapIndex.single("year", _.year),
        GigaMapIndex("author", _.authors),
      ),
    )

  private val helpLines = Chunk(
    "GigaMap Bookstore REPL commands:",
    "  insert <id;title;author1,author2;year>",
    "  list",
    "  findByTitle <title>",
    "  findByAuthor <author>",
    "  delete <id>",
    "  count",
    "  help",
    "  exit",
  )

  private def printHelp: UIO[Unit] =
    ZIO.foreachDiscard(helpLines)(line => printLine(line).orDie)

  private val insertCommand                =
    Command("insert", Args.text("payload"))
      .withHelp(HelpDoc.p(text("Insert book: id;title;author1,author2;year")))
      .map(Action.Insert.apply)
  private val listCommand                  =
    Command("list").withHelp("List all books").map(_ => Action.ListBooks)
  private val findByTitleCommand           =
    Command("findByTitle", Args.text("title")).withHelp("Find books by title index").map(Action.FindByTitle.apply)
  private val findByAuthorCommand          =
    Command("findByAuthor", Args.text("author")).withHelp("Find books by author index").map(Action.FindByAuthor.apply)
  private val deleteCommand                =
    Command("delete", Args.integer("id")).withHelp("Delete book by id").map(id => Action.Delete(id.toInt))
  private val countCommand                 =
    Command("count").withHelp("Count books").map(_ => Action.Count)
  private val rootCommand: Command[Action] =
    insertCommand | listCommand | findByTitleCommand | findByAuthorCommand | deleteCommand | countCommand

  private val dataPath = Paths.get("/tmp/data/gigamap")

  private val configLayer: ULayer[EclipseStoreConfig] =
    ZLayer.fromZIO {
      ZIO.attempt {
        Files.createDirectories(dataPath)
        EclipseStoreConfig.make(dataPath)
      }.orDie
    }

  private val layer =
    configLayer >>>
      EclipseStoreService.live.mapError(e => new RuntimeException(e.toString)) >>>
      GigaMap.make(bookMapDefinition).orDie

  private val cliCommand =
    CliApp.make(
      name = "gigamap-repl",
      version = "0.1.0",
      summary = text("GigaMap Bookstore commands"),
      command = rootCommand,
    ) { action =>
      runAction(action)
        .flatMap(lines => ZIO.foreachDiscard(lines)(line => printLine(line).orDie))
        .as(ExitCode.success)
    }

  private def withMap[A](f: GigaMap[Int, Book] => ZIO[Any, GigaMapError, A]): ZIO[GigaMap[Int, Book], Nothing, A] =
    ZIO.serviceWithZIO[GigaMap[Int, Book]](f).orDie

  def runAction(action: Action): ZIO[GigaMap[Int, Book], Nothing, Chunk[String]] =
    action match
      case Action.Insert(payload)      =>
        Book.fromInput(payload.split(";").toList) match
          case Right(book) =>
            withMap(_.put(book.id, book)).as(Chunk(s"Inserted ${pretty(book)}"))
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

  private def pretty(book: Book): String =
    val authorsText =
      Try {
        def collectScalaList(node: Any): List[String] =
          val buffer  = List.newBuilder[String]
          var current = node
          while current.isInstanceOf[scala.collection.immutable.::[?]] do
            val cons = current.asInstanceOf[scala.collection.immutable.::[Any]]
            cons.head match
              case s: String if s != null && s.nonEmpty => buffer += s
              case _                                    => ()
            current = cons.tail
          buffer.result()

        val normalized =
          book.authors.asInstanceOf[Any] match
            case null                                   => Nil
            case _: scala.collection.immutable.Nil.type => Nil
            case cons: scala.collection.immutable.::[?] => collectScalaList(cons)
            case iterable: Iterable[?]                  =>
              iterable.iterator.collect { case s: String if s != null && s.nonEmpty => s }.toList
            case javaIterable: java.lang.Iterable[?]    =>
              javaIterable
                .asInstanceOf[java.lang.Iterable[String | Null]]
                .asScala
                .collect { case s if s != null && s.nonEmpty => s }
                .toList
            case other                                  =>
              other.toString :: Nil
        if normalized.isEmpty then "Unknown author" else normalized.mkString(", ")
      }.getOrElse("Unknown author")
    s"${book.id}: ${book.title} by $authorsText (${book.year})"

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    for
      args <- getArgs
      exit <-
        if args.nonEmpty then runSingle(args.toList)
        else runRepl
    yield exit

  private def runSingle(args: List[String]): URIO[Scope, ExitCode] =
    cliCommand
      .run(args)
      .provideSomeLayer[Scope](layer)
      .foldZIO(
        err => printLineError(err.toString).orDie.as(ExitCode.failure),
        exit => ZIO.succeed(exit.getOrElse(ExitCode.success)),
      )

  private def runRepl: URIO[Scope, ExitCode] =
    (printHelp *> replLoop)
      .provideSomeLayer[Scope](layer)
      .foldZIO(
        err => printLineError(err.toString).orDie.as(ExitCode.failure),
        _ => ZIO.succeed(ExitCode.success),
      )

  private def replLoop: ZIO[GigaMap[Int, Book], Nothing, Unit] =
    for
      _        <- Console.print(prompt).orDie
      line     <- Console.readLine.orElseSucceed("").map(_.trim)
      continue <- handleInput(line)
      _        <- if continue then replLoop else ZIO.unit
    yield ()

  private def handleInput(line: String): ZIO[GigaMap[Int, Book], Nothing, Boolean] =
    if line.isEmpty then ZIO.succeed(true)
    else if helpCommands.contains(line.toLowerCase) then printHelp.as(true)
    else if exitCommands.contains(line.toLowerCase) then printLine("Goodbye!").orDie.as(false)
    else
      parseAction(line) match
        case Left("")      => ZIO.succeed(true)
        case Left(message) => printLine(message).orDie.as(true)
        case Right(action) =>
          runAction(action)
            .flatMap(lines => ZIO.foreachDiscard(lines)(line => printLine(line).orDie))
            .as(true)

  private def parseAction(line: String): Either[String, Action] =
    val parts = line.trim.split("\\s+", 2)
    parts.headOption match
      case None                 => Left("")
      case Some("insert")       =>
        parts.lift(1).filter(_.nonEmpty).map(Action.Insert.apply).toRight("Usage: insert <id;title;authors;year>")
      case Some("list")         =>
        Right(Action.ListBooks)
      case Some("findByTitle")  =>
        parts.lift(1).filter(_.nonEmpty).map(Action.FindByTitle.apply).toRight("Usage: findByTitle <title>")
      case Some("findByAuthor") =>
        parts.lift(1).filter(_.nonEmpty).map(Action.FindByAuthor.apply).toRight("Usage: findByAuthor <author>")
      case Some("delete")       =>
        parts
          .lift(1)
          .filter(_.nonEmpty)
          .flatMap(arg => Try(arg.toInt).toOption)
          .map(Action.Delete.apply)
          .toRight("Usage: delete <id>")
      case Some("count")        =>
        Right(Action.Count)
      case Some(other)          =>
        Left(s"Unknown command: $other")
