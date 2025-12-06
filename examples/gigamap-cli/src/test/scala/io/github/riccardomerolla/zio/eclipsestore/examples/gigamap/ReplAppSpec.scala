package io.github.riccardomerolla.zio.eclipsestore.examples.gigamap

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.examples.gigamap.ReplApp.Action
import io.github.riccardomerolla.zio.eclipsestore.gigamap.service.GigaMap
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object ReplAppSpec extends ZIOSpecDefault:

  private val mapLayer =
    EclipseStoreService.inMemory >>> GigaMap.make(ReplApp.bookMapDefinition).orDie

  private def entries: ZIO[GigaMap[Int, Book], Nothing, Chunk[(Int, Book)]] =
    ZIO.serviceWithZIO[GigaMap[Int, Book]](_.entries).orDie

  private def scenario[R](
      actions: List[Action]
    )(
      inspect: => ZIO[GigaMap[Int, Book], Nothing, TestResult]
    ): URIO[Any, TestResult] =
    (for
      _      <- ZIO.foreachDiscard(actions)(ReplApp.runAction)
      result <- inspect
    yield result).provideLayer(mapLayer)

  override def spec =
    suite("Gigamap CLI Commands")(
      test("insert adds a book") {
        scenario(List(Action.Insert("1;CLI;Alice;2024"))) {
          entries.map(es => assertTrue(es.exists(_._1 == 1)))
        }
      },
      test("list returns all books") {
        scenario(
          List(
            Action.Insert("2;List Title;Bob;2023"),
            Action.Insert("3;Extra;Sara;2021"),
            Action.ListBooks,
          )
        ) {
          entries.map(es => assertTrue(es.length == 2))
        }
      },
      test("find by title matches entry") {
        scenario(List(Action.Insert("4;Search Title;Jake;2020"))) {
          for results <- ReplApp.runAction(Action.FindByTitle("Search Title"))
          yield assertTrue(results.exists(_.contains("Search Title")))
        }
      },
      test("find by author matches entry") {
        scenario(List(Action.Insert("5;Author Title;John,Jane;2019"))) {
          ReplApp.runAction(Action.FindByAuthor("Jane")).map { results =>
            assertTrue(results.exists(_.contains("Author Title")))
          }
        }
      },
      test("delete removes a book") {
        scenario(List(Action.Insert("6;Delete Title;Mark;2018"), Action.Delete(6))) {
          entries.map(es => assertTrue(es.forall(_._1 != 6)))
        }
      },
      test("count reports total books") {
        scenario(List(Action.Insert("7;Count Title;Adam;2017"))) {
          for results <- ReplApp.runAction(Action.Count)
          yield assertTrue(results.exists(_.contains("Total books: 1")))
        }
      },
    )
      ,
      test("insert rejects malformed payload") {
        scenario(List(Action.Insert("bad-payload-without-fields"))) {
          for results <- ReplApp.runAction(Action.ListBooks)
          yield assertTrue(results.isEmpty)
        }
      }
