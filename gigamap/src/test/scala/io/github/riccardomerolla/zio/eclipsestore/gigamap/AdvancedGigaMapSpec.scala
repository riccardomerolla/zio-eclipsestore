package io.github.riccardomerolla.zio.eclipsestore.gigamap

import zio.*
import zio.test.*

import java.nio.file.Files
import java.time.LocalDate

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.{ GigaMapDefinition, GigaMapIndex }
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.GigaMapQuery
import io.github.riccardomerolla.zio.eclipsestore.gigamap.service.GigaMap
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import scala.jdk.CollectionConverters.*

object AdvancedGigaMapSpec extends ZIOSpecDefault:

  enum Interest:
    case SPORTS, MUSIC, TRAVEL

  final case class Person(
      firstName: String,
      lastName: String,
      dateOfBirth: LocalDate,
      country: String,
      interests: Set[Interest],
    )

  private val personIndexes = Chunk(
    GigaMapIndex.single[Person, String]("firstName", _.firstName),
    GigaMapIndex.single[Person, String]("country", _.country),
    GigaMapIndex.single[Person, Int]("birthYear", _.dateOfBirth.getYear),
    GigaMapIndex.single[Person, String]("lastName", _.lastName),
    GigaMapIndex.single[Person, Interest]("interests", _.interests.toSeq.headOption.getOrElse(Interest.SPORTS)),
  )

  private val definition = GigaMapDefinition[Int, Person]("people", personIndexes, autoPersist = false)

  private val sample = List(
    1 -> Person("Thomas", "Fischer", LocalDate.of(1999, 1, 1), "Germany", Set(Interest.SPORTS)),
    2 -> Person("Anna", "Schmidt", LocalDate.of(2000, 5, 12), "Austria", Set(Interest.MUSIC)),
    3 -> Person("Max", "Muster", LocalDate.of(1999, 3, 20), "Germany", Set(Interest.TRAVEL)),
    4 -> Person("Lena", "Schneider", LocalDate.of(1997, 8, 30), "Switzerland", Set(Interest.SPORTS)),
  )

  private def tempLayer =
    ZLayer.scoped {
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory("gigamap-advanced")))(dir =>
        ZIO.attempt {
          if Files.exists(dir) then Files.walk(dir).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
        }.orDie
      )
    }

  private def withGigaMap[A](f: GigaMap[Int, Person] => ZIO[Any, Throwable, A]) =
    ZIO.serviceWithZIO[java.nio.file.Path] { dir =>
      val cfg   = EclipseStoreConfig.make(dir)
      val layer = ZLayer.succeed(cfg) >>> EclipseStoreService.live >>> GigaMap.make[Int, Person](definition)
      ZIO.scoped {
        layer.build.flatMap { env =>
          val gm = env.get[GigaMap[Int, Person]]
          f(gm)
        }
      }
    }

  override def spec =
    suite("Advanced Gigamap scenarios")(
      test("supports indexed queries similar to upstream basic example") {
        withGigaMap { gm =>
          for
            _       <- gm.putAll(sample)
            thomas  <- gm.query(GigaMapQuery.ByIndex("firstName", "Thomas"))
            germans <- gm.query(GigaMapQuery.ByIndex("country", "Germany"))
            year99  <- gm.query(GigaMapQuery.ByIndex("birthYear", 1999))
            lastSch <- gm.query(GigaMapQuery.Filter(p => p.lastName.contains("sch")))
            sports  <- gm.query(GigaMapQuery.ByIndex("interests", Interest.SPORTS))
          yield assertTrue(
            thomas.map(_.firstName).toSet == Set("Thomas"),
            germans.size == 2,
            year99.size == 2,
            lastSch.exists(_.lastName.contains("sch")),
            sports.exists(_.interests.contains(Interest.SPORTS)),
          )
        }
      }
    ).provideLayerShared(tempLayer)
