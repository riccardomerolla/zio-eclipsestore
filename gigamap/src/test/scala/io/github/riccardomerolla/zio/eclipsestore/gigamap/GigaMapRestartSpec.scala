package io.github.riccardomerolla.zio.eclipsestore.gigamap

import zio.*
import zio.test.*

import java.nio.file.{ Files, Path }
import java.util.Comparator

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.*
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.GigaMapQuery
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError
import io.github.riccardomerolla.zio.eclipsestore.gigamap.service.GigaMap
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

/**
 * Ports a subset of the upstream restart/consistency suites:
 *  - writes a larger dataset with multiple indexes
 *  - reloads from disk and verifies indexes and counts remain consistent
 *  - exercises concurrent updates/removals before restart
 */
object GigaMapRestartSpec extends ZIOSpecDefault:

  final case class Person(id: Int, name: String, city: String, country: String, age: Int)

  private val definition =
    GigaMapDefinition[Int, Person](
      name = "people-advanced",
      indexes = Chunk(
        GigaMapIndex.single("city", _.city),
        GigaMapIndex.single("country", _.country),
        GigaMapIndex.single("ageBucket", p => s"${p.age / 10 * 10}s"),
      ),
      autoPersist = true,
    )

  private def persistentLayer(path: Path): ZLayer[Any, Nothing, GigaMap[Int, Person]] =
    ZLayer.succeed(EclipseStoreConfig.make(path)) >>>
      EclipseStoreService.live.mapError(e => GigaMapError.StorageFailure("Failed to init store", None)) >>>
      GigaMap.make[Int, Person](definition).mapError(identity).orDie

  private def withMap[E, A](f: GigaMap[Int, Person] => ZIO[Any, E, A]): ZIO[GigaMap[Int, Person], E, A] =
    ZIO.serviceWithZIO[GigaMap[Int, Person]](f)

  private def runWithLayer[E, A](layer: ZLayer[Any, Nothing, GigaMap[Int, Person]])(
      zio: ZIO[GigaMap[Int, Person], E, A]
    ): ZIO[Any, E, A] =
    ZIO.scoped {
      layer.build.flatMap(env => zio.provideEnvironment(env))
    }

  private def deleteDirectory(path: Path): Unit =
    if Files.exists(path) then
      Files
        .walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.delete)

  private val dataset: Chunk[Person] =
    Chunk.fromIterable(
      (1 to 500).map { id =>
        val city    = if id % 2 == 0 then "Berlin" else "Hamburg"
        val country = if id % 3 == 0 then "DE" else "EU"
        Person(id, s"Person-$id", city, country, 18 + (id % 40))
      }
    )

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("GigaMap restart & consistency")(
      test("persists dataset and reloads indexes across restarts") {
        ZIO.scoped {
          for
            path         <- ZIO.attemptBlocking(Files.createTempDirectory("gigamap-restart"))
            _            <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer         = persistentLayer(path)
            _            <- runWithLayer(layer) {
                              withMap(_.putAll(dataset.map(p => p.id -> p))) *>
                                withMap(_.persist)
                            }
            restoredSize <- runWithLayer(layer) {
                              for
                                count   <- withMap(_.query(GigaMapQuery.Count[Person]()))
                                berlin  <- withMap(_.query(GigaMapQuery.ByIndex("city", "Berlin")))
                                forties <- withMap(_.query(GigaMapQuery.ByIndex("ageBucket", "40s")))
                              yield assertTrue(
                                count == dataset.size,
                                berlin.nonEmpty,
                                forties.exists(_.age >= 40),
                              )
                            }
          yield restoredSize
        }
      },
      test("concurrent updates and removals keep indexes consistent after restart") {
        ZIO.scoped {
          for
            path        <- ZIO.attemptBlocking(Files.createTempDirectory("gigamap-restart-updates"))
            _           <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer        = persistentLayer(path)
            _           <- runWithLayer(layer) {
                             for
                               _ <- withMap(_.putAll(dataset.map(p => p.id -> p)))
                               _ <- ZIO.foreachParDiscard(dataset.filter(_.id % 5 == 0)) { p =>
                                      val updated = p.copy(city = "Munich", age = p.age + 1)
                                      withMap(_.put(updated.id, updated))
                                    }.withParallelism(16)
                               _ <- ZIO.foreachParDiscard(dataset.filter(_.id % 7 == 0)) { p =>
                                      withMap(_.remove(p.id)).unit
                                    }.withParallelism(16)
                               _ <- withMap(_.persist)
                             yield ()
                           }
            assertion  <- runWithLayer(layer) {
                             for
                               total    <- withMap(_.query(GigaMapQuery.Count[Person]()))
                               munich   <- withMap(_.query(GigaMapQuery.ByIndex("city", "Munich")))
                               berlin   <- withMap(_.query(GigaMapQuery.ByIndex("city", "Berlin")))
                               removed   = dataset.count(_.id % 7 == 0)
                               expected  = dataset.size - removed
                             yield assertTrue(
                               total == expected,
                               munich.forall(_.city == "Munich"),
                               berlin.forall(_.city == "Berlin"),
                             )
                           }
          yield assertion
        }
      },
    )
