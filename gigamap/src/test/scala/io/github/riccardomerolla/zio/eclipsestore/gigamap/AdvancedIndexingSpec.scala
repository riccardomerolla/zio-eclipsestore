package io.github.riccardomerolla.zio.eclipsestore.gigamap

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.*
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.{ GigaMapPredicate, GigaMapQuery }
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError
import io.github.riccardomerolla.zio.eclipsestore.gigamap.service.GigaMap
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

final case class PlaceDocument(
  id: String,
  city: String,
  status: String,
  location: GeoPoint,
  embedding: Array[Float],
)

object AdvancedIndexingSpec extends ZIOSpecDefault:
  private val definition =
    GigaMapDefinition[String, PlaceDocument](
      name = "advanced-documents",
      indexes = Chunk(
        GigaMapIndex.single("city", _.city),
        GigaMapIndex.single("status", _.status),
      ),
      vectorIndexes = Chunk(
        GigaMapVectorIndex("embedding", _.embedding, dimension = 3)
      ),
      spatialIndexes = Chunk(
        GigaMapSpatialIndex("location", _.location)
      ),
      autoPersist = false,
    )

  private val layer =
    EclipseStoreService.inMemory >>> GigaMap.make(definition)

  private val romeCenter   = GeoPoint(41.9028, 12.4964)
  private val parisCenter  = GeoPoint(48.8566, 2.3522)
  private val berlinCenter = GeoPoint(52.5200, 13.4050)

  private val docs = Chunk(
    PlaceDocument("rome-a", "Rome", "active", romeCenter, Array(1.0f, 0.0f, 0.0f)),
    PlaceDocument("rome-b", "Rome", "active", GeoPoint(41.91, 12.49), Array(0.94f, 0.30f, 0.0f)),
    PlaceDocument("paris-a", "Paris", "active", parisCenter, Array(0.0f, 1.0f, 0.0f)),
    PlaceDocument("berlin-a", "Berlin", "archived", berlinCenter, Array(-1.0f, 0.0f, 0.0f)),
  )

  private def withMap[A](
    effect: GigaMap[String, PlaceDocument] => IO[GigaMapError, A]
  ): ZIO[GigaMap[String, PlaceDocument], GigaMapError, A] =
    ZIO.serviceWithZIO[GigaMap[String, PlaceDocument]](effect)

  private val seed =
    ZIO.foreachDiscard(docs)(doc => withMap(_.put(doc.id, doc)))

  private def runWithLayer[A](
    effect: ZIO[GigaMap[String, PlaceDocument], GigaMapError, A]
  ): ZIO[Any, GigaMapError, A] =
    ZIO.scoped {
      layer.build.flatMap(env => effect.provideEnvironment(env))
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Advanced indexing")(
      test("nearest-neighbor queries return top results with expected similarity bounds") {
        runWithLayer {
          for
          _       <- seed
          results <- withMap(
                       _.query(
                         GigaMapQuery.VectorSearch(
                           indexName = "embedding",
                           vector = Chunk(1.0f, 0.0f, 0.0f),
                           limit = 2,
                           minScore = Some(0.8f),
                         )
                       )
                     )
          yield assertTrue(
            results.length == 2,
            results.headOption.exists(_.value.id == "rome-a"),
            results.forall(_.score >= 0.8f),
            results(0).score >= results(1).score,
          )
        }
      },
      test("radius queries return only entities inside the geographic filter") {
        runWithLayer {
          for
          _       <- seed
          results <- withMap(_.query(GigaMapQuery.SpatialRadius("location", romeCenter, radiusKilometers = 5.0)))
          yield assertTrue(results.map(_.id).toSet == Set("rome-a", "rome-b"))
        }
      },
      test("composite queries intersect bitmap, vector, and spatial predicates") {
        runWithLayer {
          for
          _       <- seed
          results <- withMap(
                       _.query(
                         GigaMapQuery.Composite(
                           Chunk(
                             GigaMapPredicate.ByIndex("city", "Rome"),
                             GigaMapPredicate.ByIndex("status", "active"),
                             GigaMapPredicate.VectorSearch(
                               "embedding",
                               Chunk(1.0f, 0.0f, 0.0f),
                               limit = 3,
                               minScore = Some(0.9f),
                             ),
                             GigaMapPredicate.SpatialRadius("location", romeCenter, radiusKilometers = 5.0),
                           )
                         )
                       )
                     )
          yield assertTrue(results.map(_.id).toSet == Set("rome-a", "rome-b"))
        }
      },
      test("new embeddings become queryable without a full rebuild") {
        val madrid = PlaceDocument(
          "madrid-a",
          "Madrid",
          "active",
          GeoPoint(40.4168, -3.7038),
          Array(1.0f, 0.02f, 0.0f),
        )

        runWithLayer {
          for
          _      <- seed
          before <- withMap(_.query(GigaMapQuery.VectorSearch("embedding", Chunk(1.0f, 0.0f, 0.0f), limit = 3)))
          _      <- withMap(_.put(madrid.id, madrid))
          after  <- withMap(_.query(GigaMapQuery.VectorSearch("embedding", Chunk(1.0f, 0.0f, 0.0f), limit = 3)))
          yield assertTrue(
            !before.map(_.value.id).contains("madrid-a"),
            after.map(_.value.id).contains("madrid-a"),
          )
        }
      },
      test("concurrent readers and index writers stay consistent under overlap") {
        val lisbon = PlaceDocument(
          "lisbon-a",
          "Lisbon",
          "active",
          GeoPoint(38.7223, -9.1393),
          Array(0.98f, 0.05f, 0.0f),
        )

        ZIO.scoped {
          for
            env     <- layer.build
            _       <- ZIO.foreachDiscard(docs)(doc =>
                         ZIO.serviceWithZIO[GigaMap[String, PlaceDocument]](_.put(doc.id, doc)).provideEnvironment(env)
                       )
            readers  = ZIO.foreachPar(1 to 24)(_ =>
                         ZIO.serviceWithZIO[GigaMap[String, PlaceDocument]](
                           _.query(
                             GigaMapQuery.Composite(
                               Chunk(
                                 GigaMapPredicate.ByIndex("status", "active"),
                                 GigaMapPredicate.VectorSearch(
                                   "embedding",
                                   Chunk(1.0f, 0.0f, 0.0f),
                                   limit = 4,
                                   minScore = Some(0.0f),
                                 ),
                               )
                             )
                           )
                         ).provideEnvironment(env).either
                       )
            writer  <- ZIO.serviceWithZIO[GigaMap[String, PlaceDocument]](_.put(lisbon.id, lisbon)).provideEnvironment(env).fork
            reads   <- readers
            _       <- writer.join
            results <- ZIO.serviceWithZIO[GigaMap[String, PlaceDocument]](
                         _.query(GigaMapQuery.VectorSearch("embedding", Chunk(1.0f, 0.0f, 0.0f), limit = 5))
                       ).provideEnvironment(env)
          yield assertTrue(
            reads.forall(_.isRight),
            results.map(_.value.id).contains("lisbon-a"),
          )
        }
      },
    )
