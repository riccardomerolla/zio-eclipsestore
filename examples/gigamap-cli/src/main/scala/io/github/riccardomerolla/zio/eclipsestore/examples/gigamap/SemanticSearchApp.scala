package io.github.riccardomerolla.zio.eclipsestore.examples.gigamap

import zio.*
import zio.Console.printLine

import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.{ GigaMapDefinition, GigaMapIndex, GigaMapVectorIndex }
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.{ GigaMapQuery, GigaMapScored }
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError
import io.github.riccardomerolla.zio.eclipsestore.gigamap.service.GigaMap
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

final case class SemanticDocument(
  id: String,
  title: String,
  topic: String,
  body: String,
  embedding: Chunk[Float],
)

trait SemanticSearchSample:
  def indexAll(documents: Chunk[SemanticDocument]): IO[GigaMapError, Unit]
  def nearestNeighbors(queryVector: Chunk[Float], limit: Int): IO[GigaMapError, Chunk[GigaMapScored[SemanticDocument]]]
  def byTopic(topic: String): IO[GigaMapError, Chunk[SemanticDocument]]

final case class SemanticSearchSampleLive(
  map: GigaMap[String, SemanticDocument]
) extends SemanticSearchSample:
  override def indexAll(documents: Chunk[SemanticDocument]): IO[GigaMapError, Unit] =
    map.putAll(documents.map(document => document.id -> document))

  override def nearestNeighbors(
    queryVector: Chunk[Float],
    limit: Int,
  ): IO[GigaMapError, Chunk[GigaMapScored[SemanticDocument]]] =
    map.query(GigaMapQuery.VectorSearch("embedding", queryVector, limit))

  override def byTopic(topic: String): IO[GigaMapError, Chunk[SemanticDocument]] =
    map.query(GigaMapQuery.ByIndex("topic", topic))

object SemanticSearchSample:
  val definition: GigaMapDefinition[String, SemanticDocument] =
    GigaMapDefinition(
      name = "semantic-search-sample",
      indexes = Chunk(
        GigaMapIndex.single("topic", _.topic)
      ),
      vectorIndexes = Chunk(
        GigaMapVectorIndex(
          name = "embedding",
          extract = _.embedding.toArray,
          dimension = 3,
        )
      ),
    )

  val live: ZLayer[EclipseStoreService, GigaMapError, SemanticSearchSample] =
    GigaMap.make(definition) >>> ZLayer.fromFunction(SemanticSearchSampleLive.apply)

  def indexAll(documents: Chunk[SemanticDocument]): ZIO[SemanticSearchSample, GigaMapError, Unit] =
    ZIO.serviceWithZIO[SemanticSearchSample](_.indexAll(documents))

  def nearestNeighbors(
    queryVector: Chunk[Float],
    limit: Int = 3,
  ): ZIO[SemanticSearchSample, GigaMapError, Chunk[GigaMapScored[SemanticDocument]]] =
    ZIO.serviceWithZIO[SemanticSearchSample](_.nearestNeighbors(queryVector, limit))

  def byTopic(topic: String): ZIO[SemanticSearchSample, GigaMapError, Chunk[SemanticDocument]] =
    ZIO.serviceWithZIO[SemanticSearchSample](_.byTopic(topic))

object SemanticSearchApp extends ZIOAppDefault:
  private val sampleDocuments = Chunk(
    SemanticDocument(
      id = "zio-engine",
      title = "Scala-native persistence engine",
      topic = "architecture",
      body = "Typed roots, ACID transactions, and ZLayer storage targets.",
      embedding = Chunk(0.99f, 0.05f, 0.01f),
    ),
    SemanticDocument(
      id = "vector-search",
      title = "Vector indexes in GigaMap",
      topic = "search",
      body = "Approximate nearest-neighbor queries for semantic lookup.",
      embedding = Chunk(0.96f, 0.02f, 0.04f),
    ),
    SemanticDocument(
      id = "ops-runbook",
      title = "Operations and backup runbook",
      topic = "operations",
      body = "Checkpoint, backup, restore, and housekeeping flows.",
      embedding = Chunk(0.08f, 0.98f, 0.01f),
    ),
  )

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    (for
      _       <- SemanticSearchSample.indexAll(sampleDocuments)
      matches <- SemanticSearchSample.nearestNeighbors(Chunk(1.0f, 0.0f, 0.0f), limit = 2)
      _       <- ZIO.foreachDiscard(matches) { scored =>
                   printLine(f"${scored.value.title} [${scored.score}%.3f]").orDie
                 }
    yield ())
      .provide(
        EclipseStoreService.inMemory,
        SemanticSearchSample.live.orDie,
      )
