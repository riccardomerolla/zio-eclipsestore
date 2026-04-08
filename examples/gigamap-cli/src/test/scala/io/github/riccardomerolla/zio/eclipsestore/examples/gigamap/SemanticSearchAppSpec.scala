package io.github.riccardomerolla.zio.eclipsestore.examples.gigamap

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object SemanticSearchAppSpec extends ZIOSpecDefault:

  private val sampleLayer =
    EclipseStoreService.inMemory >>> SemanticSearchSample.live.orDie

  private val documents = Chunk(
    SemanticDocument(
      id = "engine",
      title = "Scala-native engine",
      topic = "architecture",
      body = "Typed roots and ACID semantics.",
      embedding = Chunk(0.99f, 0.02f, 0.01f),
    ),
    SemanticDocument(
      id = "vectors",
      title = "Vector indexes",
      topic = "search",
      body = "Nearest-neighbor retrieval in GigaMap.",
      embedding = Chunk(0.97f, 0.03f, 0.01f),
    ),
    SemanticDocument(
      id = "backup",
      title = "Backup workflow",
      topic = "operations",
      body = "Operational lifecycle and restore commands.",
      embedding = Chunk(0.02f, 0.98f, 0.01f),
    ),
  )

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("SemanticSearchSample")(
      test("semantic queries return the nearest neighbors first") {
        (for
          _       <- SemanticSearchSample.indexAll(documents)
          matches <- SemanticSearchSample.nearestNeighbors(Chunk(1.0f, 0.0f, 0.0f), limit = 2)
        yield assertTrue(
          matches.map(_.value.id) == Chunk("engine", "vectors")
        )).provideLayer(sampleLayer)
      },
      test("topic index lookup stays available alongside vector search") {
        (for
          _       <- SemanticSearchSample.indexAll(documents)
          matches <- SemanticSearchSample.byTopic("operations")
        yield assertTrue(
          matches.map(_.id) == Chunk("backup")
        )).provideLayer(sampleLayer)
      },
    )
