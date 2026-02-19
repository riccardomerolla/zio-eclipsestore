package io.github.riccardomerolla.zio.eclipsestore.gigamap

import zio.*
import zio.Chunk
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.*
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.GigaMapQuery
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError
import io.github.riccardomerolla.zio.eclipsestore.gigamap.service.GigaMap
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

final case class Document(id: String, title: String, embedding: Array[Float]):
  override def toString: String = s"Document($id, $title, embedding=[${embedding.take(3).mkString(", ")}...])"

object VectorSimilaritySpec extends ZIOSpecDefault:

  // Test embeddings: simple unit vectors for predictable results
  private val embeddingA = Array(1.0f, 0.0f, 0.0f)  // Points in X direction
  private val embeddingB = Array(0.9f, 0.436f, 0.0f)  // ~25 degrees from X  
  private val embeddingC = Array(0.0f, 1.0f, 0.0f)  // Points in Y direction (perpendicular)
  private val embeddingD = Array(-1.0f, 0.0f, 0.0f)  // Points in -X direction (opposite)

  private val definition =
    GigaMapDefinition[String, Document](
      name = "documents",
      vectorIndexes = Chunk(
        GigaMapVectorIndex("embedding", _.embedding, dimension = 3)
      ),
      autoPersist = false,
    )

  private val layer =
    EclipseStoreService.inMemory >>> GigaMap.make(definition)

  private def withMap[A](
      f: GigaMap[String, Document] => ZIO[Any, GigaMapError, A]
    ): ZIO[Any, GigaMapError, A] =
    ZIO.serviceWithZIO[GigaMap[String, Document]](f).provideLayer(layer)

  def spec: Spec[Environment, Any] = suite("VectorSimilaritySpec")(
    test("vector similarity search returns nearest neighbors") {
      withMap { map =>
          for
            // Insert documents with different embeddings
            _ <- map.put("doc-a", Document("doc-a", "Document A", embeddingA))
            _ <- map.put("doc-b", Document("doc-b", "Document B", embeddingB))
            _ <- map.put("doc-c", Document("doc-c", "Document C", embeddingC))
            _ <- map.put("doc-d", Document("doc-d", "Document D", embeddingD))
            
            // Query with embedding similar to A
            queryVec = Array(1.0f, 0.0f, 0.0f)  // Same as A
            results <- map.query(GigaMapQuery.VectorSimilarity(
              indexName = "embedding",
              vector = queryVec,
              limit = 2
            ))
          yield assertTrue(results.length > 0 && results(0).id == "doc-a")
      }
    },
    test("vector similarity with threshold filters low similarity results") {
      withMap { map =>
          for
            _ <- map.put("doc-a", Document("doc-a", "A", embeddingA))
            _ <- map.put("doc-c", Document("doc-c", "C", embeddingC))  // Perpendicular (0 similarity)
            
            // Query with high threshold
            queryVec = Array(1.0f, 0.0f, 0.0f)
            results <- map.query(GigaMapQuery.VectorSimilarity(
              indexName = "embedding",
              vector = queryVec,
              limit = 10,
              threshold = Some(0.9f)  // Only very similar items
            ))
          yield assertTrue(results.length == 1 && results(0).id == "doc-a")
      }
    },
    test("vector similarity respects limit parameter") {
      withMap { map =>
          for
            _ <- map.put("doc-a", Document("doc-a", "A", embeddingA))
            _ <- map.put("doc-b", Document("doc-b", "B", embeddingB))
            _ <- map.put("doc-c", Document("doc-c", "C", embeddingC))
            _ <- map.put("doc-d", Document("doc-d", "D", embeddingD))
            
            // Query with limit of 2
            queryVec = Array(1.0f, 0.0f, 0.0f)
            results <- map.query(GigaMapQuery.VectorSimilarity(
              indexName = "embedding",
              vector = queryVec,
              limit = 2
            ))
          yield assertTrue(results.length <= 2)
      }
    },
    test("vector similarity returns error for non-existent index") {
      withMap { map =>
        for
          _ <- map.put("doc-a", Document("doc-a", "A", embeddingA))
          result <- map
            .query(
              GigaMapQuery.VectorSimilarity[Document](
                indexName = "does-not-exist",
                vector = Array(1.0f, 0.0f, 0.0f),
                limit = 10,
              )
            )
            .exit
        yield assertTrue(result.isFailure)
      }
    },
    test("vector similarity handles empty map gracefully") {
      withMap { map =>
          for
            // Query empty map
            results <- map.query(GigaMapQuery.VectorSimilarity(
              indexName = "embedding",
              vector = Array(1.0f, 0.0f, 0.0f),
              limit = 10
            ))
          yield assertTrue(results.isEmpty)
      }
    },
  )

