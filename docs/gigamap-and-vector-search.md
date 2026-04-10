# GigaMap and Vector Search

Use this guide for indexed map queries and semantic similarity search.

## Define a map with regular and vector indexes

```scala
import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.{ GigaMapDefinition, GigaMapIndex, GigaMapVectorIndex }
import zio.Chunk

case class Book(id: String, title: String, author: String, embedding: Array[Float])

val definition = GigaMapDefinition(
  name = "books",
  indexes = Chunk(
    GigaMapIndex.single("author", (book: Book) => book.author)
  ),
  vectorIndexes = Chunk(
    GigaMapVectorIndex("embedding", (book: Book) => book.embedding, dimension = 384)
  ),
)
```

## Run vector similarity queries

```scala
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.GigaMapQuery

val queryEmbedding: Array[Float] = Array.fill(384)(0.0f)

val query = GigaMapQuery.VectorSimilarity(
  indexName = "embedding",
  vector = queryEmbedding,
  limit = 10,
  threshold = Some(0.8f),
)
```

## Standalone VectorIndexService

```scala
import io.github.riccardomerolla.zio.eclipsestore.gigamap.vector.*
import zio.*

case class Product(id: Long, name: String, embedding: Chunk[Float])

object ProductVectorizer extends Vectorizer[Product]:
  def vectorize(p: Product): Chunk[Float] = p.embedding
  def isEmbedded: Boolean = true

val config = VectorIndexConfig(
  dimension = 768,
  similarityFunction = SimilarityFunction.Cosine,
  persistenceIntervalMs = Some(30_000L),
)

val program: ZIO[VectorIndexService, VectorError, Unit] =
  for
    idx <- VectorIndexService.createIndex[Product]("products", "search", config, ProductVectorizer)
    _   <- idx.add(1L, Product(1L, "Laptop", Chunk.fill(768)(0.1f)))
    _   <- VectorIndexService.search[Product]("search", Chunk.fill(768)(0.1f), k = 5)
  yield ()
```

## Try it locally

```bash
sbt gigamap/test
sbt "gigamapCli/run"
```

The interactive CLI supports insert/list/find/delete/count/help.
