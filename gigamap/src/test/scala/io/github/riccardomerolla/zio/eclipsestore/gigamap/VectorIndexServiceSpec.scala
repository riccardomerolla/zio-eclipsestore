package io.github.riccardomerolla.zio.eclipsestore.gigamap

import zio.*
import zio.Chunk
import zio.test.*
import zio.test.Assertion.*

import io.github.riccardomerolla.zio.eclipsestore.gigamap.vector.*

object VectorIndexServiceSpec extends ZIOSpecDefault:

  // ---- Domain model -------------------------------------------------------

  final case class Article(id: Long, title: String, embedding: Chunk[Float])

  object ArticleVectorizer extends Vectorizer[Article]:
    def vectorize(a: Article): Chunk[Float] = a.embedding
    def isEmbedded: Boolean                 = true

  // ---- Shared test embeddings (unit/direction vectors) --------------------

  // 3-D unit vectors for predictable cosine/dot-product scores
  private val xAxis  = Chunk(1.0f, 0.0f, 0.0f) // points in X direction
  private val yAxis  = Chunk(0.0f, 1.0f, 0.0f) // 90° from X
  private val negX   = Chunk(-1.0f, 0.0f, 0.0f) // 180° from X
  private val nearX  = Chunk(0.9f, 0.436f, 0.0f) // ≈25° from X

  private val articles = List(
    Article(1L, "X axis", xAxis),
    Article(2L, "Near X", nearX),
    Article(3L, "Y axis", yAxis),
    Article(4L, "Neg X", negX),
  )

  private val cosineConfig =
    VectorIndexConfig(dimension = 3, similarityFunction = SimilarityFunction.Cosine)

  private val dotConfig =
    VectorIndexConfig(dimension = 3, similarityFunction = SimilarityFunction.DotProduct)

  private val euclideanConfig =
    VectorIndexConfig(dimension = 3, similarityFunction = SimilarityFunction.Euclidean)

  // ---- Helper: create a fresh, pre-populated index ------------------------

  private def populatedIndex(
      cfg: VectorIndexConfig,
      items: List[Article] = articles,
    ): ZIO[VectorIndexService, VectorError, VectorIndex[Article]] =
    for
      idx <- VectorIndexService.createIndex[Article]("articles", "test", cfg, ArticleVectorizer)
      _   <- ZIO.foreachDiscard(items)(a => idx.add(a.id, a))
    yield idx

  // After each test we need a fresh service — provide it per test.
  private val layer: ULayer[VectorIndexService] = VectorIndexService.live

  // ---- Specs --------------------------------------------------------------

  def spec: Spec[Environment, Any] = suite("VectorIndexServiceSpec")(
    suite("cosine similarity")(
      test("nearest neighbour of xAxis is xAxis itself (score ≈ 1.0)") {
        for
          idx     <- populatedIndex(cosineConfig)
          results <- idx.search(xAxis, k = 1)
        yield assertTrue(results.head.entityId == 1L && results.head.score > 0.99f)
      },
      test("second nearest is nearX (~0.985 cosine with xAxis)") {
        for
          idx     <- populatedIndex(cosineConfig)
          results <- idx.search(xAxis, k = 2)
        yield assertTrue(results(1).entityId == 2L)
      },
      test("negX is least similar to xAxis (score ≈ -1.0)") {
        for
          idx     <- populatedIndex(cosineConfig)
          results <- idx.search(xAxis, k = 4)
        yield assertTrue(results.last.entityId == 4L && results.last.score < -0.9f)
      },
      test("minScore filters out dissimilar results") {
        for
          idx     <- populatedIndex(cosineConfig)
          results <- idx.search(xAxis, k = 10, minScore = Some(0.5f))
        // Only xAxis and nearX should pass the 0.5 threshold
        yield assertTrue(results.length == 2 && results.forall(_.score >= 0.5f))
      },
      test("k limit is respected") {
        for
          idx     <- populatedIndex(cosineConfig)
          results <- idx.search(xAxis, k = 2)
        yield assertTrue(results.length == 2)
      },
      test("results are ordered by score descending") {
        for
          idx     <- populatedIndex(cosineConfig)
          results <- idx.search(xAxis, k = 4)
          scores   = results.map(_.score)
        yield assertTrue(scores == scores.sorted.reverse)
      },
    ),
    suite("dot-product similarity")(
      test("unit xAxis has dot product 1.0 with itself") {
        for
          idx     <- populatedIndex(dotConfig)
          results <- idx.search(xAxis, k = 1)
        yield assertTrue(results.head.score > 0.99f)
      },
      test("orthogonal vectors have dot product 0") {
        for
          idx     <- populatedIndex(dotConfig)
          results <- idx.search(xAxis, k = 4)
          yResult  = results.find(_.entityId == 3L)
        yield assertTrue(yResult.isDefined && math.abs(yResult.get.score) < 0.01f)
      },
    ),
    suite("euclidean similarity")(
      test("identical vector has euclidean distance 0 (score = 0.0)") {
        for
          idx     <- populatedIndex(euclideanConfig)
          results <- idx.search(xAxis, k = 1)
        yield assertTrue(results.head.score == 0.0f)
      },
      test("negX has the largest L2 distance from xAxis") {
        for
          idx     <- populatedIndex(euclideanConfig)
          results <- idx.search(xAxis, k = 4)
        // negX is exactly opposite: L2² = (1-(-1))²+0+0 = 4, score = -4
        yield assertTrue(results.last.entityId == 4L && results.last.score <= -3.9f)
      },
    ),
    suite("lifecycle")(
      test("size reflects add and remove operations") {
        for
          idx  <- VectorIndexService.createIndex[Article](
                    "a", "idx", cosineConfig, ArticleVectorizer
                  )
          _    <- idx.add(1L, articles.head)
          _    <- idx.add(2L, articles(1))
          s1   <- idx.size
          _    <- idx.remove(1L)
          s2   <- idx.size
        yield assertTrue(s1 == 2 && s2 == 1)
      },
      test("getIndex returns the same index that was created") {
        for
          idx      <- VectorIndexService.createIndex[Article](
                        "a", "my-idx", cosineConfig, ArticleVectorizer
                      )
          _        <- idx.add(1L, articles.head)
          fetched  <- VectorIndexService.getIndex[Article]("my-idx")
          sz       <- fetched.size
        yield assertTrue(sz == 1)
      },
      test("deleteIndex removes the index") {
        for
          _      <- VectorIndexService.createIndex[Article](
                      "a", "del-idx", cosineConfig, ArticleVectorizer
                    )
          _      <- VectorIndexService.deleteIndex("del-idx")
          result <- VectorIndexService.getIndex[Article]("del-idx").exit
        yield assert(result)(fails(isSubtype[VectorError.IndexNotFound](anything)))
      },
      test("creating duplicate index names fails") {
        for
          _ <- VectorIndexService.createIndex[Article](
                 "a", "dup", cosineConfig, ArticleVectorizer
               )
          r <- VectorIndexService.createIndex[Article](
                 "a", "dup", cosineConfig, ArticleVectorizer
               ).exit
        yield assert(r)(fails(isSubtype[VectorError.IndexAlreadyExists](anything)))
      },
    ),
    suite("error cases")(
      test("search on non-existent index fails with IndexNotFound") {
        for
          r <- VectorIndexService.search[Article]("no-such", xAxis, k = 10).exit
        yield assert(r)(fails(isSubtype[VectorError.IndexNotFound](anything)))
      },
      test("add with wrong dimension fails with DimensionMismatch") {
        for
          idx <- VectorIndexService.createIndex[Article](
                   "a", "dim-idx",
                   VectorIndexConfig(dimension = 5, similarityFunction = SimilarityFunction.Cosine),
                   ArticleVectorizer,
                 )
          // articles use 3-D embeddings but index expects 5-D
          r   <- idx.add(1L, articles.head).exit
        yield assert(r)(fails(isSubtype[VectorError.DimensionMismatch](anything)))
      },
      test("search with wrong dimension fails with DimensionMismatch") {
        for
          idx <- populatedIndex(cosineConfig)
          // supply a 5-D query to a 3-D index
          r   <- idx.search(Chunk(1f, 0f, 0f, 0f, 0f), k = 1).exit
        yield assert(r)(fails(isSubtype[VectorError.DimensionMismatch](anything)))
      },
      test("getIndex on non-existent index fails with IndexNotFound") {
        for
          r <- VectorIndexService.getIndex[Article]("missing").exit
        yield assert(r)(fails(isSubtype[VectorError.IndexNotFound](anything)))
      },
      test("deleteIndex on non-existent index fails with IndexNotFound") {
        for
          r <- VectorIndexService.deleteIndex("missing").exit
        yield assert(r)(fails(isSubtype[VectorError.IndexNotFound](anything)))
      },
    ),
    suite("streaming")(
      test("searchStream emits same results as search") {
        for
          idx      <- populatedIndex(cosineConfig)
          streamed <- idx.searchStream(xAxis, k = 4).runCollect
          listed   <- idx.search(xAxis, k = 4)
        yield assertTrue(streamed.toList == listed)
      },
      test("VectorIndexService.searchStream delegates to the index") {
        for
          _       <- VectorIndexService.createIndex[Article](
                       "a", "stream-idx", cosineConfig, ArticleVectorizer
                     )
          _       <- ZIO.foreachDiscard(articles) { a =>
                       VectorIndexService.getIndex[Article]("stream-idx").flatMap(_.add(a.id, a))
                     }
          results <- VectorIndexService
                       .searchStream[Article]("stream-idx", xAxis, k = 2)
                       .runCollect
        yield assertTrue(results.length == 2)
      },
    ),
    suite("empty index")(
      test("search on empty index returns empty list") {
        for
          idx     <- VectorIndexService.createIndex[Article](
                       "a", "empty-idx", cosineConfig, ArticleVectorizer
                     )
          results <- idx.search(xAxis, k = 10)
        yield assertTrue(results.isEmpty)
      },
    ),
  ).provide(layer)
