# Migration Guide: zio-eclipsestore 1.x → 2.x (EclipseStore 4.x)

This guide explains how to upgrade from zio-eclipsestore 1.x (EclipseStore 1.x) to 2.x (EclipseStore 4.x).

## Overview

EclipseStore 4.0.0 introduced significant architectural improvements, including:
- Native vector similarity search in GigaMap
- Enhanced performance and memory management
- Improved concurrency support
- New query types for semantic search

## Breaking Changes

### 1. Build.sbt Update

**Before (1.x):**
```scala
libraryDependencies ++= Seq(
  "io.github.riccardomerolla" %% "zio-eclipsestore" % "1.0.5",
  "org.eclipse.store" % "storage-embedded" % "1.0.3",
  "org.eclipse.store" % "storage-embedded-configuration" % "1.0.3"
)
```

**After (2.x):**
```scala
libraryDependencies ++= Seq(
  "io.github.riccardomerolla" %% "zio-eclipsestore" % "2.0.0",
  "org.eclipse.store" % "storage-embedded" % "4.0.0-beta1",
  "org.eclipse.store" % "storage-embedded-configuration" % "4.0.0-beta1"
)
```

### 2. Core API Changes

**Not breaking:** The core `EclipseStoreService` API remains unchanged. Existing code for CRUD operations should work without modifications.

**New:** `GigaMapQuery` now includes a `VectorSimilarity` variant:

```scala
// Old: queries were All, Filter, ByIndex, Count
// New: Also supports VectorSimilarity

sealed trait GigaMapQuery[V, +A]
object GigaMapQuery:
  final case class All[V]() extends GigaMapQuery[V, Chunk[V]]
  final case class Filter[V](predicate: V => Boolean) extends GigaMapQuery[V, Chunk[V]]
  final case class ByIndex[V, A](index: String, value: A) extends GigaMapQuery[V, Chunk[V]]
  final case class VectorSimilarity[V](
      indexName: String,
      vector: Array[Float],
      limit: Int = 10,
      threshold: Option[Float] = None
  ) extends GigaMapQuery[V, Chunk[V]]
  final case class Count[V]() extends GigaMapQuery[V, Long]
```

### 3. GigaMapDefinition Update

**Before (1.x):**
```scala
val definition = GigaMapDefinition(
  name = "books",
  indexes = Chunk(
    GigaMapIndex.single("author", (b: Book) => b.author)
  )
)
```

**After (2.x) - With Optional Vector Index:**
```scala
val definition = GigaMapDefinition(
  name = "books",
  indexes = Chunk(
    GigaMapIndex.single("author", (b: Book) => b.author)
  ),
  vectorIndexes = Chunk(
    // NEW: Optional vector indexes for semantic search
    GigaMapVectorIndex("embedding", (b: Book) => b.embedding, dimension = 384)
  )
)
```

**Note:** Existing code without vector indexes will work unchanged. The `vectorIndexes` field has a default empty `Chunk`.

## New Features

### Vector Similarity Search

Vector similarity search enables semantic search operations. Instead of exact matches, you can find items by semantic similarity.

**Use Case Example: Document Search**

```scala
case class Document(
  id: String,
  content: String,
  embedding: Array[Float]  // Vector embedding from your ML model
)

// 1. Define the map with a vector index
val docDef = GigaMapDefinition[String, Document](
  name = "documents",
  vectorIndexes = Chunk(
    GigaMapVectorIndex("text", (doc: Document) => doc.embedding, dimension = 384)
  )
)

// 2. Create the map
val docMap = GigaMap.make[String, Document](docDef)

// 3. Add documents
for
  _ <- docMap.put("doc1", Document("doc1", "...", embeddingA))
  _ <- docMap.put("doc2", Document("doc2", "...", embeddingB))
yield ()

// 4. Search by vector similarity
val queryVector = embedder.embed("find similar documents")
val results = docMap.query(GigaMapQuery.VectorSimilarity(
  indexName = "text",
  vector = queryVector,
  limit = 10,      // Top 10 matches
  threshold = Some(0.75f)  // At least 75% similarity
))
```

### Vector Similarity Implementation Details

- **Similarity Metric:** Cosine similarity (0.0 to 1.0)
- **Search Strategy:** Linear scan with distance filtering (optimized for in-memory datasets)
- **Thread-Safe:** Concurrent reads/writes to vector indexes

## Migration Checklist

- [ ] Update Scala version to 3.3+ (if using older version)
- [ ] Update ZIO to 2.0+ (typically already compatible with 1.x)
- [ ] Update `build.sbt` with new EclipseStore 4.x versions
- [ ] Rebuild and test all existing CRUD operations (backward compatible)
- [ ] Review GigaMapDefinitions for potential vector index additions
- [ ] Update domain models to include embedding vectors if using semantic search
- [ ] Test with embedded ML models or external embedding services

## Testing

### Unit Tests

Existing tests should pass without modification:

```bash
sbt test
```

### Integration Tests

If you have custom integration tests:

```bash
sbt it:test
```

## Troubleshooting

### Issue: "Not found: VectorSimilarity"

**Cause:** Updating build.sbt but not reloading project definition.

**Solution:**
```bash
sbt reload
sbt clean compile
```

### Issue: Vector embeddings not persisting

**Cause:** Vector indexes are stored separately from regular indexes.

**Solution:** Ensure `autoPersist` is not disabled in `GigaMapDefinition`:

```scala
val definition = GigaMapDefinition(
  name = "docs",
  vectorIndexes = Chunk(...),
  autoPersist = true  // Default: true
)
```

### Issue: "Infinite recursive call" warning during compilation

**Cause:** Redundant apply method in `GigaMapVectorIndex` companion object.

**Solution:** This is already fixed in the latest version. Clean and rebuild if you encounter this:

```bash
sbt clean compile
```

## Performance Considerations

### Vector Similarity Search

- **Complexity:** O(n) per query (linear scan over all embeddings)
- **Optimization:** Use `limit` and `threshold` to filter results early
- **Memory:** Vector indexes stored in-memory alongside regular indexes
- **Scalability:** Suitable for datasets up to ~1M documents (adjust based on vector dimension)

### Migration Impact

- **Disk Space:** Minimal increase (only if storing vector indexes)
- **Memory:** Proportional to dataset size and vector dimension (typically 4-8 bytes per float)
- **Query Performance:** No impact on existing non-vector queries

## Future Improvements

Planned enhancements for zio-eclipsestore 2.x:

- [ ] Pluggable similarity metrics (Euclidean, Manhattan, etc.)
- [ ] Approximate Nearest Neighbor (ANN) algorithms for faster search
- [ ] Built-in dimension reduction for large embeddings
- [ ] Integration with popular ML libraries (TensorFlow Lite, ONNX)
- [ ] Batch vector updates optimization

## References

- [EclipseStore 4.0.0 Release](https://github.com/eclipse-store/store)
- [GigaMap Vector Indexing Docs](https://docs.eclipsestore.io/manual/gigamap/indexing/jvector/use-cases.html) (Official EclipseStore)
- [zio-eclipsestore GitHub](https://github.com/riccardomerolla/zio-eclipsestore)

## Support

For issues or questions:

1. Check [GitHub Issues](https://github.com/riccardomerolla/zio-eclipsestore/issues)
2. Review the [README.md](README.md) and examples
3. Open a new issue with reproduction steps

---

**Version:** 2.x  
**EclipseStore:** 4.x  
**Scala:** 3.3+  
**ZIO:** 2.0+
