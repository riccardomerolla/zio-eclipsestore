# Scala-Native Engine Samples

This page is written in an `mdoc`-friendly style and documents the sample applications that exercise the Scala-native engine roadmap.

## Bookstore v2

The bookstore sample uses typed roots, `ObjectStore`, `StorageOps`, and a migration plan for legacy data.

```scala
val workflowLayer =
  BookstoreWorkflow.layer(BackendConfig.FileSystem(Paths.get("bookstore-data")))

val program =
  for
    _        <- BookstoreWorkflow.createBook(
                  CreateBookRequest("Zionomicon", "John de Goes", BigDecimal(49), Chunk("zio", "scala"))
                )
    _        <- BookstoreWorkflow.backup(Paths.get("bookstore-backup"))
    catalog  <- BookstoreWorkflow.restartAndInventory
    migrated <- BookstoreMigrations.migrate(
                  LegacyBookRecord(BookId(UUID.randomUUID()), "Legacy title", "Legacy author", BigDecimal(19))
                )
  yield (catalog.sorted.map(_.title), migrated.shelf)
```

The sample demonstrates:

- typed roots via `BookstoreRoot.descriptor`
- ACID-style updates via `ObjectStore.transact`
- operational lifecycle via `StorageOps.backup` and `StorageOps.restart`
- backwards-compatible schema evolution via `MigrationPlan`

## Semantic Search

The vector-search sample uses `GigaMap` with a vector index and a secondary scalar index.

```scala
val documents = Chunk(
  SemanticDocument("engine", "Scala-native engine", "architecture", "...", Chunk(0.99f, 0.02f, 0.01f)),
  SemanticDocument("vectors", "Vector indexes", "search", "...", Chunk(0.97f, 0.03f, 0.01f))
)

val searchProgram =
  for
    _       <- SemanticSearchSample.indexAll(documents)
    nearest <- SemanticSearchSample.nearestNeighbors(Chunk(1.0f, 0.0f, 0.0f), limit = 2)
    topic   <- SemanticSearchSample.byTopic("search")
  yield (nearest.map(_.value.id), topic.map(_.id))
```

The sample demonstrates:

- `GigaMapVectorIndex` for semantic retrieval
- `GigaMapIndex.single` for exact-match filtering
- combined example coverage for search and operational docs

## Engine Concepts Covered

These samples and their specs cover the roadmap topics expected from the adoption track:

- roots: [`ObjectStore`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/ObjectStore.scala)
- lazy loading: [`Lazy`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/Lazy.scala)
- ACID effects: [`Transaction`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/ObjectStore.scala)
- storage targets: [`BackendConfig`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/config/BackendConfig.scala)
- migration: [`Migration`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/schema/Migration.scala)
- streaming: [`StreamingPersistence`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/schema/StreamingPersistence.scala)
- lifecycle ops: [`StorageOps`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/StorageOps.scala)
- testkit: [`PersistenceSpec`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/testkit/PersistenceSpec.scala)

## Verification

The executable checks for these examples live in:

- [`BookstoreWorkflowSpec.scala`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/examples/bookstore/src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/bookstore/BookstoreWorkflowSpec.scala)
- [`SemanticSearchAppSpec.scala`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/examples/gigamap-cli/src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/gigamap/SemanticSearchAppSpec.scala)
