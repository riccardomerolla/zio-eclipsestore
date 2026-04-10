# Scala-Native Engine Samples

This page is written in an `mdoc`-friendly style and documents the sample applications that exercise the Scala-native engine roadmap.

For backend-level setup and runtime semantics, see the dedicated [`NativeLocal Guide`](native-local-guide.md).

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
- backend selection via `BackendConfig` and `StorageBackend.rootServices(...)`
- backwards-compatible schema evolution via `MigrationPlan`

For a local-first runtime, the same sample can switch to `NativeLocal` without changing its service surface:

```scala
val nativeLocalLayer =
  BookstoreWorkflow.layer(
    BackendConfig.NativeLocal(Paths.get("bookstore-data/bookstore.snapshot.json"))
  )
```

There is also an event-sourced NativeLocal server variant for the bookstore HTTP example. It reuses the same `BookRoutes`, but its repository runs on the first-class NativeLocal eventing stack: stream journal, snapshot store, optimistic expected-version checks, and policy-driven snapshotting.

See:

- [`BookstoreEventSourcing.scala`](../examples/bookstore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/examples/bookstore/domain/BookstoreEventSourcing.scala)
- [`BookstoreEventSourcingServer.scala`](../examples/bookstore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/examples/bookstore/BookstoreEventSourcingServer.scala)
- [`BookRepositoryEventSourcedSpec.scala`](../examples/bookstore/src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/bookstore/BookRepositoryEventSourcedSpec.scala)
- [`BookRoutesEventSourcedSpec.scala`](../examples/bookstore/src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/bookstore/BookRoutesEventSourcedSpec.scala)

## NativeLocal Todo

The todo sample is the smallest end-to-end example of the NativeLocal backend. It uses an immutable schema-derived root, `ObjectStore.modify`, and `StorageOps.restart` against a single snapshot file. There are JSON and protobuf variants.

```scala
val jsonLayer =
  TodoService.layer(Paths.get("todo-native-local.snapshot.json"))

val protobufLayer =
  TodoService.layer(
    Paths.get("todo-native-local.snapshot.pb"),
    NativeLocalSerde.Protobuf,
  )

val program =
  for
    first    <- TodoService.add("write snapshot guide")
    _        <- TodoService.add("verify restart semantics")
    _        <- TodoService.complete(first.id)
    reloaded <- TodoService.checkpointAndReload
  yield reloaded.map(todo => (todo.title, todo.completed))
```

The sample demonstrates:

- `BackendConfig.NativeLocal` as the layer-facing backend selector
- optional protobuf snapshots through `NativeLocalSerde.Protobuf`
- immutable whole-root updates through `ObjectStore.modify`
- explicit persistence via `StorageOps.checkpoint` and `StorageOps.restart`
- a single-file local-first workflow suitable for small applications

For configuration loading and snapshot lifecycle details, see the [`NativeLocal Guide`](native-local-guide.md).

## NativeLocal Todo Versioning

The versioning sample shows the current migration path for incompatible NativeLocal schema changes. It writes `TodoRootV1`, migrates the saved snapshot to `TodoRootV2`, and then continues with the upgraded model on the same snapshot path.

The example demonstrates:

- an explicit version ADT for the snapshot model
- a v1-to-v2 migration that removes `legacyCategory` and adds `priority`
- an envelope-backed snapshot that records the root id and schema fingerprint
- automatic startup migration through `NativeLocalMigrationPlan` registration in `NativeLocalSnapshotMigrationRegistry`
- rewritten snapshots that keep migration provenance in the envelope
- reopening the same NativeLocal snapshot with the v2 root after migration
- continuing to manage both migrated and newly created todos after the upgrade

See:

- [`TodoNativeLocalVersioningApp.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/examples/nativelocal/TodoNativeLocalVersioningApp.scala)
- [`TodoNativeLocalVersioningSpec.scala`](../src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/nativelocal/TodoNativeLocalVersioningSpec.scala)

## NativeLocal Todo Event Sourcing

The event-sourcing sample shows how to map a pure command handler onto the first-class NativeLocal eventing stack instead of storing journal state inside a classic NativeLocal root.

The example demonstrates:

- a pure `decide` function that returns domain events from the current projection
- a pure `replay` function that folds persisted event envelopes back into the current state
- `EventStore` append semantics with stream ids and optimistic versions
- `EventSourcedRuntime` as the effect boundary for load, decide, append, and snapshot policy
- NativeLocal eventing persistence for journal, snapshots, and relay progress

See:

- [`TodoNativeLocalEventSourcingApp.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/examples/nativelocal/TodoNativeLocalEventSourcingApp.scala)
- [`TodoNativeLocalEventSourcingSpec.scala`](../src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/nativelocal/TodoNativeLocalEventSourcingSpec.scala)

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

- roots: [`ObjectStore`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/ObjectStore.scala)
- lazy loading: [`Lazy`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/Lazy.scala)
- ACID effects: [`Transaction`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/ObjectStore.scala)
- storage targets: [`BackendConfig`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/config/BackendConfig.scala)
- migration: [`Migration`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/schema/Migration.scala)
- streaming: [`StreamingPersistence`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/schema/StreamingPersistence.scala)
- lifecycle ops: [`StorageOps`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/StorageOps.scala)
- testkit: [`PersistenceSpec`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/testkit/PersistenceSpec.scala)

## Verification

The executable checks for these examples live in:

- [`BookstoreWorkflowSpec.scala`](../examples/bookstore/src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/bookstore/BookstoreWorkflowSpec.scala)
- [`TodoNativeLocalAppSpec.scala`](../src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/nativelocal/TodoNativeLocalAppSpec.scala)
- [`TodoNativeLocalEventSourcingSpec.scala`](../src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/nativelocal/TodoNativeLocalEventSourcingSpec.scala)
- [`SemanticSearchAppSpec.scala`](../examples/gigamap-cli/src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/gigamap/SemanticSearchAppSpec.scala)
