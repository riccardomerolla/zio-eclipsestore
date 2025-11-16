# zio-eclipsestore

A ZIO-based library for type-safe, efficient, and boilerplate-free access to [EclipseStore](https://github.com/eclipse-store/store).

## Features

- **Type-Safe API**: Leverage ZIO's type system for compile-time safety
- **Automatic Query Batching**: Batchable queries are optimized automatically
- **Parallel Execution**: Un-batchable queries execute in parallel with controlled concurrency
- **Typed Root Instances**: Declaratively describe and access root aggregates
- **Lifecycle Management**: Checkpoints, backups, and restarts via `LifecycleCommand`
- **Streaming Persistence**: Stream keys/values and batch updates with `putAll`/`persistAll`
- **Resource Safety**: ZIO's resource management ensures proper cleanup
- **ZIO Schema Integration**: Schema-derived codecs for seamless serialization
- **Effect-Oriented**: All operations are ZIO effects for composability

## Getting Started

Add the dependency to your `build.sbt`:

```scala
libraryDependencies += "io.github.riccardomerolla" %% "zio-eclipsestore" % "0.1.0"
```

### Quick Example

```scala
import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.domain.{Query, RootDescriptor}
import io.github.riccardomerolla.zio.eclipsestore.service.{EclipseStoreService, LifecycleCommand}
import zio.*
import scala.collection.mutable.ListBuffer

object MyApp extends ZIOAppDefault:
  def run =
    val program = for
      // Batch store values
      _ <- EclipseStoreService.putAll(
        List("user:1" -> "Alice", "user:2" -> "Bob", "user:3" -> "Charlie")
      )
      
      // Retrieve values
      user <- EclipseStoreService.get[String, String]("user:1")
      _ <- ZIO.logInfo(s"User: ${user.getOrElse("not found")}")
      
      // Stream all values
      streamed <- EclipseStoreService.streamValues[String].runCollect
      _ <- ZIO.logInfo(s"All users: ${streamed.mkString(", ")}")
      
      // Work with typed roots
      favoritesDescriptor = RootDescriptor(
        id = "favorite-users",
        initializer = () => ListBuffer.empty[String]
      )
      favorites <- EclipseStoreService.root(favoritesDescriptor)
      _ <- ZIO.succeed(favorites.addOne("user:1"))
      _ <- EclipseStoreService.maintenance(LifecycleCommand.Checkpoint)
      
      // Execute multiple queries in batch
      queries = List(
        Query.Get[String, String]("user:1"),
        Query.Get[String, String]("user:2"),
        Query.Get[String, String]("user:3")
      )
      results <- EclipseStoreService.executeMany(queries)
      _ <- ZIO.logInfo(s"Batch query results: ${results.mkString(", ")}")
    yield ()
    
    program.provide(
      EclipseStoreConfig.temporaryLayer,
      EclipseStoreService.live
    )
```

### Reference Examples

We ported the original EclipseStore “getting started” and “embedded storage basics” walkthroughs to ZIO:

| Example | Main Class | Highlights |
| --- | --- | --- |
| Getting Started | `io.github.riccardomerolla.zio.eclipsestore.examples.gettingstarted.GettingStartedApp` | Simple put/get/batch usage |
| Embedded Storage Basics | `io.github.riccardomerolla.zio.eclipsestore.examples.gettingstarted.EmbeddedStorageBasicsApp` | Typed roots and maintenance lifecycle |

Run them with `sbt "runMain full.qualified.ClassName"`.

### Bookstore Backend Demo

The `BookstoreServer` example reimplements the backend portion of EclipseStore’s [Bookstore demo](https://github.com/eclipse-store/bookstore-demo) using:

- `zio-eclipsestore` for persistence and typed roots
- `zio-http` for the REST API
- `zio-json` codecs for request/response bodies

Main entry point: `io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.BookstoreServer`

```
sbt bookstore/run
```

Default port: `8080`. Available routes:

| Method & Path | Description |
| --- | --- |
| `GET /books` | List all books |
| `POST /books` | Create a book (`CreateBookRequest`) |
| `GET /books/{id}` | Fetch a single book |
| `PUT /books/{id}` | Update mutable fields (`UpdateBookRequest`) |
| `DELETE /books/{id}` | Remove a book and persist the root |

All write operations automatically persist the `BookstoreRoot` and you can trigger checkpoints/backups via `EclipseStoreService` if desired.

### Configuration

Configure your EclipseStore instance:

```scala
import java.nio.file.Paths
import io.github.riccardomerolla.zio.eclipsestore.config.{EclipseStoreConfig, StoragePerformanceConfig, StorageTarget}

// Use a specific storage target and tweak performance options
val config = EclipseStoreConfig(
  storageTarget = StorageTarget.FileSystem(Paths.get("/data/mystore")),
  performance = StoragePerformanceConfig(
    channelCount = 8,
    useOffHeapPageStore = true
  )
)

// Or use temporary in-memory storage for testing
val tempConfig = EclipseStoreConfig.temporary
```

### Storage Targets & Lifecycle Operations

`StorageTarget` lets you choose between `FileSystem`, `MemoryMapped`, `InMemory`, or provide a custom foundation. Lifecycle hooks are exposed as strongly typed commands:

```scala
import java.nio.file.Paths
import io.github.riccardomerolla.zio.eclipsestore.service.{EclipseStoreService, LifecycleCommand}

for
  _ <- EclipseStoreService.maintenance(LifecycleCommand.Checkpoint)
  _ <- EclipseStoreService.maintenance(LifecycleCommand.Backup(Paths.get("/data/backup")))
  status <- EclipseStoreService.status
yield status
```

### Query API

Execute type-safe queries:

```scala
import io.github.riccardomerolla.zio.eclipsestore.domain.{Query, RootDescriptor}

val queries = List(
  Query.Get[String, String]("key1"),
  Query.Get[String, String]("key2"),
  Query.GetAllValues[String]()
)

// Execute with automatic batching and parallel execution
EclipseStoreService.executeMany(queries)

// Run a custom query against the root context
val countUsers = Query.Custom[Int](
  operation = "count-users",
  run = ctx => ctx.container.ensure(RootDescriptor.concurrentMap[Any, Any]("kv-root")).size()
)
EclipseStoreService.execute(countUsers)
```

## Development Workflow

- Keep all side effects inside `ZIO.*` constructors
- Model errors with sealed ADTs (`enum EclipseStoreError`)
- Inject dependencies with `ZLayer` and access them via `ZIO.serviceWithZIO`
- Validate behavior via `zio-test` and the default `ZTestFramework`

### Building

```bash
sbt compile
```

### Testing

```bash
sbt test
```

### Running the Example

```bash
sbt "runMain io.github.riccardomerolla.zio.eclipsestore.app.EclipseStoreApp"
```

## Key Concepts

### Query Batching

Queries marked as `batchable` are executed sequentially in an optimized manner, while non-batchable queries execute in parallel:

```scala
// These queries will be batched together
val batch = List(
  Query.Get[String, String]("key1"),
  Query.Get[String, String]("key2"),
  Query.Put("key3", "value3")
)

// This query will execute in parallel with others like it
val nonBatch = Query.GetAllValues[String]()
```

### Resource Management

The service uses ZIO's `Scope` for automatic resource cleanup, health monitoring, and optional auto-checkpoints:

```scala
val live: ZLayer[EclipseStoreConfig, EclipseStoreError, EclipseStoreService] =
  ZLayer.scoped {
    // Resources are acquired...
    // Finalizers ensure cleanup on shutdown
  }
```

## Contributing

Contributions are welcome! Please:
1. Respect the coding guidelines in `AGENTS.md`
2. Add or update tests for every change
3. Document noteworthy behavior in `README.md` and `CHANGELOG.md`
4. Run `sbt test` before opening a PR

## License

MIT License – see [LICENSE](LICENSE) for details.
