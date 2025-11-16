# zio-eclipsestore

A ZIO-based library for type-safe, efficient, and boilerplate-free access to [EclipseStore](https://github.com/eclipse-store/store).

## Features

- **Type-Safe API**: Leverage ZIO's type system for compile-time safety
- **Automatic Query Batching**: Batchable queries are optimized automatically
- **Parallel Execution**: Un-batchable queries execute in parallel with controlled concurrency
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
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import zio.*

object MyApp extends ZIOAppDefault:
  def run =
    val program = for
      // Store values
      _ <- EclipseStoreService.put("user:1", "Alice")
      _ <- EclipseStoreService.put("user:2", "Bob")
      
      // Retrieve values
      user <- EclipseStoreService.get[String, String]("user:1")
      _ <- ZIO.logInfo(s"User: ${user.getOrElse("not found")}")
      
      // Get all values
      allUsers <- EclipseStoreService.getAll[String]
      _ <- ZIO.logInfo(s"All users: ${allUsers.mkString(", ")}")
    yield ()
    
    program.provide(
      EclipseStoreConfig.temporaryLayer,
      EclipseStoreService.live
    )
```

### Configuration

Configure your EclipseStore instance:

```scala
import java.nio.file.Paths
import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig

// Use a specific storage path
val config = EclipseStoreConfig.make(Paths.get("/data/mystore"))

// Or use temporary storage for testing
val tempConfig = EclipseStoreConfig.temporary
```

### Query API

Execute type-safe queries:

```scala
import io.github.riccardomerolla.zio.eclipsestore.domain.Query

val queries = List(
  Query.Get[String, String]("key1"),
  Query.Get[String, String]("key2"),
  Query.GetAllValues[String]()
)

// Execute with automatic batching and parallel execution
EclipseStoreService.executeMany(queries)
```

### Project Structure

```
src/
  main/
    scala/io/github/riccardomerolla/zio/eclipsestore/
      app/EclipseStoreApp.scala           // example application
      config/EclipseStoreConfig.scala      // configuration model
      domain/Query.scala                   // query types
      error/EclipseStoreError.scala        // typed errors
      service/EclipseStoreService.scala    // main service
  test/
    scala/io/github/riccardomerolla/zio/eclipsestore/
      EclipseStoreServiceSpec.scala        // comprehensive tests
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

The service uses ZIO's `Scope` for automatic resource cleanup:

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
