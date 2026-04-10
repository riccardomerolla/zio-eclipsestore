# zio-eclipsestore

Type-safe persistence for Scala 3 + ZIO on top of EclipseStore.

## TL;DR

- Effect-first API (`ZIO` all the way)
- Typed roots and schema-driven handler registration
- Automatic query batching and controlled parallel execution
- Backend abstraction for EclipseStore targets and NativeLocal snapshots
- GigaMap module with indexes and vector similarity search (EclipseStore 4.x)

If you want an opinionated, local-friendly persistence layer that still feels idiomatic in ZIO apps, this is it.

## Install

Use the latest published version for your modules.

```scala
libraryDependencies ++= Seq(
  "io.github.riccardomerolla" %% "zio-eclipsestore" % "<latest>",
  "io.github.riccardomerolla" %% "zio-eclipsestore-gigamap" % "<latest>", // optional
  "io.github.riccardomerolla" %% "zio-eclipsestore-storage-sqlite" % "<latest>" // optional
)
```

## 30-Second Example

```scala
import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import zio.*

object MyApp extends ZIOAppDefault:
  def run =
    (for
      _      <- EclipseStoreService.put("user:1", "Alice")
      stored <- EclipseStoreService.get[String, String]("user:1")
      _      <- ZIO.logInfo(s"User: ${stored.getOrElse("missing")}")
    yield ())
      .provide(
        EclipseStoreConfig.temporaryLayer,
        EclipseStoreService.live,
      )
```

## What You Get

- Typed API for key-value operations, roots, lifecycle commands, and query execution
- Schema integration via `RootDescriptor.fromSchema` and `TypedStore`
- Streaming read/write primitives (`ZStream`-friendly)
- Production knobs: performance tuning, custom handlers, backup targets, import/export
- NativeLocal mode for whole-root local-first snapshots

## Documentation Map

Start here:
- [docs/getting-started.md](docs/getting-started.md)
- [docs/usage-recipes.md](docs/usage-recipes.md)
- [docs/gigamap-and-vector-search.md](docs/gigamap-and-vector-search.md)

Deep dives:
- [docs/native-local-guide.md](docs/native-local-guide.md)
- [docs/schema-serializer.md](docs/schema-serializer.md)
- [docs/scala-native-engine-contract.md](docs/scala-native-engine-contract.md)
- [docs/scala-native-engine-samples.md](docs/scala-native-engine-samples.md)

## Run

```bash
sbt compile
sbt test
```

Examples:

```bash
sbt "runMain io.github.riccardomerolla.zio.eclipsestore.examples.gettingstarted.GettingStartedApp"
sbt "runMain io.github.riccardomerolla.zio.eclipsestore.examples.nativelocal.TodoNativeLocalApp"
sbt bookstore/run
sbt gigamap/test
```

## Compatibility

| Module line | EclipseStore | Scala | ZIO |
| --- | --- | --- | --- |
| 2.x | 4.x | 3.5+ | 2.1+ |

## Contributing

- Follow project guidelines in [AGENTS.md](AGENTS.md)
- Add/update tests for behavior changes
- Keep docs in sync when APIs change

## License

MIT. See [LICENSE](LICENSE).
