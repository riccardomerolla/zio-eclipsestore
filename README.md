# zio-eclipsestore

Persistence for Scala 3 + ZIO that favors explicit effects, typed roots, and practical local-first workflows over ORM-style magic.

## TL;DR

- Built for teams that want persistence to feel like ZIO, not like hidden runtime behavior
- Typed roots + schema-driven handler registration, so model drift is harder to miss
- Automatic batching for batchable queries, controlled parallelism for everything else
- One API, multiple backends: EclipseStore targets and NativeLocal snapshots
- GigaMap support, including vector similarity search on EclipseStore 4.x

If your default is "make effects explicit, keep architecture boring, ship fast," this library is optimized for that path.

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

## Why NativeLocal Exists

NativeLocal is not a second-class demo backend. It is a deliberate implementation for local-first, single-process applications where simplicity and determinism matter more than distributed complexity.

- Use NativeLocal when you want a whole-root snapshot model with explicit checkpoint/restart semantics
- Use NativeLocal when you want predictable local development, fast startup, and easy test isolation
- Use EclipseStore-backed targets when you need the full storage foundation and broader backend integration

The point is choice without rewriting your service layer: keep the same root model and swap backend behavior at the edge.

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
