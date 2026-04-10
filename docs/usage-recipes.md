# Usage Recipes

Common tasks with copy-pasteable snippets.

## Store and load multiple values

```scala
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

val program =
  for
    _      <- EclipseStoreService.putAll(List(
                "user:1" -> "Alice",
                "user:2" -> "Bob",
                "user:3" -> "Charlie",
              ))
    users  <- EclipseStoreService.streamValues[String].runCollect
    user1  <- EclipseStoreService.get[String, String]("user:1")
  yield (users, user1)
```

## Execute queries with automatic batching

```scala
import io.github.riccardomerolla.zio.eclipsestore.domain.Query
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

val queries = List(
  Query.Get[String, String]("user:1"),
  Query.Get[String, String]("user:2"),
  Query.GetAllValues[String](),
)

val program = EclipseStoreService.executeMany(queries)
```

## TypedStore API (schema-driven)

```scala
import io.github.riccardomerolla.zio.eclipsestore.service.TypedStore

val program =
  for
    _     <- TypedStore.store("book:1", book)
    found <- TypedStore.fetch[String, Book]("book:1")
    root  <- TypedStore.typedRoot(BookstoreRoot.descriptor)
    _     <- TypedStore.storePersist(root)
  yield found
```

For more schema details, see [schema-serializer.md](schema-serializer.md).

## Lifecycle operations (checkpoint, backup, restart)

```scala
import java.nio.file.Paths
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, LifecycleCommand }

val maintenance =
  for
    _      <- EclipseStoreService.maintenance(LifecycleCommand.Checkpoint)
    _      <- EclipseStoreService.maintenance(LifecycleCommand.Backup(Paths.get("./backup")))
    state  <- EclipseStoreService.status
  yield state
```

## Backend selection with NativeLocal

```scala
import io.github.riccardomerolla.zio.eclipsestore.config.BackendConfig
import io.github.riccardomerolla.zio.eclipsestore.service.StorageBackend
import zio.*

val layer =
  ZLayer.succeed(BackendConfig.NativeLocal(java.nio.file.Paths.get("./data/root.snapshot.json"))) >>>
    StorageBackend.rootServices(MyRoot.descriptor)
```

Contract and samples:
- [scala-native-engine-contract.md](scala-native-engine-contract.md)
- [scala-native-engine-samples.md](scala-native-engine-samples.md)
