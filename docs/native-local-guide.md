# NativeLocal Guide

`NativeLocal` is the smallest persistence backend in this repository. It keeps a single schema-derived root in memory and persists that root as one snapshot file on demand.

It is intended for:

- single-process applications
- one logical root aggregate
- local-first workflows
- explicit checkpoint and restart control

It is not a second top-level engine API. The public surface remains `ObjectStore[Root]`, `StorageOps[Root]`, `StorageBackend`, and `BackendConfig`.

## Wiring

Use `BackendConfig.NativeLocal` together with `StorageBackend.rootServices(...)`.

```scala
import java.nio.file.Paths

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.config.BackendConfig
import io.github.riccardomerolla.zio.eclipsestore.service.StorageBackend

val backend =
  BackendConfig.NativeLocal(Paths.get("./data/app.snapshot.json"))

val services =
  ZLayer.succeed(backend) >>>
    StorageBackend.rootServices(MyRoot.descriptor)
```

The root type must have a `Schema[Root]`, and the root descriptor defines initialization and migration behavior.

Relevant implementation points:

- [`BackendConfig.scala`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/config/BackendConfig.scala)
- [`StorageBackend.scala`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/StorageBackend.scala)
- [`NativeLocal.scala`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/NativeLocal.scala)
- [`SnapshotCodec.scala`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/SnapshotCodec.scala)

## Config Loading

If you want to select the backend from HOCON, load `BackendConfig` instead of the full `EclipseStoreConfig`.

```scala
import java.nio.file.Paths

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfigZIO

val backendLayer =
  EclipseStoreConfigZIO.backendFromFile(Paths.get("application.conf"))
```

Example config:

```hocon
eclipsestore {
  backend {
    nativeLocal {
      snapshotPath = "./data/app.snapshot.json"
    }
  }
}
```

`EclipseStoreConfigZIO.backendFromResourcePath(...)` supports the same shape. If both `backend` and legacy `storageTarget` are present, `backend` wins.

## Snapshot Semantics

`NativeLocal` persists the whole root. There is no partial object-graph storage contract in this backend.

Current behavior:

- startup: load the snapshot if present, otherwise use `RootDescriptor.initializer()`
- decode failure: fail startup or restore with a typed `EclipseStoreError`
- `ObjectStore.modify`: serialize immutable whole-root updates behind a gate
- `StorageOps.checkpoint`: write the current root to the snapshot file
- `StorageOps.restart`: reload from the snapshot file
- `StorageOps.exportTo` and `backup`: copy snapshot bytes to another path
- `StorageOps.importFrom` and `restoreFrom`: load another snapshot, replace the in-memory root, then persist it as the current snapshot
- `StorageOps.shutdown`: checkpoint, then transition to `Stopped`
- `StorageOps.housekeep`: currently the same as `checkpoint`

The backend is optimized for determinism and simplicity, not multi-process coordination.

## Example

The smallest runnable example is the todo app:

- [`TodoNativeLocalApp.scala`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/main/scala/io/github/riccardomerolla/zio/eclipsestore/examples/nativelocal/TodoNativeLocalApp.scala)
- [`TodoNativeLocalAppSpec.scala`](/Users/riccardo/git/github/riccardomerolla/zio-eclipsestore/src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/nativelocal/TodoNativeLocalAppSpec.scala)

Run it with:

```bash
sbt "runMain io.github.riccardomerolla.zio.eclipsestore.examples.nativelocal.TodoNativeLocalApp"
```

That sample demonstrates:

- immutable root updates via `ObjectStore.modify`
- explicit `checkpoint` plus `restart`
- a single snapshot file on disk
- backend selection through `BackendConfig.NativeLocal`

## Choosing NativeLocal

Use `NativeLocal` when you want:

- a lightweight local-first store for a small app or utility
- schema-driven persistence without EclipseStore runtime configuration
- one-file snapshot export and restore semantics

Use the EclipseStore-backed targets when you need:

- larger mutable object graphs
- richer storage-target configuration
- existing EclipseStore operational behavior
