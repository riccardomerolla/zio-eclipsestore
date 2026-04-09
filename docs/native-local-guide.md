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

- [`BackendConfig.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/config/BackendConfig.scala)
- [`StorageBackend.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/StorageBackend.scala)
- [`NativeLocal.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/NativeLocal.scala)
- [`NativeLocalSTM.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/NativeLocalSTM.scala)
- [`SnapshotCodec.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/SnapshotCodec.scala)
- [`NativeLocalObjectStore.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/testkit/NativeLocalObjectStore.scala)

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

## Optional STM Adapter

If you want STM composition over the same NativeLocal root, use the additive [`NativeLocalSTM.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/NativeLocalSTM.scala) service through `NativeLocal.liveWithSTM(...)`.

The adapter commits STM updates into the shared root state and keeps `ObjectStore` and `StorageOps` semantics intact. It is intended as a stretch tool for local workflows, not as a replacement for the core backend contracts.

## Testkit Support

For specs, the repository exposes scoped temporary NativeLocal layers through [`NativeLocalObjectStore.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/testkit/NativeLocalObjectStore.scala):

- `tempLayer(...)` for `ObjectStore[Root] & StorageOps[Root]`
- `tempLayerWithSTM(...)` for `ObjectStore[Root] & StorageOps[Root] & NativeLocalSTM[Root]`

These helpers create and clean up a temporary snapshot directory automatically, which makes them suitable for property-style parity suites and restart tests.

Examples:

- [`TestkitSpec.scala`](../src/test/scala/io/github/riccardomerolla/zio/eclipsestore/TestkitSpec.scala)
- [`NativeLocalSTMSpec.scala`](../src/test/scala/io/github/riccardomerolla/zio/eclipsestore/NativeLocalSTMSpec.scala)

## Example

The smallest runnable example is the todo app:

- [`TodoNativeLocalApp.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/examples/nativelocal/TodoNativeLocalApp.scala)
- [`TodoNativeLocalAppSpec.scala`](../src/test/scala/io/github/riccardomerolla/zio/eclipsestore/examples/nativelocal/TodoNativeLocalAppSpec.scala)

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

If the root is an immutable `Map`, you can also layer the additive [`LocalRepo.scala`](../src/main/scala/io/github/riccardomerolla/zio/eclipsestore/service/LocalRepo.scala) helpers on top of `ObjectStore` for basic CRUD without introducing a domain-specific repository immediately.

Use the EclipseStore-backed targets when you need:

- larger mutable object graphs
- richer storage-target configuration
- existing EclipseStore operational behavior
