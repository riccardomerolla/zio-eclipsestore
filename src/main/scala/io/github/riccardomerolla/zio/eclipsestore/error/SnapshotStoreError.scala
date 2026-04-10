package io.github.riccardomerolla.zio.eclipsestore.error

import io.github.riccardomerolla.zio.eclipsestore.service.StreamId

enum SnapshotStoreError extends PersistenceError:
  case StorageError(message: String, cause: Option[Throwable] = None)
  case CorruptSnapshot(streamId: StreamId, message: String, cause: Option[Throwable] = None)
