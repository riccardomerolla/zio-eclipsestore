package io.github.riccardomerolla.zio.eclipsestore.error

import io.github.riccardomerolla.zio.eclipsestore.service.{ ExpectedVersion, StreamId, StreamRevision }

enum EventStoreError extends PersistenceError:
  case StorageError(message: String, cause: Option[Throwable] = None)
  case CorruptStream(streamId: StreamId, message: String, cause: Option[Throwable] = None)
  case WrongExpectedVersion(
    streamId: StreamId,
    expected: ExpectedVersion,
    actual: Option[StreamRevision],
    cause: Option[Throwable] = None,
  )
