package io.github.riccardomerolla.zio.eclipsestore.error

enum OutboxError extends PersistenceError:
  case StorageError(message: String, cause: Option[Throwable] = None)
  case PublishError(message: String, cause: Option[Throwable] = None)
