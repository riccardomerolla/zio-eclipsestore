package io.github.riccardomerolla.zio.eclipsestore.error

enum EclipseStoreError:
  case StorageError(message: String, cause: Option[Throwable] = None)
  case QueryError(message: String, cause: Option[Throwable] = None)
  case InitializationError(message: String, cause: Option[Throwable] = None)
  case ResourceError(message: String, cause: Option[Throwable] = None)
