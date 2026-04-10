package io.github.riccardomerolla.zio.eclipsestore.error

enum EclipseStoreError extends PersistenceError:
  case StorageError(message: String, cause: Option[Throwable] = None)
  case QueryError(message: String, cause: Option[Throwable] = None)
  case InitializationError(message: String, cause: Option[Throwable] = None)
  case ResourceError(message: String, cause: Option[Throwable] = None)
  case BackendError(message: String, cause: Option[Throwable] = None)
  case AuthError(message: String, cause: Option[Throwable] = None)
  case StoreNotOpen(message: String, cause: Option[Throwable] = None)
  case ConflictError(message: String, cause: Option[Throwable] = None)
  case MigrationError(message: String, cause: Option[Throwable] = None)
  case IncompatibleSchemaError(message: String, cause: Option[Throwable] = None)
