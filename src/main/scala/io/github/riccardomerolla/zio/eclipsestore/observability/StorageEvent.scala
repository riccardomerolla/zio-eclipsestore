package io.github.riccardomerolla.zio.eclipsestore.observability

import zio.Duration

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

/** Typed observability vocabulary for persistence, operations, and contention signals. */
enum StorageEvent:
  case OperationSucceeded(
    operation: String,
    duration: Duration,
    persistedObjects: Int = 0,
    detail: Option[String] = None,
  )
  case OperationFailed(
    operation: String,
    duration: Duration,
    error: EclipseStoreError,
  )
  case SlowOperationDetected(
    operation: String,
    duration: Duration,
    threshold: Duration,
  )
  case LockContentionDetected(
    operation: String,
    duration: Duration,
    threshold: Duration,
  )

  def operationName: String =
    this match
      case StorageEvent.OperationSucceeded(operation, _, _, _)  => operation
      case StorageEvent.OperationFailed(operation, _, _)        => operation
      case StorageEvent.SlowOperationDetected(operation, _, _)  => operation
      case StorageEvent.LockContentionDetected(operation, _, _) => operation

  def message: String =
    this match
      case StorageEvent.OperationSucceeded(operation, duration, persistedObjects, detail) =>
        val persistedPart = if persistedObjects > 0 then s", persistedObjects=$persistedObjects" else ""
        val detailPart    = detail.fold("")(value => s", detail=$value")
        s"storage operation succeeded: operation=$operation, duration=$duration$persistedPart$detailPart"
      case StorageEvent.OperationFailed(operation, duration, error)                       =>
        s"storage operation failed: operation=$operation, duration=$duration, error=$error"
      case StorageEvent.SlowOperationDetected(operation, duration, threshold)             =>
        s"slow storage operation detected: operation=$operation, duration=$duration, threshold=$threshold"
      case StorageEvent.LockContentionDetected(operation, duration, threshold)            =>
        s"lock contention detected: operation=$operation, duration=$duration, threshold=$threshold"
