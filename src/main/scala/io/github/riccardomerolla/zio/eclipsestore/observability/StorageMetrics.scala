package io.github.riccardomerolla.zio.eclipsestore.observability

import zio.*

trait StorageMetrics:
  def record(event: StorageEvent): UIO[Unit]
  def snapshot: UIO[StorageMetrics.Snapshot]

object StorageMetrics:
  final case class Snapshot(
    persistedObjects: Long = 0L,
    successfulOperations: Long = 0L,
    failedOperations: Long = 0L,
    slowOperations: Long = 0L,
    lockContentionEvents: Long = 0L,
    operationCounts: Map[String, Long] = Map.empty,
    durationSamplesMillis: Map[String, Chunk[Long]] = Map.empty,
    latestDurationGaugeMillis: Map[String, Long] = Map.empty,
    events: Chunk[StorageEvent] = Chunk.empty,
  )

  val inMemory: ULayer[StorageMetrics] =
    ZLayer.fromZIO {
      Ref.make(Snapshot()).map(InMemoryStorageMetrics(_))
    }

  val noop: ULayer[StorageMetrics] =
    ZLayer.succeed(
      new StorageMetrics:
        override def record(event: StorageEvent): UIO[Unit] = ZIO.unit
        override def snapshot: UIO[Snapshot]                = ZIO.succeed(Snapshot())
    )

  val snapshot: ZIO[StorageMetrics, Nothing, Snapshot] =
    ZIO.serviceWithZIO[StorageMetrics](_.snapshot)

  def record(event: StorageEvent): ZIO[StorageMetrics, Nothing, Unit] =
    ZIO.serviceWithZIO[StorageMetrics](_.record(event))

final private case class InMemoryStorageMetrics(ref: Ref[StorageMetrics.Snapshot]) extends StorageMetrics:
  override def record(event: StorageEvent): UIO[Unit] =
    ref.update(snapshot => updateSnapshot(snapshot, event))

  override def snapshot: UIO[StorageMetrics.Snapshot] =
    ref.get

  private def updateSnapshot(snapshot: StorageMetrics.Snapshot, event: StorageEvent): StorageMetrics.Snapshot =
    val withEvent = snapshot.copy(events = snapshot.events :+ event)

    event match
      case StorageEvent.OperationSucceeded(operation, duration, persistedObjects, _) =>
        registerDuration(
          increment(
            withEvent.copy(
              persistedObjects = withEvent.persistedObjects + persistedObjects.toLong,
              successfulOperations = withEvent.successfulOperations + 1L,
            ),
            s"$operation.success",
          ),
          operation,
          duration,
        )

      case StorageEvent.OperationFailed(operation, duration, _) =>
        registerDuration(
          increment(
            withEvent.copy(
              failedOperations = withEvent.failedOperations + 1L
            ),
            s"$operation.failure",
          ),
          operation,
          duration,
        )

      case StorageEvent.SlowOperationDetected(operation, duration, _) =>
        registerDuration(
          increment(
            withEvent.copy(
              slowOperations = withEvent.slowOperations + 1L
            ),
            s"$operation.slow",
          ),
          operation,
          duration,
        )

      case StorageEvent.LockContentionDetected(operation, duration, _) =>
        registerDuration(
          increment(
            withEvent.copy(
              lockContentionEvents = withEvent.lockContentionEvents + 1L
            ),
            s"$operation.contention",
          ),
          operation,
          duration,
        )

  private def increment(snapshot: StorageMetrics.Snapshot, key: String): StorageMetrics.Snapshot =
    snapshot.copy(operationCounts =
      snapshot.operationCounts.updated(key, snapshot.operationCounts.getOrElse(key, 0L) + 1L)
    )

  private def registerDuration(
    snapshot: StorageMetrics.Snapshot,
    operation: String,
    duration: Duration,
  ): StorageMetrics.Snapshot =
    val millis = duration.toMillis
    snapshot.copy(
      durationSamplesMillis = snapshot.durationSamplesMillis.updated(
        operation,
        snapshot.durationSamplesMillis.getOrElse(operation, Chunk.empty) :+ millis,
      ),
      latestDurationGaugeMillis = snapshot.latestDurationGaugeMillis.updated(operation, millis),
    )
