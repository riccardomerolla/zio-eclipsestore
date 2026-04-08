package io.github.riccardomerolla.zio.eclipsestore.schema

import zio.*
import zio.schema.Schema
import zio.stream.ZStream

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

trait StreamingPersistence[Root]:
  def descriptor: RootDescriptor[Root]
  def root: IO[EclipseStoreError, Root]
  def ingest[K: Schema, V: Schema](
    records: ZStream[Any, Nothing, IngestionItem[K, V]],
    config: IngestionConfig = IngestionConfig(),
  ): IO[EclipseStoreError, IngestionReport[K, V]]
  def extract[V: Schema](predicate: V => Boolean): ZStream[Any, EclipseStoreError, V]
  def extractBatches[V: Schema](
    predicate: V => Boolean,
    batchSize: Int,
  ): ZStream[Any, EclipseStoreError, Chunk[V]]

final case class StreamRecord[K, V](key: K, value: V)

enum IngestionItem[+K, +V]:
  case Record(key: K, value: V)
  case Malformed(payload: String, error: EclipseStoreError)

enum ErrorStrategy:
  case FailFast
  case Skip
  case Park
  case Retry(maxRetries: Int)

final case class IngestionConfig(
  batchSize: Int = 128,
  parallelism: Int = 1,
  errorStrategy: ErrorStrategy = ErrorStrategy.FailFast,
)

enum ParkedRecord[K, V]:
  case Rejected(record: StreamRecord[K, V], error: EclipseStoreError)
  case Malformed(payload: String, error: EclipseStoreError)

final case class BatchReport(
  batchIndex: Long,
  attempted: Int,
  committed: Int,
  skipped: Int,
  parked: Int,
)

final case class IngestionReport[K, V](
  attempted: Int,
  committed: Int,
  skipped: Int,
  parked: Chunk[ParkedRecord[K, V]],
  batches: Chunk[BatchReport],
)

private final case class BatchOutcome[K, V](
  committed: Int,
  skipped: Int,
  parked: Chunk[ParkedRecord[K, V]],
):
  def +(other: BatchOutcome[K, V]): BatchOutcome[K, V] =
    BatchOutcome(
      committed = committed + other.committed,
      skipped = skipped + other.skipped,
      parked = parked ++ other.parked,
    )

  def toReport(batchIndex: Long, attempted: Int): BatchReport =
    BatchReport(
      batchIndex = batchIndex,
      attempted = attempted,
      committed = committed,
      skipped = skipped,
      parked = parked.size,
    )

private object BatchOutcome:
  def empty[K, V]: BatchOutcome[K, V] =
    BatchOutcome(committed = 0, skipped = 0, parked = Chunk.empty)

final case class StreamingPersistenceLive[Root: Schema](
  descriptor: RootDescriptor[Root],
  typedStore: TypedStore,
  writeSemaphore: Semaphore,
) extends StreamingPersistence[Root]:
  override def root: IO[EclipseStoreError, Root] =
    typedStore.typedRoot(descriptor)

  override def ingest[K: Schema, V: Schema](
    records: ZStream[Any, Nothing, IngestionItem[K, V]],
    config: IngestionConfig,
  ): IO[EclipseStoreError, IngestionReport[K, V]] =
    val normalizedBatchSize   = math.max(1, config.batchSize)
    val normalizedParallelism = math.max(1, config.parallelism)

    records
      .grouped(normalizedBatchSize)
      .zipWithIndex
      .mapZIOPar(normalizedParallelism) { case (batch, index) =>
        processBatch(batch, index, config)
      }
      .runFold[IngestionReport[K, V]](IngestionReport[K, V](0, 0, 0, Chunk.empty, Chunk.empty)) {
        case (report, (attempted, outcome, batchReport)) =>
          report.copy(
            attempted = report.attempted + attempted,
            committed = report.committed + outcome.committed,
            skipped = report.skipped + outcome.skipped,
            parked = report.parked ++ outcome.parked,
            batches = report.batches :+ batchReport,
          )
      }

  override def extract[V: Schema](predicate: V => Boolean): ZStream[Any, EclipseStoreError, V] =
    extractBatches(predicate, batchSize = 128).flatMap(ZStream.fromChunk)

  override def extractBatches[V: Schema](
    predicate: V => Boolean,
    batchSize: Int,
  ): ZStream[Any, EclipseStoreError, Chunk[V]] =
    typedStore.streamAll[V].filter(predicate).grouped(math.max(1, batchSize))

  private def processBatch[K: Schema, V: Schema](
    batch: Chunk[IngestionItem[K, V]],
    batchIndex: Long,
    config: IngestionConfig,
  ): IO[EclipseStoreError, (Int, BatchOutcome[K, V], BatchReport)] =
    writeSemaphore.withPermit {
      ZIO
        .foldLeft(batch)(BatchOutcome.empty[K, V]) { (acc, item) =>
          processItem(item, config).map(acc + _)
        }
        .map(outcome => (batch.size, outcome, outcome.toReport(batchIndex, batch.size)))
    }

  private def processItem[K: Schema, V: Schema](
    item: IngestionItem[K, V],
    config: IngestionConfig,
  ): IO[EclipseStoreError, BatchOutcome[K, V]] =
    item match
      case IngestionItem.Record(key, value)       =>
        storeWithStrategy(StreamRecord(key, value), config.errorStrategy)
      case IngestionItem.Malformed(payload, error) =>
        malformedWithStrategy(payload, error, config.errorStrategy)

  private def storeWithStrategy[K: Schema, V: Schema](
    record: StreamRecord[K, V],
    strategy: ErrorStrategy,
  ): IO[EclipseStoreError, BatchOutcome[K, V]] =
    def persist: IO[EclipseStoreError, Unit] =
      typedStore.store(record.key, record.value)

    strategy match
      case ErrorStrategy.FailFast        =>
        persist.as(BatchOutcome(committed = 1, skipped = 0, parked = Chunk.empty))
      case ErrorStrategy.Skip            =>
        persist
          .as(BatchOutcome(committed = 1, skipped = 0, parked = Chunk.empty))
          .catchAll(_ => ZIO.succeed(BatchOutcome(committed = 0, skipped = 1, parked = Chunk.empty)))
      case ErrorStrategy.Park            =>
        persist
          .as(BatchOutcome(committed = 1, skipped = 0, parked = Chunk.empty))
          .catchAll(error =>
            ZIO.succeed(
              BatchOutcome(
                committed = 0,
                skipped = 0,
                parked = Chunk.single(ParkedRecord.Rejected(record, error)),
              )
            )
          )
      case ErrorStrategy.Retry(retries) =>
        persist.retry(Schedule.recurs(math.max(0, retries))).as(BatchOutcome(committed = 1, skipped = 0, parked = Chunk.empty))

  private def malformedWithStrategy[K, V](
    payload: String,
    error: EclipseStoreError,
    strategy: ErrorStrategy,
  ): IO[EclipseStoreError, BatchOutcome[K, V]] =
    strategy match
      case ErrorStrategy.FailFast | ErrorStrategy.Retry(_) =>
        ZIO.fail(error)
      case ErrorStrategy.Skip                              =>
        ZIO.succeed(BatchOutcome(committed = 0, skipped = 1, parked = Chunk.empty))
      case ErrorStrategy.Park                              =>
        ZIO.succeed(
          BatchOutcome(
            committed = 0,
            skipped = 0,
            parked = Chunk.single(ParkedRecord.Malformed(payload, error)),
          )
        )

object StreamingPersistence:
  def live[Root: Tag: Schema](descriptor: RootDescriptor[Root]): ZLayer[TypedStore, Nothing, StreamingPersistence[Root]] =
    ZLayer.fromZIO {
      for
        typedStore <- ZIO.service[TypedStore]
        semaphore  <- Semaphore.make(1)
      yield StreamingPersistenceLive(descriptor, typedStore, semaphore)
    }

  def root[Root: Tag]: ZIO[StreamingPersistence[Root], EclipseStoreError, Root] =
    ZIO.serviceWithZIO[StreamingPersistence[Root]](_.root)

  def ingest[Root: Tag, K: Schema, V: Schema](
    records: ZStream[Any, Nothing, IngestionItem[K, V]],
    config: IngestionConfig = IngestionConfig(),
  ): ZIO[StreamingPersistence[Root], EclipseStoreError, IngestionReport[K, V]] =
    ZIO.serviceWithZIO[StreamingPersistence[Root]](_.ingest(records, config))

  def extract[Root: Tag, V: Schema](predicate: V => Boolean): ZStream[StreamingPersistence[Root], EclipseStoreError, V] =
    ZStream.serviceWithStream[StreamingPersistence[Root]](_.extract(predicate))

  def extractBatches[Root: Tag, V: Schema](
    predicate: V => Boolean,
    batchSize: Int,
  ): ZStream[StreamingPersistence[Root], EclipseStoreError, Chunk[V]] =
    ZStream.serviceWithStream[StreamingPersistence[Root]](_.extractBatches(predicate, batchSize))
