package io.github.riccardomerolla.zio.eclipsestore.bench

import java.nio.file.{ Files, Path }
import java.time.Instant

import scala.compiletime.uninitialized
import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.schema.derived

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ NativeLocal, ObjectStore, StorageOps }

enum BenchPayloadShape:
  case Flat
  case Nested

object BenchPayloadShape:
  def parse(value: String): BenchPayloadShape =
    if value.equalsIgnoreCase("nested") then BenchPayloadShape.Nested
    else BenchPayloadShape.Flat

enum CheckpointCadence:
  case Never
  case Every100
  case Every1000

  def writeThreshold: Option[Long] =
    this match
      case CheckpointCadence.Never     => None
      case CheckpointCadence.Every100  => Some(100L)
      case CheckpointCadence.Every1000 => Some(1000L)

object CheckpointCadence:
  def parse(value: String): CheckpointCadence =
    value.toLowerCase match
      case "every-1000" | "every1000" => CheckpointCadence.Every1000
      case "every-100" | "every100"   => CheckpointCadence.Every100
      case _                          => CheckpointCadence.Never

enum StressWorkload:
  case ReadOnly
  case WriteOnly
  case Mixed8020
  case Mixed5050

object StressWorkload:
  def parse(value: String): StressWorkload =
    value.toLowerCase match
      case "read" | "read-only"     => StressWorkload.ReadOnly
      case "write" | "write-only"   => StressWorkload.WriteOnly
      case "mixed-50-50" | "50-50"  => StressWorkload.Mixed5050
      case _                        => StressWorkload.Mixed8020

final case class BenchScenarioConfig(
  serde: NativeLocalSerde,
  elementCount: Int,
  nestedPayloadSize: Int,
  concurrency: Int,
  checkpointCadence: CheckpointCadence,
  payloadShape: BenchPayloadShape,
  warmupSeconds: Int = 2,
  measurementSeconds: Int = 5,
  workload: StressWorkload = StressWorkload.Mixed8020,
  machine: Option[String] = None,
  allowUnsafeHeap: Boolean = false,
)

final case class LatencySummary(
  count: Long,
  p50Millis: Double,
  p95Millis: Double,
  p99Millis: Double,
)

object LatencySummary:
  val empty: LatencySummary =
    LatencySummary(0L, 0.0, 0.0, 0.0)

  def fromNanos(samples: Chunk[Long]): LatencySummary =
    if samples.isEmpty then empty
    else
      val sorted = samples.sorted
      def percentile(rank: Double): Double =
        val index = math.max(0, math.min(sorted.size - 1, math.ceil(sorted.size.toDouble * rank).toInt - 1))
        sorted(index).toDouble / 1_000_000.0

      LatencySummary(
        count = sorted.size.toLong,
        p50Millis = percentile(0.50),
        p95Millis = percentile(0.95),
        p99Millis = percentile(0.99),
      )

final case class BenchMetrics(
  totalOperations: Long,
  elapsedMillis: Long,
  operationsPerSecond: Double,
  loadLatency: LatencySummary,
  modifyLatency: LatencySummary,
  checkpointLatency: LatencySummary,
  restartMillis: Long,
  snapshotBytes: Long,
  heapBeforeBytes: Long,
  heapAfterWarmupBytes: Long,
  heapAfterCheckpointBytes: Long,
  heapAfterRestartBytes: Long,
)

final case class MachineMetadata(
  machine: String,
  jvmVersion: String,
  scalaVersion: String,
  sbtVersion: String,
  projectVersion: String,
)

final case class StressReport(
  scenario: BenchScenarioConfig,
  machine: MachineMetadata,
  assessment: ScenarioAssessment,
  metrics: BenchMetrics,
)

final case class ScenarioAssessment(
  safeScale: String,
  recommendedMinHeapBytes: Long,
  warnings: Chunk[String],
)

final case class FlatBenchItem(
  id: Int,
  title: String,
  priority: Int,
  payload: String,
  tags: Chunk[String],
) derives Schema

final case class FlatBenchRoot(items: Chunk[FlatBenchItem]) derives Schema

final case class NestedBenchLeaf(
  code: String,
  values: Chunk[Int],
) derives Schema

final case class NestedBenchMetadata(
  createdAtEpochMillis: Long,
  tags: Chunk[String],
  note: String,
) derives Schema

final case class NestedBenchItem(
  id: Int,
  title: String,
  metadata: NestedBenchMetadata,
  children: Chunk[NestedBenchLeaf],
) derives Schema

final case class NestedBenchRoot(items: Chunk[NestedBenchItem]) derives Schema

trait BenchHarness:
  def scenario: BenchScenarioConfig
  def snapshotPath: Path
  def seed(): IO[RuntimeException, Unit]
  def loadSize: IO[RuntimeException, Int]
  def modifyAppendOne: IO[RuntimeException, Int]
  def replaceAll(elementCount: Int): IO[RuntimeException, Int]
  def checkpoint: IO[RuntimeException, Long]
  def restartAndCount: IO[RuntimeException, Int]

object BenchHarness:
  def snapshotPath(baseDir: Path, scenario: BenchScenarioConfig): Path =
    BenchModel.snapshotPath(baseDir, scenario)

  def scoped(
    baseDir: Path,
    scenario: BenchScenarioConfig,
  ): ZIO[Scope, RuntimeException, BenchHarness] =
    scenario.payloadShape match
      case BenchPayloadShape.Flat   => FlatBenchModel.scopedHarness(baseDir, scenario)
      case BenchPayloadShape.Nested => NestedBenchModel.scopedHarness(baseDir, scenario)

  def seedSnapshot(
    baseDir: Path,
    scenario: BenchScenarioConfig,
  ): IO[RuntimeException, Unit] =
    ZIO.scoped {
      scoped(baseDir, scenario).flatMap(_.seed())
    }

  def snapshotBytes(path: Path): Long =
    if Files.exists(path) then Files.size(path) else 0L

object HeapSampler:
  def usedBytes: Long =
    val runtime = java.lang.Runtime.getRuntime
    runtime.totalMemory() - runtime.freeMemory()

private trait BenchModel:
  type Root

  given rootSchema: Schema[Root]
  given rootTag: Tag[Root]

  def descriptor: RootDescriptor[Root]
  def empty: Root
  def seed(count: Int, nestedPayloadSize: Int): Root
  def size(root: Root): Int
  def appendOne(root: Root, nestedPayloadSize: Int): Root

  def scopedHarness(baseDir: Path, scenario: BenchScenarioConfig): ZIO[Scope, RuntimeException, BenchHarness] =
    val resolvedPath     = BenchModel.snapshotPath(baseDir, scenario)
    val currentScenario  = scenario
    NativeLocal
      .live[Root](resolvedPath, descriptor, currentScenario.serde)
      .build
      .mapError(toRuntimeException)
      .map { environment =>
        new BenchHarness:
          override val scenario: BenchScenarioConfig =
            currentScenario

          override val snapshotPath: Path =
            resolvedPath

          override def seed(): IO[RuntimeException, Unit] =
            for
              _ <- ObjectStore
                     .replace(BenchModel.this.seed(currentScenario.elementCount, currentScenario.nestedPayloadSize))
                     .provideEnvironment(environment)
                     .mapError(toRuntimeException)
              _ <- StorageOps.checkpoint[Root].provideEnvironment(environment).mapError(toRuntimeException)
            yield ()

          override def loadSize: IO[RuntimeException, Int] =
            ObjectStore.load[Root].provideEnvironment(environment).map(size).mapError(toRuntimeException)

          override def modifyAppendOne: IO[RuntimeException, Int] =
            ObjectStore
              .modify[Root, Int](root =>
                ZIO.succeed {
                  val updated = appendOne(root, scenario.nestedPayloadSize)
                  (size(updated), updated)
                }
              )
              .provideEnvironment(environment)
              .mapError(toRuntimeException)

          override def replaceAll(elementCount: Int): IO[RuntimeException, Int] =
            ObjectStore
              .replace(BenchModel.this.seed(elementCount, scenario.nestedPayloadSize))
              .provideEnvironment(environment)
              .as(elementCount)
              .mapError(toRuntimeException)

          override def checkpoint: IO[RuntimeException, Long] =
            StorageOps
              .checkpoint[Root]
              .provideEnvironment(environment)
              .as(BenchHarness.snapshotBytes(snapshotPath))
              .mapError(toRuntimeException)

          override def restartAndCount: IO[RuntimeException, Int] =
            for
              _    <- StorageOps.restart[Root].provideEnvironment(environment).mapError(toRuntimeException)
              root <- ObjectStore.load[Root].provideEnvironment(environment).mapError(toRuntimeException)
            yield size(root)
      }

  private def toRuntimeException(error: EclipseStoreError): RuntimeException =
    RuntimeException(error.toString)

private object BenchModel:
  def snapshotPath(baseDir: Path, scenario: BenchScenarioConfig): Path =
    baseDir.resolve(
      s"${scenario.payloadShape.toString.toLowerCase}.${scenario.serde match
          case NativeLocalSerde.Json     => "json"
          case NativeLocalSerde.Protobuf => "pb"
        }"
    )

private object FlatBenchModel extends BenchModel:
  override type Root = FlatBenchRoot

  override given rootSchema: Schema[FlatBenchRoot] = DeriveSchema.gen[FlatBenchRoot]
  override given rootTag: Tag[FlatBenchRoot]       = Tag.derived

  override val descriptor: RootDescriptor[FlatBenchRoot] =
    RootDescriptor.fromSchema("native-local-flat-bench-root", () => empty)

  override val empty: FlatBenchRoot =
    FlatBenchRoot(Chunk.empty)

  override def seed(count: Int, nestedPayloadSize: Int): FlatBenchRoot =
    FlatBenchRoot(
      Chunk.fromIterable(0 until count).map(index =>
        FlatBenchItem(
          id = index,
          title = s"todo-$index",
          priority = index % 5,
          payload = "x" * math.max(8, nestedPayloadSize),
          tags = Chunk(s"tag-${index % 4}", s"bucket-${index % 8}"),
        )
      )
    )

  override def size(root: FlatBenchRoot): Int =
    root.items.size

  override def appendOne(root: FlatBenchRoot, nestedPayloadSize: Int): FlatBenchRoot =
    val nextId = root.items.size
    root.copy(
      items = root.items :+ FlatBenchItem(
        id = nextId,
        title = s"todo-$nextId",
        priority = nextId % 5,
        payload = "x" * math.max(8, nestedPayloadSize),
        tags = Chunk("hot-path", s"bucket-${nextId % 8}"),
      )
    )

private object NestedBenchModel extends BenchModel:
  override type Root = NestedBenchRoot

  override given rootSchema: Schema[NestedBenchRoot] = DeriveSchema.gen[NestedBenchRoot]
  override given rootTag: Tag[NestedBenchRoot]       = Tag.derived

  override val descriptor: RootDescriptor[NestedBenchRoot] =
    RootDescriptor.fromSchema("native-local-nested-bench-root", () => empty)

  override val empty: NestedBenchRoot =
    NestedBenchRoot(Chunk.empty)

  override def seed(count: Int, nestedPayloadSize: Int): NestedBenchRoot =
    NestedBenchRoot(
      Chunk.fromIterable(0 until count).map(index => nestedItem(index, nestedPayloadSize))
    )

  override def size(root: NestedBenchRoot): Int =
    root.items.size

  override def appendOne(root: NestedBenchRoot, nestedPayloadSize: Int): NestedBenchRoot =
    val nextId = root.items.size
    root.copy(items = root.items :+ nestedItem(nextId, nestedPayloadSize))

  private def nestedItem(index: Int, nestedPayloadSize: Int): NestedBenchItem =
    NestedBenchItem(
      id = index,
      title = s"nested-$index",
      metadata = NestedBenchMetadata(
        createdAtEpochMillis = Instant.EPOCH.plusSeconds(index.toLong).toEpochMilli,
        tags = Chunk(s"shape-${index % 4}", s"group-${index % 10}"),
        note = "payload" * math.max(2, nestedPayloadSize / 8),
      ),
      children = Chunk.fromIterable(0 until math.max(2, nestedPayloadSize / 4)).map(childIndex =>
        NestedBenchLeaf(
          code = s"$index-$childIndex",
          values = Chunk.fromIterable(0 until math.max(2, nestedPayloadSize / 2)).map(_ + childIndex),
        )
      ),
    )

object NativeLocalStressRunner:
  private final case class LatencyBuffers(
    load: Chunk[Long],
    modify: Chunk[Long],
    checkpoint: Chunk[Long],
  )

  private object LatencyBuffers:
    val empty: LatencyBuffers =
      LatencyBuffers(Chunk.empty, Chunk.empty, Chunk.empty)

  def run(
    scenario: BenchScenarioConfig,
    baseDir: Path,
  ): ZIO[Scope, RuntimeException, StressReport] =
    for
      assessment       <- validateScenario(scenario)
      harness          <- BenchHarness.scoped(baseDir, scenario)
      _                <- harness.seed()
      heapBefore        = HeapSampler.usedBytes
      writeCountRef    <- Ref.make(0L)
      checkpointedRef  <- Ref.make(0L)
      warmupDeadline   <- Clock.instant.map(_.plusSeconds(scenario.warmupSeconds.toLong))
      _                <- driveWorkers(harness, scenario.workload, warmupDeadline, writeCountRef, None).forkScoped.flatMap(_.join)
      heapAfterWarmup   = HeapSampler.usedBytes
      latencyRef       <- Ref.make(LatencyBuffers.empty)
      totalOpsRef      <- Ref.make(0L)
      checkpointFiber  <- startCheckpointWorker(harness, scenario.checkpointCadence, writeCountRef, checkpointedRef, latencyRef)
      measurementUntil <- Clock.instant.map(_.plusSeconds(scenario.measurementSeconds.toLong))
      startedAtNanos   <- Clock.nanoTime
      workerFibers     <- ZIO.foreach(0 until math.max(1, scenario.concurrency))(_ =>
                            driveWorkers(
                              harness,
                              scenario.workload,
                              measurementUntil,
                              writeCountRef,
                              Some((latencyRef, totalOpsRef)),
                            ).forkScoped
                          )
      _                <- ZIO.foreachDiscard(workerFibers)(_.join)
      _                <- checkpointFiber.interrupt
      finishedAtNanos  <- Clock.nanoTime
      checkpointBytes  <- harness.checkpoint
      heapAfterCkpt     = HeapSampler.usedBytes
      restartStarted   <- Clock.nanoTime
      restartedCount   <- harness.restartAndCount
      restartFinished  <- Clock.nanoTime
      heapAfterRestart  = HeapSampler.usedBytes
      latencies        <- latencyRef.get
      totalOps         <- totalOpsRef.get
      elapsedMillis     = math.max(1L, (finishedAtNanos - startedAtNanos) / 1_000_000L)
      report            = StressReport(
                            scenario = scenario,
                            machine = machineMetadata(scenario.machine),
                            assessment = assessment,
                            metrics = BenchMetrics(
                              totalOperations = totalOps,
                              elapsedMillis = elapsedMillis,
                              operationsPerSecond = totalOps.toDouble / (elapsedMillis.toDouble / 1000.0),
                              loadLatency = LatencySummary.fromNanos(latencies.load),
                              modifyLatency = LatencySummary.fromNanos(latencies.modify),
                              checkpointLatency = LatencySummary.fromNanos(latencies.checkpoint),
                              restartMillis = (restartFinished - restartStarted) / 1_000_000L,
                              snapshotBytes = checkpointBytes,
                              heapBeforeBytes = heapBefore,
                              heapAfterWarmupBytes = heapAfterWarmup,
                              heapAfterCheckpointBytes = heapAfterCkpt,
                              heapAfterRestartBytes = heapAfterRestart,
                            ),
                          )
      _                <- ZIO.fail(
                            RuntimeException(
                              s"Stress restart verification failed. Expected at least ${scenario.elementCount} elements, found $restartedCount"
                            )
                          ).when(restartedCount < scenario.elementCount)
    yield report

  private def validateScenario(scenario: BenchScenarioConfig): IO[RuntimeException, ScenarioAssessment] =
    val assessment = assessScenario(scenario)
    val maxHeap    = java.lang.Runtime.getRuntime.maxMemory()

    if !scenario.allowUnsafeHeap && maxHeap < assessment.recommendedMinHeapBytes then
      ZIO.fail(
        RuntimeException(
          s"Scenario requires more heap. Requested serde=${scenario.serde}, shape=${scenario.payloadShape}, size=${scenario.elementCount}, concurrency=${scenario.concurrency}. " +
            s"Recommended minimum heap is ${assessment.recommendedMinHeapBytes / (1024L * 1024L)} MiB, current max heap is ${maxHeap / (1024L * 1024L)} MiB. " +
            "Re-run with a larger `-J-Xmx` or pass --allow-unsafe-heap=true to bypass the guardrail."
        )
      )
    else ZIO.succeed(assessment)

  private def assessScenario(scenario: BenchScenarioConfig): ScenarioAssessment =
    val shapeFactor =
      scenario.payloadShape match
        case BenchPayloadShape.Flat   => 1L
        case BenchPayloadShape.Nested => 8L
    val serdeFactor =
      scenario.serde match
        case NativeLocalSerde.Json     => 2L
        case NativeLocalSerde.Protobuf => 4L
    val concurrencyFactor = math.max(1, scenario.concurrency).toLong
    val payloadFactor     = math.max(8, scenario.nestedPayloadSize).toLong
    val scenarioWeight    = math.max(1L, scenario.elementCount.toLong) * shapeFactor * serdeFactor * payloadFactor * concurrencyFactor
    val recommendedMiB    =
      if scenario.payloadShape == BenchPayloadShape.Nested && scenario.serde == NativeLocalSerde.Protobuf then
        math.max(256L, scenarioWeight / 16384L)
      else math.max(192L, scenarioWeight / 32768L)
    val safeScale         =
      if scenario.payloadShape == BenchPayloadShape.Flat && scenario.elementCount <= 10000 then "good"
      else if scenario.payloadShape == BenchPayloadShape.Flat then "moderate"
      else if scenario.elementCount <= 1000 then "moderate"
      else "caution"
    val warnings          =
      Chunk.fromIterable(
        List(
          Option.when(scenario.payloadShape == BenchPayloadShape.Nested && scenario.serde == NativeLocalSerde.Protobuf)(
            "Nested protobuf snapshots have the highest peak heap usage in classic NativeLocal."
          ),
          Option.when(scenario.elementCount >= 100000)(
            "100k-item runs are intended to define the upper envelope; prefer eventing if this shape is a steady-state workload."
          ),
          Option.when(scenario.concurrency >= 16)(
            "High concurrency amplifies immutable root copy cost during modify/checkpoint phases."
          ),
        ).flatten
      )

    ScenarioAssessment(
      safeScale = safeScale,
      recommendedMinHeapBytes = recommendedMiB * 1024L * 1024L,
      warnings = warnings,
    )

  private def driveWorkers(
    harness: BenchHarness,
    workload: StressWorkload,
    deadline: Instant,
    writeCountRef: Ref[Long],
    metricsRef: Option[(Ref[LatencyBuffers], Ref[Long])],
  ): IO[RuntimeException, Unit] =
    Clock.instant.flatMap { now =>
      if now.isAfter(deadline) then ZIO.unit
      else
        performOperation(harness, workload, writeCountRef, metricsRef) *> driveWorkers(
          harness,
          workload,
          deadline,
          writeCountRef,
          metricsRef,
        )
    }

  private def performOperation(
    harness: BenchHarness,
    workload: StressWorkload,
    writeCountRef: Ref[Long],
    metricsRef: Option[(Ref[LatencyBuffers], Ref[Long])],
  ): IO[RuntimeException, Unit] =
    for
      step        <- randomStep(workload)
      startedAt   <- Clock.nanoTime
      _           <- step match
                       case "load"   => harness.loadSize.unit
                       case "modify" => harness.modifyAppendOne *> writeCountRef.update(_ + 1L)
                       case _        => ZIO.unit
      finishedAt  <- Clock.nanoTime
      latencyNanos = finishedAt - startedAt
      _           <- metricsRef match
                       case None                         => ZIO.unit
                       case Some((latencyRef, totalRef)) =>
                         val update =
                           step match
                             case "load"   => latencyRef.update(latencies => latencies.copy(load = latencies.load :+ latencyNanos))
                             case "modify" => latencyRef.update(latencies => latencies.copy(modify = latencies.modify :+ latencyNanos))
                             case _        => ZIO.unit
                         update *> totalRef.update(_ + 1L)
    yield ()

  private def randomStep(workload: StressWorkload): UIO[String] =
    workload match
      case StressWorkload.ReadOnly  => ZIO.succeed("load")
      case StressWorkload.WriteOnly => ZIO.succeed("modify")
      case StressWorkload.Mixed8020 =>
        Random.nextIntBetween(0, 10).map(value => if value < 8 then "load" else "modify")
      case StressWorkload.Mixed5050 =>
        Random.nextBoolean.map(flag => if flag then "load" else "modify")

  private def startCheckpointWorker(
    harness: BenchHarness,
    cadence: CheckpointCadence,
    writeCountRef: Ref[Long],
    checkpointedRef: Ref[Long],
    latencyRef: Ref[LatencyBuffers],
  ): URIO[Scope, Fiber.Runtime[Nothing, Unit]] =
    cadence.writeThreshold match
      case None            => ZIO.unit.forkScoped
      case Some(threshold) =>
        checkpointLoop(harness, threshold, writeCountRef, checkpointedRef, latencyRef).forkScoped

  private def checkpointLoop(
    harness: BenchHarness,
    threshold: Long,
    writeCountRef: Ref[Long],
    checkpointedRef: Ref[Long],
    latencyRef: Ref[LatencyBuffers],
  ): IO[Nothing, Unit] =
    (for
      writes       <- writeCountRef.get
      checkpointed <- checkpointedRef.get
      _            <-
        if writes - checkpointed >= threshold then
          for
            startedAt  <- Clock.nanoTime
            _          <- harness.checkpoint.orDie
            finishedAt <- Clock.nanoTime
            _          <- latencyRef.update(latencies => latencies.copy(checkpoint = latencies.checkpoint :+ (finishedAt - startedAt)))
            _          <- checkpointedRef.set(writes)
          yield ()
        else ZIO.sleep(25.millis)
    yield ()).forever

  def formatText(report: StressReport): String =
    val metrics = report.metrics
    val warnings =
      if report.assessment.warnings.isEmpty then "warnings=none"
      else s"warnings=${report.assessment.warnings.mkString(" | ")}"
    s"""NativeLocal Stress Report
       |machine=${report.machine.machine}
       |jvm=${report.machine.jvmVersion}
       |scala=${report.machine.scalaVersion}
       |sbt=${report.machine.sbtVersion}
       |project=${report.machine.projectVersion}
       |serde=${report.scenario.serde}
       |shape=${report.scenario.payloadShape}
       |size=${report.scenario.elementCount}
       |concurrency=${report.scenario.concurrency}
       |safeScale=${report.assessment.safeScale}
       |recommendedMinHeapMiB=${report.assessment.recommendedMinHeapBytes / (1024L * 1024L)}
       |cadence=${report.scenario.checkpointCadence}
       |workload=${report.scenario.workload}
       |$warnings
       |ops=${metrics.totalOperations}
       |elapsedMillis=${metrics.elapsedMillis}
       |opsPerSecond=${"%.2f".format(metrics.operationsPerSecond)}
       |load.p50=${"%.3f".format(metrics.loadLatency.p50Millis)}ms
       |load.p95=${"%.3f".format(metrics.loadLatency.p95Millis)}ms
       |modify.p50=${"%.3f".format(metrics.modifyLatency.p50Millis)}ms
       |modify.p95=${"%.3f".format(metrics.modifyLatency.p95Millis)}ms
       |checkpoint.p50=${"%.3f".format(metrics.checkpointLatency.p50Millis)}ms
       |checkpoint.p95=${"%.3f".format(metrics.checkpointLatency.p95Millis)}ms
       |restartMillis=${metrics.restartMillis}
       |snapshotBytes=${metrics.snapshotBytes}
       |heap.before=${metrics.heapBeforeBytes}
       |heap.afterWarmup=${metrics.heapAfterWarmupBytes}
       |heap.afterCheckpoint=${metrics.heapAfterCheckpointBytes}
       |heap.afterRestart=${metrics.heapAfterRestartBytes}
       |""".stripMargin

  def formatJson(report: StressReport): String =
    val metrics = report.metrics
    val warningsJson = report.assessment.warnings.map(value => s""""${escapeJson(value)}"""").mkString("[", ", ", "]")
    s"""{
       |  "machine": "${escapeJson(report.machine.machine)}",
       |  "jvmVersion": "${escapeJson(report.machine.jvmVersion)}",
       |  "scalaVersion": "${escapeJson(report.machine.scalaVersion)}",
       |  "sbtVersion": "${escapeJson(report.machine.sbtVersion)}",
       |  "projectVersion": "${escapeJson(report.machine.projectVersion)}",
       |  "serde": "${report.scenario.serde.toString}",
       |  "shape": "${report.scenario.payloadShape.toString}",
       |  "size": ${report.scenario.elementCount},
       |  "concurrency": ${report.scenario.concurrency},
       |  "safeScale": "${escapeJson(report.assessment.safeScale)}",
       |  "recommendedMinHeapMiB": ${report.assessment.recommendedMinHeapBytes / (1024L * 1024L)},
       |  "warnings": $warningsJson,
       |  "checkpointCadence": "${report.scenario.checkpointCadence.toString}",
       |  "workload": "${report.scenario.workload.toString}",
       |  "operations": ${metrics.totalOperations},
       |  "elapsedMillis": ${metrics.elapsedMillis},
       |  "operationsPerSecond": ${"%.4f".format(metrics.operationsPerSecond)},
       |  "restartMillis": ${metrics.restartMillis},
       |  "snapshotBytes": ${metrics.snapshotBytes},
       |  "heapBytes": {
       |    "before": ${metrics.heapBeforeBytes},
       |    "afterWarmup": ${metrics.heapAfterWarmupBytes},
       |    "afterCheckpoint": ${metrics.heapAfterCheckpointBytes},
       |    "afterRestart": ${metrics.heapAfterRestartBytes}
       |  },
       |  "latencyMillis": {
       |    "load": ${latencyJson(metrics.loadLatency)},
       |    "modify": ${latencyJson(metrics.modifyLatency)},
       |    "checkpoint": ${latencyJson(metrics.checkpointLatency)}
       |  }
       |}""".stripMargin

  private def machineMetadata(machine: Option[String]): MachineMetadata =
    MachineMetadata(
      machine = machine.orElse(sys.env.get("BENCH_MACHINE")).getOrElse("unknown"),
      jvmVersion = sys.props.getOrElse("java.runtime.version", sys.props.getOrElse("java.version", "unknown")),
      scalaVersion = util.Properties.versionNumberString,
      sbtVersion = sys.props.get("sbt.version").orElse(sys.env.get("SBT_VERSION")).getOrElse("unknown"),
      projectVersion = sys.props.get("project.version").orElse(sys.env.get("PROJECT_VERSION")).getOrElse("unknown"),
    )

  private def latencyJson(summary: LatencySummary): String =
    s"""{"count": ${summary.count}, "p50": ${"%.4f".format(summary.p50Millis)}, "p95": ${"%.4f".format(summary.p95Millis)}, "p99": ${"%.4f".format(summary.p99Millis)}}"""

  private def escapeJson(value: String): String =
    value.replace("\\", "\\\\").replace("\"", "\\\"")

final case class StartupBenchmarkState(
  freshDir: Path,
  seededDir: Path,
)

object StartupBenchmarkState:
  def make(config: BenchScenarioConfig): IO[RuntimeException, StartupBenchmarkState] =
    for
      dir       <- ZIO.attempt(Files.createTempDirectory("native-local-startup-bench")).mapError(cause => RuntimeException(cause))
      freshDir   = dir.resolve("fresh")
      seededDir  = dir.resolve("seeded")
      _         <- ZIO.attempt(Files.createDirectories(freshDir)).mapError(cause => RuntimeException(cause))
      _         <- ZIO.attempt(Files.createDirectories(seededDir)).mapError(cause => RuntimeException(cause))
      _         <- BenchHarness.seedSnapshot(seededDir, config)
    yield StartupBenchmarkState(freshDir, seededDir)

@org.openjdk.jmh.annotations.State(org.openjdk.jmh.annotations.Scope.Benchmark)
abstract class NativeLocalJmhState:
  val runtime: Runtime[Any] = Runtime.default

  @org.openjdk.jmh.annotations.Param(Array("json", "protobuf"))
  var serdeName: String = uninitialized

  @org.openjdk.jmh.annotations.Param(Array("flat", "nested"))
  var payloadShapeName: String = uninitialized

  @org.openjdk.jmh.annotations.Param(Array("1000", "10000", "100000"))
  var elementCountName: String = uninitialized

  @org.openjdk.jmh.annotations.Param(Array("16"))
  var nestedPayloadSizeName: String = uninitialized

  def serde: NativeLocalSerde =
    if serdeName.equalsIgnoreCase("protobuf") then NativeLocalSerde.Protobuf else NativeLocalSerde.Json

  def payloadShape: BenchPayloadShape =
    BenchPayloadShape.parse(payloadShapeName)

  def elementCount: Int =
    elementCountName.toInt

  def nestedPayloadSize: Int =
    nestedPayloadSizeName.toInt

  def baseScenario(
    concurrency: Int = 1,
    cadence: CheckpointCadence = CheckpointCadence.Never,
  ): BenchScenarioConfig =
    BenchScenarioConfig(
      serde = serde,
      elementCount = elementCount,
      nestedPayloadSize = nestedPayloadSize,
      concurrency = concurrency,
      checkpointCadence = cadence,
      payloadShape = payloadShape,
    )

  protected def deleteDirectory(path: Path): Unit =
    if path != null && Files.exists(path) then
      Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
