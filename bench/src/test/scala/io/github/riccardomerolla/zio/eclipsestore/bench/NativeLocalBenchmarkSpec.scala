package io.github.riccardomerolla.zio.eclipsestore.bench

import java.nio.file.Files

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde

object NativeLocalBenchmarkSpec extends ZIOSpecDefault:

  private def withTempDirectory[A](prefix: String)(use: java.nio.file.Path => ZIO[Any, Throwable, A]): ZIO[Any, Throwable, A] =
    ZIO.scoped {
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory(prefix))) { path =>
        ZIO.attemptBlocking {
          if Files.exists(path) then
            Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
        }.ignore
      }.flatMap(use)
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("NativeLocalBenchmark")(
      test("startup benchmarks load the expected number of seeded values") {
        val benchmark = NativeLocalStartupBenchmark()

        withTempDirectory("native-local-startup-spec") { _ =>
          ZIO.attempt {
            val state = NativeLocalStartupBenchmark.BenchmarkState()
            try
              state.serdeName = "json"
              state.payloadShapeName = "flat"
              state.elementCountName = "1000"
              state.nestedPayloadSizeName = "16"
              state.setup()

              val fresh    = benchmark.startupFresh(state)
              val seeded   = benchmark.startupFromSnapshot(state)
              val restarted = benchmark.restartAfterCheckpoint(state)

              assertTrue(
                fresh == 0,
                seeded == 1000,
                restarted == 1000,
              )
            finally state.tearDown()
          }
        }
      },
      test("concurrency workload preserves root invariants under parallel modify and checkpoint traffic") {
        val benchmark = NativeLocalConcurrencyBenchmark()

        ZIO.attempt {
          val state = NativeLocalConcurrencyBenchmark.BenchmarkState()
          try
            state.serdeName = "json"
            state.payloadShapeName = "flat"
            state.elementCountName = "1000"
            state.nestedPayloadSizeName = "16"
            state.concurrencyName = "4"
            state.readPercentName = "50"
            state.setup()

            val parallelLoads = benchmark.parallelLoad(state)
            val parallelWrites = benchmark.parallelModify(state)
            val mixed          = benchmark.mixedReadWrite(state)
            val withCheckpoint = benchmark.modifyWhileCheckpointing(state)

            assertTrue(
              parallelLoads >= 4000,
              parallelWrites >= 4000,
              mixed >= 2000,
              withCheckpoint >= 4000,
            )
          finally state.tearDown()
        }
      },
      test("bulk and serde fixtures restart successfully for JSON and protobuf roots") {
        val bulkBenchmark  = NativeLocalBulkBenchmark()
        val serdeBenchmark = NativeLocalSerdeBenchmark()

        ZIO.attempt {
          val bulkState  = NativeLocalBulkBenchmark.BenchmarkState()
          val serdeState = NativeLocalSerdeBenchmark.BenchmarkState()
          try
            bulkState.serdeName = "protobuf"
            bulkState.payloadShapeName = "nested"
            bulkState.elementCountName = "1000"
            bulkState.nestedPayloadSizeName = "16"
            bulkState.appendBatchName = "100"
            bulkState.setup()

            serdeState.serdeName = "json"
            serdeState.payloadShapeName = "nested"
            serdeState.elementCountName = "1000"
            serdeState.nestedPayloadSizeName = "16"
            serdeState.setup()

            val replaced      = bulkBenchmark.bulkReplace(bulkState)
            val grown         = bulkBenchmark.appendGrowth(bulkState)
            val restored      = bulkBenchmark.restoreLargeSnapshot(bulkState)
            val checkpointed  = serdeBenchmark.checkpointBySerde(serdeState)
            val restartedSize = serdeBenchmark.restartBySerde(serdeState)

            assertTrue(
              replaced == 1000,
              grown >= 1000,
              restored >= 1000,
              checkpointed > 0L,
              restartedSize == 1000,
            )
          finally
            bulkState.tearDown()
            serdeState.tearDown()
        }
      },
      test("stress runner reports metrics for mixed concurrency and restart correctness") {
        withTempDirectory("native-local-stress-spec") { dir =>
          val scenario =
            BenchScenarioConfig(
              serde = NativeLocalSerde.Protobuf,
              elementCount = 1000,
              nestedPayloadSize = 12,
              concurrency = 4,
              checkpointCadence = CheckpointCadence.Every100,
              payloadShape = BenchPayloadShape.Nested,
              warmupSeconds = 1,
              measurementSeconds = 1,
              workload = StressWorkload.Mixed5050,
              machine = Some("spec-machine"),
            )

          Live.live(ZIO.scoped(NativeLocalStressRunner.run(scenario, dir))).map { report =>
            val json = NativeLocalStressRunner.formatJson(report)
            val text = NativeLocalStressRunner.formatText(report)

            assertTrue(
              report.metrics.totalOperations > 0L,
              report.metrics.snapshotBytes > 0L,
              report.metrics.restartMillis >= 0L,
              report.metrics.heapAfterRestartBytes > 0L,
              json.contains("operationsPerSecond"),
              text.contains("NativeLocal Stress Report"),
            )
          }
        }
      },
    )
