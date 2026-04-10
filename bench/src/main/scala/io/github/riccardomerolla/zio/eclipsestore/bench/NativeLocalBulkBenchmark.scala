package io.github.riccardomerolla.zio.eclipsestore.bench

import java.nio.file.Path
import java.util.concurrent.TimeUnit

import scala.compiletime.uninitialized

import org.openjdk.jmh.annotations.{ Level, Scope as JmhScope, * }

import zio.*

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class NativeLocalBulkBenchmark:
  @Benchmark
  def bulkReplace(state: NativeLocalBulkBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.bulkReplaceProgram).getOrThrowFiberFailure()
    }

  @Benchmark
  def appendGrowth(state: NativeLocalBulkBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.appendGrowthProgram).getOrThrowFiberFailure()
    }

  @Benchmark
  def checkpointLargeRoot(state: NativeLocalBulkBenchmark.BenchmarkState): Long =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.checkpointProgram).getOrThrowFiberFailure()
    }

  @Benchmark
  def restoreLargeSnapshot(state: NativeLocalBulkBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.restoreProgram).getOrThrowFiberFailure()
    }

object NativeLocalBulkBenchmark:
  @org.openjdk.jmh.annotations.State(JmhScope.Benchmark)
  class BenchmarkState extends NativeLocalJmhState:
    @Param(Array("100", "1000"))
    var appendBatchName: String = uninitialized

    private var baseDir0: Path = uninitialized

    def appendBatch: Int =
      appendBatchName.toInt

    @Setup(Level.Trial)
    def setup(): Unit =
      Unsafe.unsafe { implicit unsafe =>
        baseDir0 = runtime.unsafe
          .run(ZIO.attempt(java.nio.file.Files.createTempDirectory("native-local-bulk-bench")).mapError(cause => RuntimeException(cause)))
          .getOrThrowFiberFailure()
        runtime.unsafe
          .run(BenchHarness.seedSnapshot(baseDir0, baseScenario()))
          .getOrThrowFiberFailure()
      }

    @TearDown(Level.Trial)
    def tearDown(): Unit =
      deleteDirectory(baseDir0)

    def bulkReplaceProgram: IO[RuntimeException, Int] =
      ZIO.scoped(BenchHarness.scoped(baseDir0, baseScenario())).flatMap(_.replaceAll(elementCount))

    def appendGrowthProgram: IO[RuntimeException, Int] =
      ZIO.scoped(BenchHarness.scoped(baseDir0, baseScenario())).flatMap { harness =>
        ZIO.foldLeft(0 until appendBatch)(0) { (_, _) =>
          harness.modifyAppendOne
        }
      }

    def checkpointProgram: IO[RuntimeException, Long] =
      ZIO.scoped(BenchHarness.scoped(baseDir0, baseScenario())).flatMap(_.checkpoint)

    def restoreProgram: IO[RuntimeException, Int] =
      ZIO.scoped(BenchHarness.scoped(baseDir0, baseScenario())).flatMap { harness =>
        harness.checkpoint *> harness.restartAndCount
      }
