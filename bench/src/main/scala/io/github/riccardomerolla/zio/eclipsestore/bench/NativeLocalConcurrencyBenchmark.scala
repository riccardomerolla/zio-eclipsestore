package io.github.riccardomerolla.zio.eclipsestore.bench

import java.nio.file.Path
import java.util.concurrent.TimeUnit

import scala.compiletime.uninitialized

import org.openjdk.jmh.annotations.{ Level, Scope as JmhScope, * }

import zio.*

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class NativeLocalConcurrencyBenchmark:
  @Benchmark
  def parallelLoad(state: NativeLocalConcurrencyBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.parallelLoadProgram).getOrThrowFiberFailure()
    }

  @Benchmark
  def parallelModify(state: NativeLocalConcurrencyBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.parallelModifyProgram).getOrThrowFiberFailure()
    }

  @Benchmark
  def mixedReadWrite(state: NativeLocalConcurrencyBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.mixedReadWriteProgram).getOrThrowFiberFailure()
    }

  @Benchmark
  def modifyWhileCheckpointing(state: NativeLocalConcurrencyBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.modifyWhileCheckpointingProgram).getOrThrowFiberFailure()
    }

object NativeLocalConcurrencyBenchmark:
  @org.openjdk.jmh.annotations.State(JmhScope.Benchmark)
  class BenchmarkState extends NativeLocalJmhState:
    @Param(Array("1", "4", "8", "16"))
    var concurrencyName: String = uninitialized

    @Param(Array("80", "50"))
    var readPercentName: String = uninitialized

    private var baseDir0: Path = uninitialized

    def concurrency: Int =
      concurrencyName.toInt

    def readPercent: Int =
      readPercentName.toInt

    @Setup(Level.Trial)
    def setup(): Unit =
      Unsafe.unsafe { implicit unsafe =>
        baseDir0 = runtime.unsafe
          .run(ZIO.attempt(java.nio.file.Files.createTempDirectory("native-local-concurrency-bench")).mapError(cause => RuntimeException(cause)))
          .getOrThrowFiberFailure()
        runtime.unsafe
          .run(BenchHarness.seedSnapshot(baseDir0, baseScenario(concurrency)))
          .getOrThrowFiberFailure()
      }

    @TearDown(Level.Trial)
    def tearDown(): Unit =
      deleteDirectory(baseDir0)

    def parallelLoadProgram: IO[RuntimeException, Int] =
      scopedHarness(baseScenario(concurrency)).flatMap { harness =>
        ZIO.foreachPar(0 until concurrency)(_ => harness.loadSize).map(_.sum)
      }

    def parallelModifyProgram: IO[RuntimeException, Int] =
      scopedHarness(baseScenario(concurrency)).flatMap { harness =>
        ZIO.foreachPar(0 until concurrency)(_ => harness.modifyAppendOne).map(_.sum)
      }

    def mixedReadWriteProgram: IO[RuntimeException, Int] =
      scopedHarness(baseScenario(concurrency)).flatMap { harness =>
        val writers = math.max(1, math.round(concurrency.toDouble * (100 - readPercent).toDouble / 100.0).toInt)
        val readers = math.max(0, concurrency - writers)
        for
          readSizes   <- ZIO.foreachPar(0 until readers)(_ => harness.loadSize)
          writeSizes  <- ZIO.foreachPar(0 until writers)(_ => harness.modifyAppendOne)
        yield readSizes.sum + writeSizes.sum
      }

    def modifyWhileCheckpointingProgram: IO[RuntimeException, Int] =
      scopedHarness(baseScenario(concurrency, CheckpointCadence.Every100)).flatMap { harness =>
        ZIO.scoped {
          for
            checkpointFiber <- (ZIO.sleep(25.millis) *> harness.checkpoint.unit).forever.forkScoped
            sizes           <- ZIO.foreachPar(0 until concurrency)(_ => harness.modifyAppendOne)
            _               <- checkpointFiber.interrupt
          yield sizes.sum
        }
      }

    private def scopedHarness(scenario: BenchScenarioConfig): ZIO[Any, RuntimeException, BenchHarness] =
      ZIO.scoped(BenchHarness.scoped(baseDir0, scenario))
