package io.github.riccardomerolla.zio.eclipsestore.bench

import java.nio.file.Path
import java.util.concurrent.TimeUnit

import scala.compiletime.uninitialized

import org.openjdk.jmh.annotations.{ Level, Scope as JmhScope, * }

import zio.*

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class NativeLocalSerdeBenchmark:
  @Benchmark
  def checkpointBySerde(state: NativeLocalSerdeBenchmark.BenchmarkState): Long =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.checkpointProgram).getOrThrowFiberFailure()
    }

  @Benchmark
  def restartBySerde(state: NativeLocalSerdeBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.restartProgram).getOrThrowFiberFailure()
    }

object NativeLocalSerdeBenchmark:
  @org.openjdk.jmh.annotations.State(JmhScope.Benchmark)
  class BenchmarkState extends NativeLocalJmhState:
    private var baseDir0: Path = uninitialized

    @Setup(Level.Trial)
    def setup(): Unit =
      Unsafe.unsafe { implicit unsafe =>
        baseDir0 = runtime.unsafe
          .run(ZIO.attempt(java.nio.file.Files.createTempDirectory("native-local-serde-bench")).mapError(cause => RuntimeException(cause)))
          .getOrThrowFiberFailure()
        runtime.unsafe
          .run(BenchHarness.seedSnapshot(baseDir0, baseScenario()))
          .getOrThrowFiberFailure()
      }

    @TearDown(Level.Trial)
    def tearDown(): Unit =
      deleteDirectory(baseDir0)

    def checkpointProgram: IO[RuntimeException, Long] =
      ZIO.scoped(BenchHarness.scoped(baseDir0, baseScenario())).flatMap(_.checkpoint)

    def restartProgram: IO[RuntimeException, Int] =
      ZIO.scoped(BenchHarness.scoped(baseDir0, baseScenario())).flatMap(_.restartAndCount)
