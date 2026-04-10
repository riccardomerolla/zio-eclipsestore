package io.github.riccardomerolla.zio.eclipsestore.bench

import java.util.concurrent.TimeUnit

import scala.compiletime.uninitialized

import org.openjdk.jmh.annotations.{ Level, Scope as JmhScope, * }

import zio.*

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class NativeLocalStartupBenchmark:
  @Benchmark
  def startupFresh(state: NativeLocalStartupBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.runFresh()).getOrThrowFiberFailure()
    }

  @Benchmark
  def startupFromSnapshot(state: NativeLocalStartupBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.runSeeded()).getOrThrowFiberFailure()
    }

  @Benchmark
  def restartAfterCheckpoint(state: NativeLocalStartupBenchmark.BenchmarkState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe.run(state.runRestartAfterCheckpoint()).getOrThrowFiberFailure()
    }

object NativeLocalStartupBenchmark:
  @org.openjdk.jmh.annotations.State(JmhScope.Benchmark)
  class BenchmarkState extends NativeLocalJmhState:
    private var startupState0: StartupBenchmarkState = uninitialized

    @Setup(Level.Trial)
    def setup(): Unit =
      Unsafe.unsafe { implicit unsafe =>
        startupState0 = runtime.unsafe.run(StartupBenchmarkState.make(baseScenario())).getOrThrowFiberFailure()
      }

    @TearDown(Level.Trial)
    def tearDown(): Unit =
      deleteDirectory(startupState0.freshDir.getParent)

    def runFresh(): IO[RuntimeException, Int] =
      ZIO.scoped {
        BenchHarness.scoped(startupState0.freshDir, baseScenario()).flatMap(_.loadSize)
      }

    def runSeeded(): IO[RuntimeException, Int] =
      ZIO.scoped {
        BenchHarness.scoped(startupState0.seededDir, baseScenario()).flatMap(_.loadSize)
      }

    def runRestartAfterCheckpoint(): IO[RuntimeException, Int] =
      ZIO.scoped {
        for
          harness <- BenchHarness.scoped(startupState0.seededDir, baseScenario())
          _       <- harness.checkpoint
          size    <- harness.restartAndCount
        yield size
      }
