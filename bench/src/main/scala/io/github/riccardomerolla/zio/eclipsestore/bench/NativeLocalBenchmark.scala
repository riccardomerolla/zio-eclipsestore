package io.github.riccardomerolla.zio.eclipsestore.bench

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

/**
 * Compatibility wrapper for older JMH metadata that still references `NativeLocalBenchmark`.
 * The new suite is split across specialized benchmark classes.
 */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class NativeLocalBenchmark:
  @Benchmark
  def startupFresh(state: NativeLocalStartupBenchmark.BenchmarkState): Int =
    NativeLocalStartupBenchmark().startupFresh(state)

  @Benchmark
  def startupFromSnapshot(state: NativeLocalStartupBenchmark.BenchmarkState): Int =
    NativeLocalStartupBenchmark().startupFromSnapshot(state)

  @Benchmark
  def modifyWarm(state: NativeLocalBulkBenchmark.BenchmarkState): Int =
    NativeLocalBulkBenchmark().appendGrowth(state)

  @Benchmark
  def checkpointWarm(state: NativeLocalSerdeBenchmark.BenchmarkState): Long =
    NativeLocalSerdeBenchmark().checkpointBySerde(state)

  @Benchmark
  def restoreFromSnapshot(state: NativeLocalBulkBenchmark.BenchmarkState): Int =
    NativeLocalBulkBenchmark().restoreLargeSnapshot(state)

object NativeLocalBenchmark:
  class StartupState extends NativeLocalStartupBenchmark.BenchmarkState
  class WarmState extends NativeLocalBulkBenchmark.BenchmarkState
  class RestoreState extends NativeLocalBulkBenchmark.BenchmarkState
