package io.github.riccardomerolla.zio.eclipsestore.bench

import zio.test.*

object NativeLocalBenchmarkSpec extends ZIOSpecDefault:

  override def spec: Spec[TestEnvironment, Any] =
    suite("NativeLocalBenchmark")(
      test("startup benchmarks load the expected number of values") {
        val benchmark = NativeLocalBenchmark()
        val state     = NativeLocalBenchmark.StartupState()

        try
          state.setup()

          val fresh  = benchmark.startupFresh(state)
          val seeded = benchmark.startupFromSnapshot(state)

          assertTrue(
            fresh == 0,
            seeded == 1024,
          )
        finally state.tearDown()
      },
      test("warm benchmarks mutate state and write snapshots") {
        val benchmark = NativeLocalBenchmark()
        val state     = NativeLocalBenchmark.WarmState()

        try
          state.setup()

          val modified       = benchmark.modifyWarm(state)
          val checkpointSize = benchmark.checkpointWarm(state)

          assertTrue(
            modified >= 1025,
            checkpointSize > 0,
          )
        finally state.tearDown()
      },
      test("restore benchmark reloads imported snapshot contents") {
        val benchmark = NativeLocalBenchmark()
        val state     = NativeLocalBenchmark.RestoreState()

        try
          state.setup()

          val restored = benchmark.restoreFromSnapshot(state)

          assertTrue(restored == 2048)
        finally state.tearDown()
      },
    )
