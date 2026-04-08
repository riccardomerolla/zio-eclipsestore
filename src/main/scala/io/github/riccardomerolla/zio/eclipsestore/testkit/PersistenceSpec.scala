package io.github.riccardomerolla.zio.eclipsestore.testkit

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

object PersistenceSpec:
  def parity[A](
    title: String
  )(
    live: => IO[EclipseStoreError, A],
    inMemory: => IO[EclipseStoreError, A],
  )(
    assertion: (A, A) => TestResult
  ): Spec[TestEnvironment & Scope, Any] =
    zio.test.test(title) {
      for
        liveResult     <- live
        inMemoryResult <- inMemory
      yield assertion(liveResult, inMemoryResult)
    }
