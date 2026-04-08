package io.github.riccardomerolla.zio.eclipsestore.testkit

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

object PersistenceSpec:
  def compare[A, B](
    live: => IO[EclipseStoreError, A],
    baseline: => IO[EclipseStoreError, B],
  )(
    assertion: (A, B) => TestResult
  ): IO[EclipseStoreError, TestResult] =
    for
      liveResult     <- live
      baselineResult <- baseline
    yield assertion(liveResult, baselineResult)

  def parity[A](
    title: String
  )(
    live: => IO[EclipseStoreError, A],
    inMemory: => IO[EclipseStoreError, A],
  )(
    assertion: (A, A) => TestResult
  ): Spec[TestEnvironment & Scope, Any] =
    zio.test.test(title) {
      compare(live, inMemory)(assertion)
    }

  def modelParity[A, B](
    title: String
  )(
    live: => IO[EclipseStoreError, A],
    model: => UIO[B],
  )(
    assertion: (A, B) => TestResult
  ): Spec[TestEnvironment & Scope, Any] =
    zio.test.test(title) {
      compare(live, model)(assertion)
    }
