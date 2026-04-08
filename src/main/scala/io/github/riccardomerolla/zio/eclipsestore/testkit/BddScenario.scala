package io.github.riccardomerolla.zio.eclipsestore.testkit

import zio.*
import zio.test.*

final case class BddScenario[-R, +E, A](
  title: String,
  givenLabel: String,
  givenStep: ZIO[R, E, Unit],
  whenLabel: String,
  whenStep: ZIO[R, E, A],
  thenLabel: String,
  assertion: A => TestResult,
):
  def spec: Spec[R, E] =
    zio.test.test(s"$title | Given $givenLabel | When $whenLabel | Then $thenLabel") {
      for
        _      <- givenStep
        result <- whenStep
      yield assertion(result)
    }

object BddScenario:
  def scenario(title: String): BddGivenBuilder =
    BddGivenBuilder(title)

  final case class BddGivenBuilder(title: String):
    def `given`[R, E](label: String)(step: ZIO[R, E, Unit]): BddWhenBuilder[R, E] =
      BddWhenBuilder(title, label, step)

  final case class BddWhenBuilder[R, E](
    title: String,
    givenLabel: String,
    givenStep: ZIO[R, E, Unit],
  ):
    def when[A](label: String)(step: ZIO[R, E, A]): BddThenBuilder[R, E, A] =
      BddThenBuilder(title, givenLabel, givenStep, label, step)

  final case class BddThenBuilder[R, E, A](
    title: String,
    givenLabel: String,
    givenStep: ZIO[R, E, Unit],
    whenLabel: String,
    whenStep: ZIO[R, E, A],
  ):
    def thenAssert(label: String)(assertion: A => TestResult): BddScenario[R, E, A] =
      BddScenario(title, givenLabel, givenStep, whenLabel, whenStep, label, assertion)
