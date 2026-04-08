package io.github.riccardomerolla.zio.eclipsestore.testkit

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

trait FaultInjector:
  def inject[A](operation: String)(effect: IO[EclipseStoreError, A]): IO[EclipseStoreError, A]
  def failNext(
    operation: String,
    error: EclipseStoreError,
    cleanup: UIO[Unit] = ZIO.unit,
  ): UIO[Unit]
  def triggeredOperations: UIO[Chunk[String]]

object FaultInjector:
  def inMemory: ULayer[FaultInjector] =
    ZLayer.fromZIO {
      for
        plans     <- Ref.make(Map.empty[String, Chunk[FaultPlan]])
        triggered <- Ref.make(Chunk.empty[String])
      yield InMemoryFaultInjector(plans, triggered)
    }

  def inject[A](operation: String)(effect: IO[EclipseStoreError, A]): ZIO[FaultInjector, EclipseStoreError, A] =
    ZIO.serviceWithZIO[FaultInjector](_.inject(operation)(effect))

  def failNext(
    operation: String,
    error: EclipseStoreError,
    cleanup: UIO[Unit] = ZIO.unit,
  ): ZIO[FaultInjector, Nothing, Unit] =
    ZIO.serviceWithZIO[FaultInjector](_.failNext(operation, error, cleanup))

  val triggeredOperations: ZIO[FaultInjector, Nothing, Chunk[String]] =
    ZIO.serviceWithZIO[FaultInjector](_.triggeredOperations)

private final case class FaultPlan(
  error: EclipseStoreError,
  cleanup: UIO[Unit],
)

private final case class InMemoryFaultInjector(
  plans: Ref[Map[String, Chunk[FaultPlan]]],
  triggered: Ref[Chunk[String]],
) extends FaultInjector:
  override def inject[A](operation: String)(effect: IO[EclipseStoreError, A]): IO[EclipseStoreError, A] =
    for
      maybePlan <- plans.modify { current =>
                     current.get(operation) match
                       case Some(entries) if entries.nonEmpty =>
                         val remaining = entries.tail
                         val updated   =
                           if remaining.isEmpty then current - operation
                           else current.updated(operation, remaining)
                         (Some(entries.head), updated)
                       case _                                 =>
                         (None, current)
                   }
      result    <- maybePlan match
                     case Some(plan) =>
                       triggered.update(_ :+ operation) *>
                         plan.cleanup *>
                         ZIO.fail(plan.error)
                     case None       =>
                       effect
    yield result

  override def failNext(
    operation: String,
    error: EclipseStoreError,
    cleanup: UIO[Unit] = ZIO.unit,
  ): UIO[Unit] =
    plans.update(current =>
      current.updated(operation, current.getOrElse(operation, Chunk.empty) :+ FaultPlan(error, cleanup))
    )

  override def triggeredOperations: UIO[Chunk[String]] =
    triggered.get

