package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.file.Path

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

/** Typed operational interface for lifecycle, backup, restore, and housekeeping flows. */
trait StorageOps[Root]:
  def descriptor: RootDescriptor[Root]
  def load: IO[EclipseStoreError, Root]
  def status: UIO[LifecycleStatus]
  def checkpoint: IO[EclipseStoreError, LifecycleStatus]
  def backup(target: Path, includeConfig: Boolean = true): IO[EclipseStoreError, LifecycleStatus]
  def exportTo(target: Path): IO[EclipseStoreError, LifecycleStatus]
  def importFrom(source: Path): IO[EclipseStoreError, LifecycleStatus]
  def restoreFrom(source: Path): IO[EclipseStoreError, LifecycleStatus]
  def restart: IO[EclipseStoreError, LifecycleStatus]
  def shutdown: IO[EclipseStoreError, LifecycleStatus]
  def housekeep: IO[EclipseStoreError, LifecycleStatus]
  def scheduleCheckpoints(schedule: Schedule[Any, Any, Any]): URIO[Scope, Fiber.Runtime[Nothing, Unit]]

object StorageOps:
  def live[Root: Tag](descriptor0: RootDescriptor[Root]): ZLayer[EclipseStoreService, Nothing, StorageOps[Root]] =
    ZLayer.fromZIO {
      ZIO.service[EclipseStoreService].map(StorageOpsLive(descriptor0, _))
    }

  def load[Root: Tag]: ZIO[StorageOps[Root], EclipseStoreError, Root] =
    ZIO.serviceWithZIO[StorageOps[Root]](_.load)

  val status: ZIO[StorageOps[?], Nothing, LifecycleStatus] =
    ZIO.serviceWithZIO[StorageOps[?]](_.status)

  def checkpoint[Root: Tag]: ZIO[StorageOps[Root], EclipseStoreError, LifecycleStatus] =
    ZIO.serviceWithZIO[StorageOps[Root]](_.checkpoint)

  def backup[Root: Tag](target: Path, includeConfig: Boolean = true)
    : ZIO[StorageOps[Root], EclipseStoreError, LifecycleStatus] =
    ZIO.serviceWithZIO[StorageOps[Root]](_.backup(target, includeConfig))

  def exportTo[Root: Tag](target: Path): ZIO[StorageOps[Root], EclipseStoreError, LifecycleStatus] =
    ZIO.serviceWithZIO[StorageOps[Root]](_.exportTo(target))

  def importFrom[Root: Tag](source: Path): ZIO[StorageOps[Root], EclipseStoreError, LifecycleStatus] =
    ZIO.serviceWithZIO[StorageOps[Root]](_.importFrom(source))

  def restoreFrom[Root: Tag](source: Path): ZIO[StorageOps[Root], EclipseStoreError, LifecycleStatus] =
    ZIO.serviceWithZIO[StorageOps[Root]](_.restoreFrom(source))

  def restart[Root: Tag]: ZIO[StorageOps[Root], EclipseStoreError, LifecycleStatus] =
    ZIO.serviceWithZIO[StorageOps[Root]](_.restart)

  def shutdown[Root: Tag]: ZIO[StorageOps[Root], EclipseStoreError, LifecycleStatus] =
    ZIO.serviceWithZIO[StorageOps[Root]](_.shutdown)

  def housekeep[Root: Tag]: ZIO[StorageOps[Root], EclipseStoreError, LifecycleStatus] =
    ZIO.serviceWithZIO[StorageOps[Root]](_.housekeep)

  def scheduleCheckpoints[Root: Tag](
    schedule: Schedule[Any, Any, Any]
  ): ZIO[StorageOps[Root] & Scope, Nothing, Fiber.Runtime[Nothing, Unit]] =
    ZIO.serviceWithZIO[StorageOps[Root]](_.scheduleCheckpoints(schedule))

final case class StorageOpsLive[Root](
  descriptor: RootDescriptor[Root],
  underlying: EclipseStoreService,
) extends StorageOps[Root]:
  override def load: IO[EclipseStoreError, Root] =
    underlying.root(descriptor)

  override val status: UIO[LifecycleStatus] =
    underlying.status

  override def checkpoint: IO[EclipseStoreError, LifecycleStatus] =
    underlying.maintenance(LifecycleCommand.Checkpoint)

  override def backup(target: Path, includeConfig: Boolean = true): IO[EclipseStoreError, LifecycleStatus] =
    underlying.maintenance(LifecycleCommand.Backup(target, includeConfig))

  override def exportTo(target: Path): IO[EclipseStoreError, LifecycleStatus] =
    underlying.exportData(target) *> status

  override def importFrom(source: Path): IO[EclipseStoreError, LifecycleStatus] =
    underlying.importData(source) *> underlying.reloadRoots *> status

  override def restoreFrom(source: Path): IO[EclipseStoreError, LifecycleStatus] =
    importFrom(source)

  override def restart: IO[EclipseStoreError, LifecycleStatus] =
    underlying.maintenance(LifecycleCommand.Restart)

  override def shutdown: IO[EclipseStoreError, LifecycleStatus] =
    underlying.maintenance(LifecycleCommand.Shutdown)

  override def housekeep: IO[EclipseStoreError, LifecycleStatus] =
    checkpoint *> underlying.reloadRoots *> status

  override def scheduleCheckpoints(schedule: Schedule[Any, Any, Any]): URIO[Scope, Fiber.Runtime[Nothing, Unit]] =
    checkpoint
      .ignore
      .repeat(schedule)
      .unit
      .forkScoped
