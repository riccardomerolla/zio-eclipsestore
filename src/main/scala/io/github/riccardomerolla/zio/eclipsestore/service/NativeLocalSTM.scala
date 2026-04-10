package io.github.riccardomerolla.zio.eclipsestore.service

import zio.*
import zio.stm.{ STM, TRef }

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

/** Optional STM adapter for NativeLocal roots.
  *
  * This is additive. The primary backend contracts remain `ObjectStore` and `StorageOps`.
  */
trait NativeLocalSTM[Root]:
  def atomically[A](effect: TRef[Root] => STM[EclipseStoreError, A]): IO[EclipseStoreError, A]
  def snapshot: UIO[Root]

object NativeLocalSTM:
  def atomically[Root: Tag, A](
    effect: TRef[Root] => STM[EclipseStoreError, A]
  ): ZIO[NativeLocalSTM[Root], EclipseStoreError, A] =
    ZIO.serviceWithZIO[NativeLocalSTM[Root]](_.atomically(effect))

  def snapshot[Root: Tag]: URIO[NativeLocalSTM[Root], Root] =
    ZIO.serviceWithZIO[NativeLocalSTM[Root]](_.snapshot)
