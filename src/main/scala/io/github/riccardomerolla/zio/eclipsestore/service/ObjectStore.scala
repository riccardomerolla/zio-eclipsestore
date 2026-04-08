package io.github.riccardomerolla.zio.eclipsestore.service

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

/** Typed access to a single rooted object graph.
  *
  * This service wraps the lower-level `EclipseStoreService` with a root-focused API so applications can work against a
  * typed root instead of treating EclipseStore as a key-value store.
  */
trait ObjectStore[Root]:
  def descriptor: RootDescriptor[Root]
  def load: IO[EclipseStoreError, Root]
  def storeSubgraph(subgraph: AnyRef): IO[EclipseStoreError, Unit]
  def storeRoot: IO[EclipseStoreError, Unit]
  def checkpoint: IO[EclipseStoreError, Unit]
  def transact[A](transaction: Transaction[Root, A]): IO[EclipseStoreError, A]

final case class Transaction[-Root, +A](run: Root => IO[EclipseStoreError, A]):
  def map[B](f: A => B): Transaction[Root, B] =
    Transaction(root => run(root).map(f))

  def flatMap[Root1 <: Root, B](f: A => Transaction[Root1, B]): Transaction[Root1, B] =
    Transaction(root => run(root).flatMap(a => f(a).run(root)))

object Transaction:
  def succeed[Root, A](value: => A): Transaction[Root, A] =
    Transaction(_ => ZIO.succeed(value))

  def fail[Root, A](error: EclipseStoreError): Transaction[Root, A] =
    Transaction(_ => ZIO.fail(error))

  def effect[Root, A](f: Root => IO[EclipseStoreError, A]): Transaction[Root, A] =
    Transaction(f)

object ObjectStore:
  def live[Root: Tag](descriptor0: RootDescriptor[Root]): ZLayer[EclipseStoreService, Nothing, ObjectStore[Root]] =
    ZLayer.fromZIO {
      for
        service <- ZIO.service[EclipseStoreService]
        txSem   <- Semaphore.make(1)
      yield ObjectStoreLive(descriptor0, service, txSem)
    }

  def load[Root: Tag]: ZIO[ObjectStore[Root], EclipseStoreError, Root] =
    ZIO.serviceWithZIO[ObjectStore[Root]](_.load)

  def storeSubgraph[Root: Tag](subgraph: AnyRef): ZIO[ObjectStore[Root], EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[ObjectStore[Root]](_.storeSubgraph(subgraph))

  def storeRoot[Root: Tag]: ZIO[ObjectStore[Root], EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[ObjectStore[Root]](_.storeRoot)

  def checkpoint[Root: Tag]: ZIO[ObjectStore[Root], EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[ObjectStore[Root]](_.checkpoint)

  def transact[Root: Tag, A](transaction: Transaction[Root, A]): ZIO[ObjectStore[Root], EclipseStoreError, A] =
    ZIO.serviceWithZIO[ObjectStore[Root]](_.transact(transaction))

final case class ObjectStoreLive[Root](
  descriptor: RootDescriptor[Root],
  underlying: EclipseStoreService,
  txSemaphore: Semaphore,
) extends ObjectStore[Root]:
  override def load: IO[EclipseStoreError, Root] =
    underlying.root(descriptor)

  override def storeSubgraph(subgraph: AnyRef): IO[EclipseStoreError, Unit] =
    underlying.persist(subgraph) *> checkpoint

  override def storeRoot: IO[EclipseStoreError, Unit] =
    checkpoint

  override def checkpoint: IO[EclipseStoreError, Unit] =
    underlying.maintenance(LifecycleCommand.Checkpoint).unit

  override def transact[A](transaction: Transaction[Root, A]): IO[EclipseStoreError, A] =
    txSemaphore.withPermit {
      for
        root   <- load
        result <- transaction.run(root)
        _      <- checkpoint
      yield result
    }
