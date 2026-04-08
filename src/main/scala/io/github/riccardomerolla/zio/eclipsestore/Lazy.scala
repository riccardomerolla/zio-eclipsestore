package io.github.riccardomerolla.zio.eclipsestore

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

/** Scala-native lazy reference with effectful, memoized loading semantics. */
final class Lazy[A] private (
  private val serializedSnapshot: Option[A],
  private val serializedUnavailableMessage: Option[String],
  @transient private val loader: Option[() => IO[EclipseStoreError, A]],
) extends Serializable:

  @transient private lazy val stateRef: Ref.Synchronized[Lazy.State[A]] =
    Lazy.makeStateRef(
      serializedUnavailableMessage match
        case Some(message) => Lazy.State.Unavailable(message)
        case None          =>
          serializedSnapshot match
            case Some(value) => Lazy.State.Snapshot(value)
            case None        =>
              loader match
                case Some(activeLoader) => Lazy.State.Unloaded(activeLoader)
                case None               => Lazy.State.Unavailable("Lazy value was deserialized without an active store scope")
    )

  def load: IO[EclipseStoreError, A] =
    ZIO.uninterruptibleMask { restore =>
      Promise.make[EclipseStoreError, A].flatMap { promise =>
        stateRef.modifyZIO {
          case Lazy.State.Loaded(value)                  =>
            ZIO.succeed((ZIO.succeed(value), Lazy.State.Loaded(value)))
          case Lazy.State.Snapshot(value)                =>
            ZIO.succeed((ZIO.succeed(value), Lazy.State.Loaded(value)))
          case current @ Lazy.State.Loading(existing, _) =>
            ZIO.succeed((restore(existing.await), current))
          case current @ Lazy.State.Unavailable(message) =>
            ZIO.succeed((ZIO.fail(EclipseStoreError.StoreNotOpen(message, None)), current))
          case Lazy.State.Unloaded(activeLoader)         =>
            val start = restore(activeLoader())
              .exit
              .flatMap {
                case Exit.Success(value) =>
                  stateRef.set(Lazy.State.Loaded(value)) *> promise.succeed(value).unit
                case Exit.Failure(cause) =>
                  val error = cause.failureOption.getOrElse(
                    EclipseStoreError.ResourceError("Lazy load interrupted before completion", None)
                  )
                  stateRef.set(Lazy.State.Unloaded(activeLoader)) *> promise.fail(error).unit
              }
              .forkDaemon
              .unit
            ZIO.succeed((start *> restore(promise.await), Lazy.State.Loading(promise, activeLoader)))
        }.flatten
      }
    }

  val isLoaded: UIO[Boolean] =
    stateRef.get.map {
      case Lazy.State.Loaded(_) => true
      case _                    => false
    }

  val isUnloaded: UIO[Boolean] =
    isLoaded.map(!_)

  def map[B](f: A => B): Lazy[B] =
    unsafeState match
      case Lazy.State.Loaded(value)      => Lazy.fromSnapshot(f(value))
      case Lazy.State.Snapshot(value)    => Lazy.fromSnapshot(f(value))
      case Lazy.State.Unavailable(error) => Lazy.unavailable(error)
      case _                             => Lazy.fromZIO(load.map(f))

  def flatMap[B](f: A => Lazy[B]): Lazy[B] =
    unsafeState match
      case Lazy.State.Loaded(value)      => f(value)
      case Lazy.State.Snapshot(value)    => f(value)
      case Lazy.State.Unavailable(error) => Lazy.unavailable(error)
      case _                             => Lazy.fromZIO(load.flatMap(a => f(a).load))

  def reset: UIO[Unit] =
    stateRef.update {
      case Lazy.State.Loaded(value) => Lazy.State.Snapshot(value)
      case other                    => other
    }

  private def unsafeState: Lazy.State[A] =
    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(stateRef.get).getOrThrowFiberFailure()
    }

object Lazy:
  def fromZIO[A](load: => IO[EclipseStoreError, A]): Lazy[A] =
    new Lazy[A](None, None, Some(() => load))

  def fromSnapshot[A](value: A): Lazy[A] =
    new Lazy[A](Some(value), None, None)

  def succeed[A](value: => A): Lazy[A] =
    fromSnapshot(value)

  def unavailable[A](message: String): Lazy[A] =
    new Lazy[A](None, Some(message), None)

  private def makeStateRef[A](state: State[A]): Ref.Synchronized[State[A]] =
    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(Ref.Synchronized.make(state)).getOrThrowFiberFailure()
    }

  sealed private trait State[A]

  private object State:
    final case class Unloaded[A](loader: () => IO[EclipseStoreError, A]) extends State[A]
    final case class Loading[A](promise: Promise[EclipseStoreError, A], loader: () => IO[EclipseStoreError, A])
      extends State[A]
    final case class Snapshot[A](value: A)                               extends State[A]
    final case class Loaded[A](value: A)                                 extends State[A]
    final case class Unavailable[A](message: String)                     extends State[A]
