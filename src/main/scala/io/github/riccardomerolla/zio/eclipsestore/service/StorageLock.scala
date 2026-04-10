package io.github.riccardomerolla.zio.eclipsestore.service

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import java.lang.reflect.Modifier
import java.util.concurrent.ConcurrentHashMap

import zio.*
import zio.stm.*

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

/** ZIO-native locking surface for rooted storage access. */
trait StorageLock[Root]:
  def readLock[A](effect: Root => IO[EclipseStoreError, A]): IO[EclipseStoreError, A]
  def writeLock[A](effect: Root => IO[EclipseStoreError, A]): IO[EclipseStoreError, A]
  def optimisticUpdate[A](
    prepare: Root => IO[EclipseStoreError, A]
  )(
    commit: (Root, A) => IO[EclipseStoreError, A],
    maxRetries: Int = 8,
  ): IO[EclipseStoreError, A]

final case class StorageLockLive[Root](
  store: ObjectStore[Root],
  state: TRef[StorageLockLive.State],
) extends StorageLock[Root]:
  override def readLock[A](effect: Root => IO[EclipseStoreError, A]): IO[EclipseStoreError, A] =
    ZIO.uninterruptibleMask { restore =>
      restore(acquireRead.commit) *> restore(store.load.flatMap(effect)).ensuring(releaseRead.commit)
    }

  override def writeLock[A](effect: Root => IO[EclipseStoreError, A]): IO[EclipseStoreError, A] =
    ZIO.uninterruptibleMask { restore =>
      restore(acquireWrite.commit) *>
        restore {
          for
            root     <- store.load
            snapshot <- RootSnapshot.capture(root)
            exit     <- effect(root).exit
            result   <- exit match
                          case Exit.Success(value) =>
                            store.checkpoint *> completeWrite(committed = true).commit.as(value)
                          case Exit.Failure(cause) =>
                            RootSnapshot.restore(root, snapshot) *>
                              store.checkpoint.ignore *>
                              completeWrite(committed = false).commit *>
                              ZIO.failCause(cause)
          yield result
        }.ensuring(completeWrite(committed = false).commit)
    }

  override def optimisticUpdate[A](
    prepare: Root => IO[EclipseStoreError, A]
  )(
    commit: (Root, A) => IO[EclipseStoreError, A],
    maxRetries: Int = 8,
  ): IO[EclipseStoreError, A] =
    def loop(remaining: Int): IO[EclipseStoreError, A] =
      for
        prepared       <- readLock(root => prepare(root).zip(version))
        (candidate, v0) = prepared
        result         <- attemptOptimistic(candidate, v0, commit).either.flatMap {
                            case Right(value)                                              => ZIO.succeed(value)
                            case Left(_: EclipseStoreError.ConflictError) if remaining > 0 => loop(remaining - 1)
                            case Left(error)                                               => ZIO.fail(error)
                          }
      yield result

    loop(maxRetries)

  private def version: UIO[Long] =
    state.get.map(_.version).commit

  private def attemptOptimistic[A](
    prepared: A,
    observedVersion: Long,
    commit: (Root, A) => IO[EclipseStoreError, A],
  ): IO[EclipseStoreError, A] =
    ZIO.uninterruptibleMask { restore =>
      restore(acquireWrite.commit) *>
        restore {
          for
            currentVersion <- version
            _              <-
              if currentVersion == observedVersion then ZIO.unit
              else
                ZIO.fail(
                  EclipseStoreError.ConflictError(
                    s"Optimistic update conflict: expected version $observedVersion but observed $currentVersion",
                    None,
                  )
                )
            root           <- store.load
            snapshot       <- RootSnapshot.capture(root)
            exit           <- commit(root, prepared).exit
            result         <- exit match
                                case Exit.Success(value) =>
                                  store.checkpoint *> completeWrite(committed = true).commit.as(value)
                                case Exit.Failure(cause) =>
                                  RootSnapshot.restore(root, snapshot) *>
                                    store.checkpoint.ignore *>
                                    completeWrite(committed = false).commit *>
                                    ZIO.failCause(cause)
          yield result
        }.ensuring(completeWrite(committed = false).commit)
    }

  private val acquireRead: USTM[Unit] =
    state.get.flatMap { current =>
      STM.check(!current.writer) *> state.update(s => s.copy(readers = s.readers + 1))
    }

  private val releaseRead: USTM[Unit] =
    state.update(s => s.copy(readers = math.max(0, s.readers - 1)))

  private val acquireWrite: USTM[Unit] =
    state.get.flatMap { current =>
      STM.check(!current.writer && current.readers == 0) *> state.update(_.copy(writer = true))
    }

  private def completeWrite(committed: Boolean): USTM[Unit] =
    state.update(s => s.copy(writer = false, version = if committed then s.version + 1 else s.version))

object StorageLockLive:
  final case class State(readers: Int, writer: Boolean, version: Long)

object StorageLock:
  def live[Root: Tag]: ZLayer[ObjectStore[Root], Nothing, StorageLock[Root]] =
    ZLayer.fromZIO {
      for
        store <- ZIO.service[ObjectStore[Root]]
        state <- TRef.make(StorageLockLive.State(readers = 0, writer = false, version = 0L)).commit
      yield StorageLockLive(store, state)
    }

  def readLock[Root: Tag, A](effect: Root => IO[EclipseStoreError, A]): ZIO[StorageLock[Root], EclipseStoreError, A] =
    ZIO.serviceWithZIO[StorageLock[Root]](_.readLock(effect))

  def writeLock[Root: Tag, A](effect: Root => IO[EclipseStoreError, A]): ZIO[StorageLock[Root], EclipseStoreError, A] =
    ZIO.serviceWithZIO[StorageLock[Root]](_.writeLock(effect))

  def optimisticUpdate[Root: Tag, A](
    prepare: Root => IO[EclipseStoreError, A]
  )(
    commit: (Root, A) => IO[EclipseStoreError, A],
    maxRetries: Int = 8,
  ): ZIO[StorageLock[Root], EclipseStoreError, A] =
    ZIO.serviceWithZIO[StorageLock[Root]](_.optimisticUpdate(prepare)(commit, maxRetries))

private object RootSnapshot:
  def capture[A](root: A): IO[EclipseStoreError, A] =
    ZIO
      .attempt {
        val bytes = ByteArrayOutputStream()
        val out   = ObjectOutputStream(bytes)
        try out.writeObject(root)
        finally out.close()

        val in = ObjectInputStream(ByteArrayInputStream(bytes.toByteArray))
        try in.readObject().asInstanceOf[A]
        finally in.close()
      }
      .mapError(cause => EclipseStoreError.ResourceError("Failed to capture root snapshot for rollback", Some(cause)))

  def restore[A](target: A, snapshot: A): IO[EclipseStoreError, Unit] =
    ZIO
      .attempt {
        (target, snapshot) match
          case (targetMap: ConcurrentHashMap[?, ?], snapshotMap: ConcurrentHashMap[?, ?]) =>
            val typedTarget   = targetMap.asInstanceOf[ConcurrentHashMap[Any, Any]]
            val typedSnapshot = snapshotMap.asInstanceOf[ConcurrentHashMap[Any, Any]]
            typedTarget.clear()
            typedTarget.putAll(typedSnapshot)
          case _                                                                          =>
            copyFields(target.asInstanceOf[AnyRef], snapshot.asInstanceOf[AnyRef])
      }
      .mapError(cause =>
        EclipseStoreError.ResourceError("Failed to restore root snapshot after aborted write", Some(cause))
      )

  private def copyFields(target: AnyRef, snapshot: AnyRef): Unit =
    def hierarchy(current: Class[?]): List[Class[?]] =
      Option(current.getSuperclass) match
        case Some(parent) => current :: hierarchy(parent)
        case None         => List(current)

    val fields = hierarchy(target.getClass)
      .flatMap(_.getDeclaredFields.toList)
      .filterNot(field => Modifier.isStatic(field.getModifiers))

    fields.foreach { field =>
      field.setAccessible(true)
      field.set(target, field.get(snapshot))
    }
