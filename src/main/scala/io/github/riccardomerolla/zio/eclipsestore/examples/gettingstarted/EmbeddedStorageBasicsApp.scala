package io.github.riccardomerolla.zio.eclipsestore.examples.gettingstarted

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

final case class User(id: UUID, name: String, city: String)

final case class UserDirectory(users: ConcurrentHashMap[UUID, User])

object UserDirectory:
  val descriptor: RootDescriptor[UserDirectory] =
    RootDescriptor(
      id = "user-directory",
      initializer = () => UserDirectory(new ConcurrentHashMap[UUID, User]()),
    )

/** Demonstrates EclipseStore root instances, updates, and reloading with zio-eclipsestore. */
object EmbeddedStorageBasicsApp extends ZIOAppDefault:

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    val program =
      for
        root      <- EclipseStoreService.root(UserDirectory.descriptor)
        _         <- putUser(root, User(UUID.randomUUID(), "Dana", "Berlin"))
        _         <- putUser(root, User(UUID.randomUUID(), "Lee", "London"))
        all       <- ZIO.succeed(root.users.values().toArray(new Array[User](0)).toList)
        _         <- ZIO.logInfo(s"Users on disk: ${all.mkString(", ")}")
        _         <- EclipseStoreService.maintenance(
                       io.github.riccardomerolla.zio.eclipsestore.service.LifecycleCommand.Checkpoint
                     )
        _         <- EclipseStoreService.reloadRoots
        userCount <- ZIO.succeed(root.users.size())
        _         <- ZIO.logInfo(s"Reloaded directory contains $userCount entries")
      yield ()

    program
      .provide(
        EclipseStoreConfig.temporaryLayer,
        EclipseStoreService.live,
      )
      .catchAll(e => ZIO.logError(e.toString))

  private def putUser(directory: UserDirectory, user: User): ZIO[EclipseStoreService, EclipseStoreError, Unit] =
    for
      _ <- ZIO.succeed(directory.users.put(user.id, user))
      _ <- EclipseStoreService.persist(directory)
    yield ()
