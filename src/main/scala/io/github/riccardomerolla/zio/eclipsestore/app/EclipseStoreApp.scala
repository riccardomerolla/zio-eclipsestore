package io.github.riccardomerolla.zio.eclipsestore.app

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.domain.{ Query, RootDescriptor }
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, LifecycleCommand }
import scala.collection.mutable.ListBuffer

/** Example application demonstrating ZIO EclipseStore usage */
object EclipseStoreApp extends ZIOAppDefault:

  override def run =
    val program =
      for
        // Store some values in batch
        _ <- EclipseStoreService.putAll(
               List("user:1" -> "Alice", "user:2" -> "Bob", "user:3" -> "Charlie")
             )
        _ <- ZIO.logInfo("Stored 3 users")

        // Retrieve a single value
        user <- EclipseStoreService.get[String, String]("user:1")
        _    <- ZIO.logInfo(s"Retrieved user:1 = ${user.getOrElse("not found")}")

        // Stream all values
        snapshot <- EclipseStoreService.streamValues[String].runCollect
        _        <- ZIO.logInfo(s"All users: ${snapshot.mkString(", ")}")

        // Work with typed root instances
        favoritesDescriptor: RootDescriptor[ListBuffer[String]] = RootDescriptor(
                                                                    id = "favorite-users",
                                                                    initializer = () => ListBuffer.empty[String],
                                                                  )
        favorites                                              <- EclipseStoreService.root(favoritesDescriptor)
        _                                                      <- ZIO.succeed(favorites.addOne("user:1"))
        _                                                      <- EclipseStoreService.maintenance(LifecycleCommand.Checkpoint)

        // Execute multiple queries in batch
        queries  = List(
                     Query.Get[String, String]("user:1"),
                     Query.Get[String, String]("user:2"),
                     Query.Get[String, String]("user:3"),
                   )
        results <- EclipseStoreService.executeMany(queries)
        _       <- ZIO.logInfo(s"Batch query results: ${results.mkString(", ")}")

        // Delete a value
        _ <- EclipseStoreService.delete("user:2")
        _ <- ZIO.logInfo("Deleted user:2")

        // Verify deletion
        remainingUsers <- EclipseStoreService.getAll[String]
        _              <- ZIO.logInfo(s"Remaining users: ${remainingUsers.mkString(", ")}")
      yield ()

    program
      .tapError(e => ZIO.logError(s"Application error: $e"))
      .provide(
        EclipseStoreConfig.temporaryLayer,
        EclipseStoreService.live,
      )
