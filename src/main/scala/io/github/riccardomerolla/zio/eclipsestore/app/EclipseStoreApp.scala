package io.github.riccardomerolla.zio.eclipsestore.app

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import io.github.riccardomerolla.zio.eclipsestore.domain.Query
import zio.*

/** Example application demonstrating ZIO EclipseStore usage */
object EclipseStoreApp extends ZIOAppDefault:
  
  override def run =
    val program =
      for
        // Store some values
        _ <- EclipseStoreService.put("user:1", "Alice")
        _ <- EclipseStoreService.put("user:2", "Bob")
        _ <- EclipseStoreService.put("user:3", "Charlie")
        _ <- ZIO.logInfo("Stored 3 users")
        
        // Retrieve a single value
        user <- EclipseStoreService.get[String, String]("user:1")
        _ <- ZIO.logInfo(s"Retrieved user:1 = ${user.getOrElse("not found")}")
        
        // Retrieve all values
        allUsers <- EclipseStoreService.getAll[String]
        _ <- ZIO.logInfo(s"All users: ${allUsers.mkString(", ")}")
        
        // Execute multiple queries in batch
        queries = List(
          Query.Get[String, String]("user:1"),
          Query.Get[String, String]("user:2"),
          Query.Get[String, String]("user:3")
        )
        results <- EclipseStoreService.executeMany(queries)
        _ <- ZIO.logInfo(s"Batch query results: ${results.mkString(", ")}")
        
        // Delete a value
        _ <- EclipseStoreService.delete("user:2")
        _ <- ZIO.logInfo("Deleted user:2")
        
        // Verify deletion
        remainingUsers <- EclipseStoreService.getAll[String]
        _ <- ZIO.logInfo(s"Remaining users: ${remainingUsers.mkString(", ")}")
        
      yield ()
    
    program
      .tapError(e => ZIO.logError(s"Application error: $e"))
      .provide(
        EclipseStoreConfig.temporaryLayer,
        EclipseStoreService.live
      )
