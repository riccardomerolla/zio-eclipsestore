package io.github.riccardomerolla.zio.eclipsestore.examples.gettingstarted

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

/** Minimal port of EclipseStore's getting-started sample built on top of zio-eclipsestore. */
object GettingStartedApp extends ZIOAppDefault:

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    val program =
      for
        _         <- EclipseStoreService.put("user:1", "Alice")
        _         <- EclipseStoreService.put("user:2", "Bob")
        first     <- EclipseStoreService.get[String, String]("user:1")
        _         <- ZIO.logInfo(s"Fetched user: ${first.getOrElse("n/a")}")
        keys      <- EclipseStoreService.executeMany(
                       List(
                         io.github.riccardomerolla.zio.eclipsestore.domain.Query.Get[String, String]("user:1"),
                         io.github.riccardomerolla.zio.eclipsestore.domain.Query.Get[String, String]("user:2"),
                         io.github.riccardomerolla.zio.eclipsestore.domain.Query.GetAllKeys[String](),
                       )
                     )
        _         <- ZIO.logInfo(s"Batched lookup results: ${keys.mkString(", ")}")
        allValues <- EclipseStoreService.getAll[String]
        _         <- ZIO.logInfo(s"Stored values: ${allValues.mkString(", ")}")
      yield ()

    program
      .provide(
        EclipseStoreConfig.temporaryLayer,
        EclipseStoreService.live,
      )
      .orDieWith(e => new RuntimeException(e.toString))
