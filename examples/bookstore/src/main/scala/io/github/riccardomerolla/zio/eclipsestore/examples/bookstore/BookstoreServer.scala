package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import zio.*
import zio.http.Server

import java.nio.file.Paths

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.http.BookRoutes
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.BookRepository
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object BookstoreServer extends ZIOAppDefault:

  private val serverLayer: ZLayer[Any, Nothing, Server] =
    Server.defaultWithPort(8080).orDie

  private val repositoryLayer: ZLayer[Any, Nothing, BookRepository] =
    (
      EclipseStoreConfig.layer(Paths.get("bookstore-data")) >>>
        EclipseStoreService.live.mapError(e => new RuntimeException(e.toString)) >>>
        BookRepository.live
    ).orDie

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    Server
      .serve(BookRoutes.routes)
      .provide(
        serverLayer,
        repositoryLayer,
      )
