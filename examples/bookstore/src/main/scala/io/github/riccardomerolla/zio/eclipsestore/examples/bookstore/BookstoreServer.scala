package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import zio.*
import zio.http.Server

import java.nio.file.Paths

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.http.BookRoutes
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.{ BookRepository, BookRepositoryError }
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object BookstoreServer extends ZIOAppDefault:

  private val serverLayer: ZLayer[Any, Nothing, Server] =
    Server.defaultWithPort(8080).orDie

  private val repositoryLayer: ZLayer[Any, Nothing, BookRepository] =
    (
      EclipseStoreConfig.layer(Paths.get("bookstore-data")) >>>
        EclipseStoreService.live.mapError(err => BookRepositoryError.StorageFailure(err.toString)).orDie >>>
        BookRepository.live
    )

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    Server
      .serve(BookRoutes.routes)
      .provide(
        serverLayer,
        repositoryLayer,
      )
