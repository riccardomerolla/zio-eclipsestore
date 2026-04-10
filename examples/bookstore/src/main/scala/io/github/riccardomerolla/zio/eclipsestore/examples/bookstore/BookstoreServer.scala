package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import java.nio.file.Paths

import zio.*
import zio.http.Server

import io.github.riccardomerolla.zio.eclipsestore.config.BackendConfig
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.BookstoreRoot
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.http.BookRoutes
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.BookRepository
import io.github.riccardomerolla.zio.eclipsestore.service.StorageBackend

object BookstoreServer extends ZIOAppDefault:

  private val serverLayer: ZLayer[Any, Nothing, Server] =
    Server.defaultWithPort(8080).orDie

  private val defaultBackendConfig: BackendConfig =
    BackendConfig.FileSystem(Paths.get("bookstore-data"))

  private val repositoryLayer: ZLayer[Any, Nothing, BookRepository] =
    ZLayer.succeed(defaultBackendConfig) >>>
      StorageBackend
        .rootServices(
          BookstoreRoot.descriptor,
          _.copy(rootDescriptors = Chunk.single(BookstoreRoot.descriptor)),
        )
        .mapError(err => new RuntimeException(err.toString))
        .orDie >>>
      BookRepository.live

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    Server
      .serve(BookRoutes.routes)
      .provide(
        serverLayer,
        repositoryLayer,
      )
