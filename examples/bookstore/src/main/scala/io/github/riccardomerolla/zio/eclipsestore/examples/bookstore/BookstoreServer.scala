package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import zio.*
import zio.http.Server

import java.nio.file.Paths

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.BookstoreRoot
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.http.BookRoutes
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.BookRepository
import io.github.riccardomerolla.zio.eclipsestore.schema.TypedStore
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object BookstoreServer extends ZIOAppDefault:

  private val serverLayer: ZLayer[Any, Nothing, Server] =
    Server.defaultWithPort(8080).orDie

  private val repositoryLayer: ZLayer[Any, Nothing, BookRepository] =
    (
      ZLayer.succeed(
        EclipseStoreConfig(
          storageTarget = StorageTarget.FileSystem(Paths.get("bookstore-data")),
          rootDescriptors = Chunk.single(BookstoreRoot.descriptor),
        )
      ) >>>
        EclipseStoreService.live.mapError(err => new RuntimeException(err.toString)).orDie >>>
        TypedStore.live >>>
        BookRepository.live
    )

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    Server
      .serve(BookRoutes.routes)
      .provide(
        serverLayer,
        repositoryLayer,
      )
