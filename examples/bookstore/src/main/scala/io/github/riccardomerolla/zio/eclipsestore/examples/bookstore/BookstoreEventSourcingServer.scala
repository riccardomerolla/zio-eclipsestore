package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import java.nio.file.Paths

import zio.*
import zio.http.Server

import io.github.riccardomerolla.zio.eclipsestore.config.BackendConfig
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.BookstoreEventRoot
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.http.BookRoutes
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.BookRepository
import io.github.riccardomerolla.zio.eclipsestore.service.StorageBackend

object BookstoreEventSourcingServer:
  val defaultBackendConfig: BackendConfig =
    BackendConfig.NativeLocal(Paths.get("bookstore-event-sourcing.snapshot.json"))

  val repositoryLayer: ZLayer[Any, Nothing, BookRepository] =
    ZLayer.succeed(defaultBackendConfig) >>>
      StorageBackend
        .rootServices(BookstoreEventRoot.descriptor)
        .mapError(err => new RuntimeException(err.toString))
        .orDie >>>
      BookRepository.eventSourcedLive

object BookstoreEventSourcingServerApp extends ZIOAppDefault:
  private val serverLayer: ZLayer[Any, Nothing, Server] =
    Server.defaultWithPort(8081).orDie

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    Server
      .serve(BookRoutes.routes)
      .provide(
        serverLayer,
        BookstoreEventSourcingServer.repositoryLayer,
      )
