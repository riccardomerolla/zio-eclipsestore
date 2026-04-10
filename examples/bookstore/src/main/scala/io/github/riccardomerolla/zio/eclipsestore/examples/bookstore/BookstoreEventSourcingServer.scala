package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import java.nio.file.Paths

import zio.*
import zio.http.Server

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalEventingConfig
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.{ BookstoreEvent, BookstoreEventSourcing, BookstoreProjection }
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.http.BookRoutes
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.BookRepository
import io.github.riccardomerolla.zio.eclipsestore.service.{ EventSourcedRuntimeLive, NativeLocalEventStore, NativeLocalSnapshotStore, SnapshotPolicy }

object BookstoreEventSourcingServer:
  val defaultEventingConfig: NativeLocalEventingConfig =
    NativeLocalEventingConfig(Paths.get("bookstore-eventing"))

  val repositoryLayer: ZLayer[Any, Nothing, BookRepository] =
    ZLayer.make[BookRepository](
      NativeLocalEventStore.live[BookstoreEvent](defaultEventingConfig),
      NativeLocalSnapshotStore.live[BookstoreProjection](defaultEventingConfig),
      EventSourcedRuntimeLive.layer(
        BookstoreEventSourcing,
        SnapshotPolicy.everyNEvents[BookstoreProjection, BookstoreEvent](100),
      ),
      BookRepository.eventSourcedLive,
    )

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
