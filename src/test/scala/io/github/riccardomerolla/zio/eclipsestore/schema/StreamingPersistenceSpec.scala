package io.github.riccardomerolla.zio.eclipsestore.schema

import java.nio.file.{ Files, Path }
import java.time.Instant

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.{ Schema, derived }
import zio.stream.ZStream
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object StreamingPersistenceSpec extends ZIOSpecDefault:
  final case class StreamRoot(version: Int) derives Schema
  final case class Event(id: String, partition: Int, payload: String, createdAt: Instant) derives Schema

  private val descriptor =
    RootDescriptor.fromSchema[StreamRoot]("stream-root", () => StreamRoot(0))

  @annotation.nowarn("cat=deprecation")
  private val inMemoryTypedStore: ULayer[TypedStore] =
    EclipseStoreService.inMemory >>> TypedStore.live

  private val inMemoryLayer: ULayer[StreamingPersistence[StreamRoot] & TypedStore] =
    inMemoryTypedStore ++ (inMemoryTypedStore >>> StreamingPersistence.live(descriptor))

  @annotation.nowarn("cat=deprecation")
  private def persistentLayer(path: Path)
    : ZLayer[Any, EclipseStoreError, StreamingPersistence[StreamRoot] & TypedStore] =
    val typedStoreLayer =
      ZLayer.succeed(
        EclipseStoreConfig(storageTarget = StorageTarget.FileSystem(path))
      ) >>>
        TypedStore.handlersFor[String, Event] >>>
        EclipseStoreService.live >>>
        TypedStore.live

    typedStoreLayer ++ (typedStoreLayer >>> StreamingPersistence.live(descriptor))

  private def tempDirectory(prefix: String): ZIO[Scope, EclipseStoreError, Path] =
    ZIO.acquireRelease(
      ZIO
        .attempt(Files.createTempDirectory(prefix))
        .mapError(cause =>
          EclipseStoreError.InitializationError(s"Failed to create $prefix temp directory", Some(cause))
        )
    )(path =>
      ZIO.attempt {
        if Files.exists(path) then
          Files.walk(path).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
      }.orDie
    )

  private def event(index: Int): Event =
    Event(
      id = s"evt-$index",
      partition = index % 8,
      payload = s"payload-$index",
      createdAt = Instant.ofEpochMilli(index.toLong),
    )

  private def waitForCommitted(env: ZEnvironment[StreamingPersistence[StreamRoot] & TypedStore])
    : IO[EclipseStoreError, Unit] =
    TypedStore.fetchAll[Event].provideEnvironment(env).flatMap { values =>
      if values.nonEmpty then ZIO.unit
      else
        Live.live(ZIO.sleep(10.millis)) *>
          waitForCommitted(env)
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StreamingPersistence")(
      test("large ingestion completes with batching and reaches eventual consistency") {
        val count = 2048
        val input = ZStream.fromIterable(1 to count).map(index => IngestionItem.Record(s"event:$index", event(index)))

        for
          report <- StreamingPersistence.ingest[StreamRoot, String, Event](
                      input,
                      IngestionConfig(batchSize = 64, parallelism = 4, errorStrategy = ErrorStrategy.FailFast),
                    )
          all    <- TypedStore.fetchAll[Event]
        yield assertTrue(
          report.committed == count,
          report.parked.isEmpty,
          all.size == count,
        )
      }.provideLayer(inMemoryLayer),
      test("park strategy routes malformed records aside without stopping ingestion") {
        val input = ZStream.fromIterable(
          Chunk(
            IngestionItem.Record("event:1", event(1)),
            IngestionItem.Malformed(
              """{"id":"broken"}""",
              EclipseStoreError.QueryError("Malformed event payload", None),
            ),
            IngestionItem.Record("event:2", event(2)),
          )
        )

        for
          report <- StreamingPersistence.ingest[StreamRoot, String, Event](
                      input,
                      IngestionConfig(batchSize = 2, parallelism = 1, errorStrategy = ErrorStrategy.Park),
                    )
          all    <- TypedStore.fetchAll[Event]
        yield assertTrue(
          report.committed == 2,
          report.parked.size == 1,
          all.map(_.id).toSet == Set("evt-1", "evt-2"),
        )
      }.provideLayer(inMemoryLayer),
      test("interruption keeps already committed records durable") {
        val delayedInput =
          ZStream
            .fromIterable(1 to 120)
            .mapZIO(index =>
              Live.live(ZIO.sleep(5.millis)).as(IngestionItem.Record(s"event:$index", event(index)))
            )

        for
          dir <- tempDirectory("streaming-interrupt")
          out <- ZIO.scoped {
                   for {
                     env    <- persistentLayer(dir).build
                     fiber  <-
                       StreamingPersistence
                         .ingest[StreamRoot, String, Event](
                           delayedInput,
                           IngestionConfig(batchSize = 8, parallelism = 1, errorStrategy = ErrorStrategy.FailFast),
                         )
                         .provideEnvironment(env)
                         .fork
                     _      <- waitForCommitted(env).timeoutFail(
                                 EclipseStoreError.QueryError("Timed out waiting for committed streaming records", None)
                               )(1.second)
                     _      <- fiber.interrupt
                     values <- TypedStore.fetchAll[Event].provideEnvironment(env)
                   } yield values
                 }
        yield assertTrue(out.nonEmpty, out.size < 120)
      },
      test("parallel ingestion from multiple fibers does not lose records") {
        val first  =
          ZStream.fromIterable(1 to 200).map(index => IngestionItem.Record(s"a:$index", event(index)))
        val second =
          ZStream.fromIterable(201 to 400).map(index => IngestionItem.Record(s"b:$index", event(index)))

        ZIO.scoped {
          for
            env     <- inMemoryLayer.build
            reportA <- StreamingPersistence
                         .ingest[StreamRoot, String, Event](
                           first,
                           IngestionConfig(batchSize = 32, parallelism = 2, errorStrategy = ErrorStrategy.FailFast),
                         )
                         .provideEnvironment(env)
                         .fork
            reportB <- StreamingPersistence
                         .ingest[StreamRoot, String, Event](
                           second,
                           IngestionConfig(batchSize = 32, parallelism = 2, errorStrategy = ErrorStrategy.FailFast),
                         )
                         .provideEnvironment(env)
                         .fork
            doneA   <- reportA.join
            doneB   <- reportB.join
            values  <- TypedStore.fetchAll[Event].provideEnvironment(env)
          yield assertTrue(
            doneA.committed == 200,
            doneB.committed == 200,
            values.size == 400,
            values.map(_.id).toSet.size == 400,
          )
        }
      },
      test("filtered extraction streams bounded batches") {
        val input = ZStream.fromIterable(1 to 150).map(index => IngestionItem.Record(s"event:$index", event(index)))

        for
          _       <- StreamingPersistence.ingest[StreamRoot, String, Event](
                       input,
                       IngestionConfig(batchSize = 25, parallelism = 2, errorStrategy = ErrorStrategy.FailFast),
                     )
          batches <- StreamingPersistence
                       .extractBatches[StreamRoot, Event](_.partition == 0, batchSize = 11)
                       .runCollect
        yield
          val flattened = batches.flatten
          assertTrue(
            batches.forall(_.size <= 11),
            flattened.forall(_.partition == 0),
            flattened.nonEmpty,
          )
      }.provideLayer(inMemoryLayer),
      test("batch extraction does not drop elements or exceed configured limits") {
        check(Gen.int(1, 60), Gen.int(1, 12)) { (size, batchSize) =>
          val input = ZStream.fromIterable(1 to size).map(index => IngestionItem.Record(s"event:$index", event(index)))

          (
            for
              report  <-
                StreamingPersistence.ingest[StreamRoot, String, Event](
                  input,
                  IngestionConfig(batchSize = batchSize, parallelism = 1, errorStrategy = ErrorStrategy.FailFast),
                )
              batches <- StreamingPersistence
                           .extractBatches[StreamRoot, Event](_ => true, batchSize)
                           .runCollect
            yield
              val flattened = batches.flatten
              assertTrue(
                report.committed == size,
                batches.forall(_.size <= batchSize),
                flattened.size == size,
                flattened.map(_.id).toSet.size == size,
              )
          ).provideLayer(inMemoryLayer)
        }
      },
    )
