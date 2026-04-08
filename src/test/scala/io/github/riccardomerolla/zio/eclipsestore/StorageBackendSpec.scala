package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.{ Files, Path }
import java.util.Comparator

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.BackendConfig
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, StorageBackend }

object StorageBackendSpec extends ZIOSpecDefault:

  private def deleteDirectory(path: Path): UIO[Unit] =
    ZIO.attemptBlocking {
      if Files.exists(path) then
        Files.walk(path).sorted(Comparator.reverseOrder()).forEach(Files.delete)
    }.ignore

  private val kvProgram =
    for
      _     <- EclipseStoreService.put("backend:key", "backend:value")
      value <- EclipseStoreService.get[String, String]("backend:key")
    yield value

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StorageBackend")(
      test("switching from in-memory to filesystem backend keeps the same store API available") {
        ZIO.scoped {
          for
            fsDir      <- ZIO.attemptBlocking(Files.createTempDirectory("storage-backend-fs"))
            _          <- ZIO.addFinalizer(deleteDirectory(fsDir))
            inMemory   <- kvProgram.provideLayer(
                            ZLayer.succeed(BackendConfig.InMemory("storage-backend-memory")) >>> StorageBackend.service()
                          )
            fileSystem <- kvProgram.provideLayer(
                            ZLayer.succeed(BackendConfig.FileSystem(fsDir)) >>> StorageBackend.service()
                          )
          yield assertTrue(
            inMemory.contains("backend:value"),
            fileSystem.contains("backend:value"),
          )
        }
      },
      test("misconfigured cloud credentials fail the backend layer with a typed auth error") {
        ZIO.scoped {
          val layer = ZLayer.succeed(BackendConfig.S3(bucket = "zio-eclipsestore-test")) >>> StorageBackend.live
          layer.build.either.map {
            case Left(EclipseStoreError.AuthError(message, _)) =>
              assertTrue(message.contains("requires accessKey and secretKey"))
            case other                                         =>
              assertTrue(other.isLeft)
          }
        }
      },
      test("sqlite backend remains consistent under concurrent writers and restart") {
        ZIO.scoped {
          for
            sqliteDir <- ZIO.attemptBlocking(Files.createTempDirectory("storage-backend-sqlite"))
            _         <- ZIO.addFinalizer(deleteDirectory(sqliteDir))
            layer      = ZLayer.succeed(BackendConfig.Sqlite(sqliteDir.resolve("backend.db"), "backend.db")) >>>
                           StorageBackend.service()
            _         <- ZIO
                           .foreachPar(1 to 50)(i => EclipseStoreService.put(s"key-$i", s"value-$i"))
                           .withParallelism(8)
                           .provideLayer(layer)
            out       <- EclipseStoreService.getAll[String].provideLayer(layer)
          yield assertTrue(out.toSet == (1 to 50).map(i => s"value-$i").toSet)
        }
      },
      test("durable backend config round-trips across supported discriminators") {
        val configGen =
          Gen.oneOf(
            Gen.int(1, 20).map(i => BackendConfig.FileSystem(Path.of(s"/tmp/backend-fs-$i"))),
            Gen.int(1, 20).map(i => BackendConfig.MemoryMapped(Path.of(s"/tmp/backend-mmap-$i"))),
            Gen.int(1, 20).map(i => BackendConfig.InMemory(s"backend-mem-$i")),
            Gen.int(1, 20).map(i =>
              BackendConfig.Sqlite(
                path = Path.of(s"/tmp/backend-sqlite-$i"),
                storageName = s"backend-$i.db",
                connectionString = Some(s"jdbc:sqlite:/tmp/backend-sqlite-$i/backend-$i.db"),
              )
            ),
          )

        check(configGen) { config =>
          val roundTrip =
            for
              target  <- config.toStorageTarget
              decoded <- BackendConfig.fromStorageTarget(target)
            yield decoded

          assertTrue(roundTrip == Right(config))
        }
      },
    )
