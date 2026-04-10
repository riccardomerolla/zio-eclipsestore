package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.{ Files, Path }
import java.util.Comparator

import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{
  BackendConfig,
  CorruptSnapshotPolicy,
  MissingSnapshotPolicy,
  NativeLocalSerde,
  NativeLocalStartupPolicy,
}
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{
  EclipseStoreService,
  ObjectStore,
  StorageBackend,
  StorageOps,
}

object StorageBackendSpec extends ZIOSpecDefault:
  final case class NativeCounter(total: Int)

  given Schema[NativeCounter] = DeriveSchema.gen[NativeCounter]
  given Tag[NativeCounter]    = Tag.derived

  private val nativeCounterDescriptor =
    RootDescriptor.fromSchema[NativeCounter]("native-counter", () => NativeCounter(0))

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
      test("NativeLocal backend config wires the shared root services") {
        ZIO.scoped {
          for
            snapshotPath <- ZIO.attemptBlocking(Files.createTempFile("storage-backend-native-local", ".json"))
            _            <- ZIO.attemptBlocking(Files.deleteIfExists(snapshotPath)).ignore
            _            <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(snapshotPath)).ignore)
            layer         = ZLayer.succeed(BackendConfig.NativeLocal(snapshotPath)) >>>
                              StorageBackend.rootServices(nativeCounterDescriptor)
            _            <- (for
                              _ <- ObjectStore.replace(NativeCounter(1))
                              _ <- StorageOps.checkpoint[NativeCounter]
                            yield ()).provideLayer(layer)
            out          <- ObjectStore.load[NativeCounter].provideLayer(layer)
            exists       <- ZIO.attemptBlocking(Files.exists(snapshotPath))
          yield assertTrue(out == NativeCounter(1), exists)
        }
      },
      test("NativeLocal protobuf backend config wires the shared root services") {
        ZIO.scoped {
          for
            snapshotPath <- ZIO.attemptBlocking(Files.createTempFile("storage-backend-native-local-protobuf", ".pb"))
            _            <- ZIO.attemptBlocking(Files.deleteIfExists(snapshotPath)).ignore
            _            <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(snapshotPath)).ignore)
            layer         = ZLayer.succeed(BackendConfig.NativeLocal(snapshotPath, NativeLocalSerde.Protobuf)) >>>
                              StorageBackend.rootServices(nativeCounterDescriptor)
            _            <- (for
                              _ <- ObjectStore.replace(NativeCounter(7))
                              _ <- StorageOps.checkpoint[NativeCounter]
                            yield ()).provideLayer(layer)
            out          <- ObjectStore.load[NativeCounter].provideLayer(layer)
            exists       <- ZIO.attemptBlocking(Files.exists(snapshotPath))
            size         <- ZIO.attemptBlocking(Files.size(snapshotPath))
          yield assertTrue(out == NativeCounter(7), exists, size > 0L)
        }
      },
      test("NativeLocal startup fails with a typed schema error when the snapshot is corrupt") {
        ZIO.scoped {
          for
            snapshotPath <- ZIO.attemptBlocking(Files.createTempFile("storage-backend-native-corrupt", ".json"))
            _            <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(snapshotPath)).ignore)
            _            <- ZIO.attemptBlocking(Files.writeString(snapshotPath, "{ this is not valid json"))
            layer         = ZLayer.succeed(BackendConfig.NativeLocal(snapshotPath)) >>>
                              StorageBackend.rootServices(nativeCounterDescriptor)
            built        <- layer.build.either
          yield built match
            case Left(EclipseStoreError.IncompatibleSchemaError(message, _)) =>
              assertTrue(message.contains("Failed to decode NativeLocal snapshot payload"))
            case other                                                       =>
              assertTrue(other.isLeft)
        }
      },
      test("NativeLocal protobuf startup fails with a typed schema error when the snapshot is corrupt") {
        ZIO.scoped {
          for
            snapshotPath <- ZIO.attemptBlocking(Files.createTempFile("storage-backend-native-protobuf-corrupt", ".pb"))
            _            <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(snapshotPath)).ignore)
            _            <- ZIO.attemptBlocking(Files.write(snapshotPath, Array[Byte](1, 2, 3, 4, 5)))
            layer         = ZLayer.succeed(BackendConfig.NativeLocal(snapshotPath, NativeLocalSerde.Protobuf)) >>>
                              StorageBackend.rootServices(nativeCounterDescriptor)
            built        <- layer.build.either
          yield built match
            case Left(EclipseStoreError.IncompatibleSchemaError(message, _)) =>
              assertTrue(message.contains("Failed to decode NativeLocal protobuf snapshot payload"))
            case other                                                       =>
              assertTrue(other.isLeft)
        }
      },
      test("NativeLocal startup policy can require an existing snapshot") {
        ZIO.scoped {
          for
            snapshotPath <-
              ZIO.attemptBlocking(Files.createTempFile("storage-backend-native-require-existing", ".json"))
            _            <- ZIO.attemptBlocking(Files.deleteIfExists(snapshotPath)).ignore
            layer         = ZLayer.succeed(
                              BackendConfig.NativeLocal(
                                snapshotPath,
                                NativeLocalSerde.Json,
                                NativeLocalStartupPolicy(
                                  missingSnapshot = MissingSnapshotPolicy.RequireExistingSnapshot,
                                  corruptSnapshot = CorruptSnapshotPolicy.Fail,
                                ),
                              )
                            ) >>> StorageBackend.rootServices(nativeCounterDescriptor)
            built        <- layer.build.either
          yield built match
            case Left(EclipseStoreError.InitializationError(message, _)) =>
              assertTrue(message.contains("requires an existing snapshot"))
            case other                                                   =>
              assertTrue(other.isLeft)
        }
      },
      test("NativeLocal startup policy can recover from a corrupt snapshot by starting from empty") {
        ZIO.scoped {
          for
            snapshotPath <- ZIO.attemptBlocking(Files.createTempFile("storage-backend-native-start-empty", ".json"))
            _            <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(snapshotPath)).ignore)
            _            <- ZIO.attemptBlocking(Files.writeString(snapshotPath, "{ broken json"))
            layer         = ZLayer.succeed(
                              BackendConfig.NativeLocal(
                                snapshotPath,
                                NativeLocalSerde.Json,
                                NativeLocalStartupPolicy.startFromEmptyOnCorruptSnapshot,
                              )
                            ) >>> StorageBackend.rootServices(nativeCounterDescriptor)
            out          <- ObjectStore.load[NativeCounter].provideLayer(layer)
          yield assertTrue(out == NativeCounter(0))
        }
      },
    )
