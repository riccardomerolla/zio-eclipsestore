package io.github.riccardomerolla.zio.eclipsestore.config

import java.nio.file.{ Files, Path }

import zio.*
import zio.test.*

object EclipseStoreConfigZIOSpec extends ZIOSpecDefault:

  private def tempConfigFile(contents: String): ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(
      ZIO.attemptBlocking {
        val path = Files.createTempFile("zio-eclipsestore-config", ".conf")
        Files.writeString(path, contents)
        path
      }
    )(path => ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)

  override def spec: Spec[Environment & (TestEnvironment & Scope), Any] =
    suite("EclipseStoreConfigZIO")(
      test("loads config from resource path") {
        for
          env    <- EclipseStoreConfigZIO.fromResourcePath.build
          cfg     = env.get[EclipseStoreConfig]
          _      <- ZIO.logInfo(s"Loaded config: $cfg")
          result <- cfg.storageTarget match
                      case StorageTarget.FileSystem(path) =>
                        ZIO.succeed(
                          assertTrue(
                            cfg.maxParallelism == 8,
                            cfg.batchSize == 50,
                            !cfg.autoRegisterSchemaHandlers,
                            path == Path.of("/tmp/zio-eclipsestore-config-test"),
                            cfg
                              .backupDirectory
                              .contains(
                                Path.of("/tmp/zio-eclipsestore-config-test/backup")
                              ),
                          )
                        )
                      case _                              => ZIO.succeed(assertTrue(false))
        yield result
      },
      test("loads legacy backend selection from storageTarget config") {
        for
          env   <- EclipseStoreConfigZIO.backendFromResourcePath.build
          config = env.get[BackendConfig]
        yield assertTrue(config == BackendConfig.FileSystem(Path.of("/tmp/zio-eclipsestore-config-test")))
      },
      test("loads NativeLocal backend selection from backend config and prefers it over storageTarget") {
        val fileContents =
          """eclipsestore {
            |  storageTarget {
            |    fileSystem {
            |      path = "/tmp/zio-eclipsestore-config-test"
            |    }
            |  }
            |  backend {
            |    nativeLocal {
            |      snapshotPath = "/tmp/zio-eclipsestore-native-local.snapshot"
            |    }
            |  }
            |}
            |""".stripMargin

        for
          path  <- tempConfigFile(fileContents)
          env   <- EclipseStoreConfigZIO.backendFromFile(path).build
          config = env.get[BackendConfig]
        yield assertTrue(config == BackendConfig.NativeLocal(Path.of("/tmp/zio-eclipsestore-native-local.snapshot")))
      },
    )
