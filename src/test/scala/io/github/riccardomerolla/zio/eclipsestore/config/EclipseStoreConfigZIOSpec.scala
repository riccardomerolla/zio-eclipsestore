package io.github.riccardomerolla.zio.eclipsestore.config

import zio.*
import zio.test.*

import java.nio.file.Path

object EclipseStoreConfigZIOSpec extends ZIOSpecDefault:

  override def spec =
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
      }
    )
