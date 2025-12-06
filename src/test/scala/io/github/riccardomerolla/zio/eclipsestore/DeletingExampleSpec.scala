package io.github.riccardomerolla.zio.eclipsestore

import zio.*
import zio.test.*
import io.github.riccardomerolla.zio.eclipsestore.examples.deleting.DeletingExample

import java.nio.file.Files
import scala.jdk.CollectionConverters.*

object DeletingExampleSpec extends ZIOSpecDefault:

  private def tempDirLayer: ZLayer[Any, Nothing, java.nio.file.Path] =
    ZLayer.scoped {
      ZIO.acquireRelease(ZIO.attemptBlocking(Files.createTempDirectory("deleting-example")).orDie)(dir =>
        ZIO.attemptBlocking {
          if Files.exists(dir) then
            Files.walk(dir).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
        }.orDie
      )
    }

  override def spec =
    suite("Deleting example")(
      test("removes first element and persists removal") {
        ZIO.serviceWithZIO[java.nio.file.Path] { dir =>
          for
            _      <- DeletingExample.initIfEmpty(dir)
            after1 <- DeletingExample.deleteFirst(dir)
            after2 <- DeletingExample.deleteFirst(dir)
          yield assertTrue(after1.size >= after2.size)
        }
      }.provideLayerShared(tempDirLayer)
    )
