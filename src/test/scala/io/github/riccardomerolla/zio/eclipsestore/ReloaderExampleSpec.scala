package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.Files

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.examples.reloader.ReloaderExample

object ReloaderExampleSpec extends ZIOSpecDefault:

  private def tempDirLayer: ZLayer[Any, Nothing, java.nio.file.Path] =
    ZLayer.scoped {
      ZIO.acquireRelease(ZIO.attemptBlocking(Files.createTempDirectory("reloader-example")).orDie)(dir =>
        ZIO.attemptBlocking {
          if Files.exists(dir) then Files.walk(dir).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
        }.orDie
      )
    }

  override def spec: Spec[Environment & (TestEnvironment & Scope), Any] =
    suite("Reloader example")(
      test("reload restores cleared collection") {
        ZIO.serviceWithZIO[java.nio.file.Path] { dir =>
          for values <- ReloaderExample.runExample(dir)
          yield assertTrue(values == List("value 1", "value 2"))
        }
      }.provideLayerShared(tempDirLayer)
    )
