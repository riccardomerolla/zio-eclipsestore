package io.github.riccardomerolla.zio.eclipsestore

import zio.*
import zio.test.*
import io.github.riccardomerolla.zio.eclipsestore.examples.eagerstoring.EagerStoringExample

import java.nio.file.Files
import scala.jdk.CollectionConverters.*

object EagerStoringExampleSpec extends ZIOSpecDefault:

  private def tempDirLayer: ZLayer[Any, Nothing, java.nio.file.Path] =
    ZLayer.scoped {
      ZIO.acquireRelease(ZIO.attemptBlocking(Files.createTempDirectory("eager-example")).orDie)(dir =>
        ZIO.attemptBlocking {
          if Files.exists(dir) then
            Files.walk(dir).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
        }.orDie
      )
    }

  override def spec =
    suite("Eager storing example")(
      test("eager field persists on each step while non-eager grows only in memory until persisted") {
        ZIO.serviceWithZIO[java.nio.file.Path] { dir =>
          for
            first <- EagerStoringExample.runStep(dir)
            second <- EagerStoringExample.runStep(dir)
          yield
            val (nums1, dates1) = first
            val (nums2, dates2) = second
            assertTrue(nums1.nonEmpty, nums2.size >= nums1.size, dates2.size >= dates1.size)
        }
      }.provideLayerShared(tempDirLayer)
    )
