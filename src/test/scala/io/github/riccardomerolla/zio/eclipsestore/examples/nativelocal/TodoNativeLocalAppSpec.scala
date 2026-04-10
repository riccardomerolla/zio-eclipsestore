package io.github.riccardomerolla.zio.eclipsestore.examples.nativelocal

import java.nio.file.{ Files, Path }

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde

object TodoNativeLocalAppSpec extends ZIOSpecDefault:

  private def withTempDirectory[A](prefix: String)(use: Path => ZIO[Any, Throwable, A]): ZIO[Any, Throwable, A] =
    ZIO.scoped {
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory(prefix))) { path =>
        ZIO.attemptBlocking {
          if Files.exists(path) then
            Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
        }.ignore
      }.flatMap(use)
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("TodoNativeLocalApp")(
      test("persists todo mutations across checkpoint and restart") {
        withTempDirectory("todo-native-local-sample") { dir =>
          val snapshotPath = dir.resolve("todo.snapshot.json")
          val layer        = TodoService.layer(snapshotPath)

          (for
            created  <- (for
                          first <- TodoService.add("write docs")
                          _     <- TodoService.add("record benchmark baseline")
                          _     <- TodoService.complete(first.id)
                          after <- TodoService.checkpointAndReload
                        yield (first, after)).provideLayer(layer)
            reopened <- TodoService.list.provideLayer(layer)
          yield assertTrue(
            created._2.size == 2,
            reopened.size == 2,
            reopened.exists(todo => todo.id == created._1.id && todo.completed),
            reopened.exists(todo => todo.title == "record benchmark baseline" && !todo.completed),
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
      test("persists protobuf todo mutations across checkpoint and restart") {
        withTempDirectory("todo-native-local-protobuf-sample") { dir =>
          val snapshotPath = dir.resolve("todo.snapshot.pb")
          val layer        = TodoService.layer(snapshotPath, NativeLocalSerde.Protobuf)

          (for
            created  <- (for
                          first <- TodoService.add("write protobuf docs")
                          _     <- TodoService.add("verify protobuf sample")
                          _     <- TodoService.complete(first.id)
                          after <- TodoService.checkpointAndReload
                        yield (first, after)).provideLayer(layer)
            reopened <- TodoService.list.provideLayer(layer)
          yield assertTrue(
            created._2.size == 2,
            reopened.size == 2,
            reopened.exists(todo => todo.id == created._1.id && todo.completed),
            reopened.exists(todo => todo.title == "verify protobuf sample" && !todo.completed),
          )).mapError(err => new RuntimeException(err.toString))
        }
      },
    )
