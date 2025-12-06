package io.github.riccardomerolla.zio.eclipsestore.examples.deleting

import zio.*

import java.nio.file.Path

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

final case class Person(name: String, age: Int)
final case class DeletingRoot(objects: scala.collection.mutable.ListBuffer[Person])

object DeletingExample:
  private val rootDescriptor =
    io.github
      .riccardomerolla
      .zio
      .eclipsestore
      .domain
      .RootDescriptor[DeletingRoot](
        id = "deleting-root",
        initializer = () => DeletingRoot(scala.collection.mutable.ListBuffer.empty),
        migrate = identity,
        onLoad = identity,
      )

  private def layer(path: Path): ZLayer[Any, EclipseStoreError, EclipseStoreService] =
    val cfg = EclipseStoreConfig(
      storageTarget = StorageTarget.FileSystem(path),
      rootDescriptors = zio.Chunk(rootDescriptor),
    )
    ZLayer.succeed(cfg) >>> EclipseStoreService.live

  def initIfEmpty(path: Path): ZIO[Any, EclipseStoreError, Unit] =
    ZIO.scoped {
      layer(path).build.flatMap { env =>
        val svc = env.get[EclipseStoreService]
        for {
          root <- svc.root(rootDescriptor)
          _    <- ZIO.when(root.objects.isEmpty) {
                    ZIO
                      .attempt {
                        root
                          .objects
                          .addAll(
                            Seq(Person("Alice", 20), Person("Bob", 25), Person("Claire", 18))
                          )
                      }
                      .mapError(e => EclipseStoreError.StorageError("init", Some(e))) *>
                      svc.persist(root.objects)
                  }
        } yield ()
      }
    }

  def deleteFirst(path: Path): ZIO[Any, EclipseStoreError, List[Person]] =
    ZIO.scoped {
      layer(path).build.flatMap { env =>
        val svc = env.get[EclipseStoreService]
        for {
          root   <- svc.root(rootDescriptor)
          _      <- ZIO
                      .attempt {
                        if root.objects.nonEmpty then root.objects.remove(0)
                      }
                      .mapError(e => EclipseStoreError.StorageError("delete", Some(e)))
          _      <- svc.persist(root.objects)
          result <- ZIO.succeed(root.objects.toList)
        } yield result
      }
    }
