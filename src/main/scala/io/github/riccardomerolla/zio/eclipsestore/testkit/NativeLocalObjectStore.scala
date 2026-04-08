package io.github.riccardomerolla.zio.eclipsestore.testkit

import java.nio.file.{ Files, Path }

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.Schema

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ NativeLocal, ObjectStore, StorageOps }

object NativeLocalObjectStore:
  def layer[Root: Tag: Schema](
    snapshotPath: Path,
    descriptor: RootDescriptor[Root],
  ): ZLayer[Any, EclipseStoreError, ObjectStore[Root] & StorageOps[Root]] =
    NativeLocal.live(snapshotPath, descriptor)

  def tempLayer[Root: Tag: Schema](
    descriptor: RootDescriptor[Root],
    prefix: String = "native-local-testkit",
  ): ZLayer[Scope, EclipseStoreError, ObjectStore[Root] & StorageOps[Root]] =
    ZLayer.scopedEnvironment {
      for
        snapshotDir <- ZIO.acquireRelease(createTempDirectory(prefix))(deleteDirectory)
        snapshotPath = snapshotDir.resolve(s"${descriptor.id}.snapshot.json")
        built       <- layer(snapshotPath, descriptor).build
      yield ZEnvironment[ObjectStore[Root], StorageOps[Root]](
        built.get[ObjectStore[Root]],
        built.get[StorageOps[Root]],
      )
    }

  private def createTempDirectory(prefix: String): IO[EclipseStoreError, Path] =
    ZIO
      .attemptBlocking(Files.createTempDirectory(prefix))
      .mapError(cause =>
        EclipseStoreError.ResourceError(s"Failed to create NativeLocal testkit directory for $prefix", Some(cause))
      )

  private def deleteDirectory(path: Path): UIO[Unit] =
    ZIO
      .attemptBlocking {
        if Files.exists(path) then
          Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
      }
      .ignore
