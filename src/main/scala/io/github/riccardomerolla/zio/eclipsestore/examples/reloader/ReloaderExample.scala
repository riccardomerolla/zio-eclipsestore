package io.github.riccardomerolla.zio.eclipsestore.examples.reloader

import zio.*

import java.nio.file.Files

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import org.eclipse.serializer.persistence.binary.types.Binary
import org.eclipse.serializer.persistence.util.Reloader

final case class DataRoot(data: scala.collection.mutable.ListBuffer[String])

object ReloaderExample:
  private val rootDescriptor = io
    .github
    .riccardomerolla
    .zio
    .eclipsestore
    .domain
    .RootDescriptor[DataRoot](
      id = "reloader-root",
      initializer = () => DataRoot(scala.collection.mutable.ListBuffer.empty),
      migrate = identity,
      onLoad = identity,
    )

  private def program: ZIO[EclipseStoreService, EclipseStoreError, List[String]] =
    for
      root <- EclipseStoreService.root(rootDescriptor)
      _    <- ZIO
                .foreachDiscard(List("value 1", "value 2"))(v => ZIO.attempt(root.data.addOne(v)))
                .mapError(e => EclipseStoreError.StorageError("seed", Some(e)))
      _    <- EclipseStoreService.persist(root.data)
      _    <- ZIO.attempt(root.data.clear()).mapError(e => EclipseStoreError.StorageError("clear", Some(e)))
      // reload list contents from store
      _    <- EclipseStoreService.execute(
                io.github
                  .riccardomerolla
                  .zio
                  .eclipsestore
                  .domain
                  .Query
                  .Custom[Unit](
                    "reload-flat",
                    ctx =>
                      ctx.storageManager.foreach { sm =>
                        val pm       = sm
                          .persistenceManager()
                          .asInstanceOf[org.eclipse.serializer.persistence.types.PersistenceManager[Binary]]
                        val reloader = Reloader.New(pm)
                        reloader.reloadFlat(root.data)
                      },
                  )
              )
    yield root.data.toList

  def runExample(path: java.nio.file.Path): ZIO[Any, EclipseStoreError, List[String]] =
    val cfg = EclipseStoreConfig(
      storageTarget = StorageTarget.FileSystem(path),
      rootDescriptors = zio.Chunk(rootDescriptor),
    )
    ZIO.scoped {
      (ZLayer.succeed(cfg) >>> EclipseStoreService.live).build.flatMap { env =>
        program.provideEnvironment(env)
      }
    }
