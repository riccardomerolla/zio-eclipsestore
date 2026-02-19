package io.github.riccardomerolla.zio.eclipsestore.sqlite

import java.nio.file.Path

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

/** Convenience wiring for EclipseStore backed by a SQLite storage target. */
object SQLiteAdapter:

  /** Builds a config pointing to a SQLite target under the given directory. */
  def config(
    basePath: Path,
    storageName: String = "eclipsestore.db",
    connectionString: Option[String] = None,
  ): EclipseStoreConfig =
    EclipseStoreConfig(storageTarget =
      StorageTarget.Sqlite(basePath.resolve(storageName), storageName, connectionString)
    )

  /** Live layer with SQLite-backed EclipseStoreService. */
  def live(
    basePath: Path,
    storageName: String = "eclipsestore.db",
    connectionString: Option[String] = None,
  ): ZLayer[Any, EclipseStoreError, EclipseStoreService] =
    ZLayer.succeed(config(basePath, storageName, connectionString)) >>> EclipseStoreService.live

  /** Accessor layer if config already provided. */
  val service: ZLayer[EclipseStoreConfig, EclipseStoreError, EclipseStoreService] =
    EclipseStoreService.live
