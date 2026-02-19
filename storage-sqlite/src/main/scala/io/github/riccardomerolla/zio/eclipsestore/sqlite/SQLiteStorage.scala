package io.github.riccardomerolla.zio.eclipsestore.sqlite

import java.nio.file.Path

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

/** ZIO-friendly wiring helpers for SQLite storage targets. */
object SQLiteStorage:

  /** Builds an [[EclipseStoreConfig]] for a SQLite target located under the provided directory. */
  def config(
    path: Path,
    storageName: String = "eclipsestore.db",
    connectionString: Option[String] = None,
  ): EclipseStoreConfig =
    EclipseStoreConfig(storageTarget = StorageTarget.Sqlite(path.resolve(storageName), storageName, connectionString))

  /** Layer that provides [[EclipseStoreService]] backed by SQLite-style storage. */
  def live(
    path: Path,
    storageName: String = "eclipsestore.db",
    connectionString: Option[String] = None,
  ): ZLayer[Any, EclipseStoreError, EclipseStoreService] =
    ZLayer.succeed(config(path, storageName, connectionString)) >>> EclipseStoreService.live

  /** Convenience accessor to use an existing config layer (e.g., from test setup). */
  val service: ZLayer[EclipseStoreConfig, EclipseStoreError, EclipseStoreService] =
    EclipseStoreService.live
