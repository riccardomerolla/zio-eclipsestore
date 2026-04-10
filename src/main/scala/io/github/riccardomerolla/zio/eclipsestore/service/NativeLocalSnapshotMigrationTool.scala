package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.file.Path

import zio.*
import zio.schema.Schema

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

object NativeLocalSnapshotMigrationTool:
  final case class MigrationRewriteResult[Root](
    value: Root,
    provenance: Option[NativeLocalMigrationProvenance],
  )

  def rewrite[Root: Schema](
    snapshotPath: Path,
    rootId: String,
    serde: NativeLocalSerde,
    orElse: => Root,
    migrationRegistry: NativeLocalSnapshotMigrationRegistry[Root],
  ): IO[EclipseStoreError, MigrationRewriteResult[Root]] =
    for
      loaded <- SnapshotCodec.loadEnvelopedOrElse(snapshotPath, rootId, serde, orElse, migrationRegistry)
      _      <- ZIO.when(loaded.rewriteRequired)(
                  SnapshotCodec.saveEnveloped(
                    snapshotPath,
                    loaded.value,
                    rootId,
                    serde,
                    loaded.targetSchemaVersion,
                    loaded.migrationProvenance,
                  )
                )
    yield MigrationRewriteResult(loaded.value, loaded.migrationProvenance)
