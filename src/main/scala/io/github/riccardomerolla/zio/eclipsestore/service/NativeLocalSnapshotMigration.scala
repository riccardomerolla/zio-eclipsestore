package io.github.riccardomerolla.zio.eclipsestore.service

import zio.*
import zio.schema.Schema

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.schema.{ DerivedMigrationPlan, MigrationPlan }

final case class NativeLocalSnapshotMigrationRegistry[Root](entries: Chunk[NativeLocalMigrationPlan[Root]]):
  def migrateOrFail(
    envelope: NativeLocalSnapshotEnvelope,
    serde: NativeLocalSerde,
  ): IO[EclipseStoreError, NativeLocalMigrationOutcome[Root]] =
    entries.find(_.matches(envelope)) match
      case Some(entry) => entry.migrate(envelope, serde)
      case None        =>
        ZIO.fail(
          EclipseStoreError.MigrationError(
            s"No NativeLocal snapshot migration is registered for root '${envelope.rootId}' and fingerprint '${envelope.schemaFingerprint}'",
            None,
          )
        )

object NativeLocalSnapshotMigrationRegistry:
  def none[Root]: NativeLocalSnapshotMigrationRegistry[Root] =
    NativeLocalSnapshotMigrationRegistry(Chunk.empty)

  def single[Root](entry: NativeLocalMigrationPlan[Root]): NativeLocalSnapshotMigrationRegistry[Root] =
    NativeLocalSnapshotMigrationRegistry(Chunk(entry))

object NativeLocalSnapshotMigration:
  def fromPlan[Old: Schema, Root: Schema](
    rootId: String,
    plan: MigrationPlan[Old, Root],
    sourceSchemaVersion: Option[Int] = None,
    targetSchemaVersion: Option[Int] = None,
  ): NativeLocalMigrationPlan[Root] =
    NativeLocalMigrationPlan.fromPlan(rootId, plan, sourceSchemaVersion, targetSchemaVersion)

  def fromDerived[Old: Schema, Root: Schema](
    rootId: String,
    plan: DerivedMigrationPlan[Old, Root],
    sourceSchemaVersion: Option[Int] = None,
    targetSchemaVersion: Option[Int] = None,
  ): NativeLocalMigrationPlan[Root] =
    NativeLocalMigrationPlan.fromDerived(rootId, plan, sourceSchemaVersion, targetSchemaVersion)

  def fromFunction[Old: Schema, Root: Schema](
    rootId: String,
    sourceSchemaVersion: Option[Int] = None,
    targetSchemaVersion: Option[Int] = None,
  )(
    f: Old => IO[EclipseStoreError, Root]
  ): NativeLocalMigrationPlan[Root] =
    NativeLocalMigrationPlan.fromFunction(rootId, sourceSchemaVersion, targetSchemaVersion)(f)
