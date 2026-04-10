package io.github.riccardomerolla.zio.eclipsestore.service

import zio.*
import zio.schema.Schema

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.schema.{ DerivedMigrationPlan, MigrationPlan, SchemaIntrospection }

final case class NativeLocalMigrationProvenance(
  fromFingerprint: String,
  fromSchemaVersion: Option[Int],
  migratedAtEpochMillis: Long,
)

final case class NativeLocalMigrationOutcome[Root](
  value: Root,
  targetSchemaVersion: Option[Int],
  provenance: NativeLocalMigrationProvenance,
)

final case class NativeLocalMigrationPlan[Root](
  rootId: String,
  fromFingerprint: String,
  targetFingerprint: String,
  sourceSchemaVersion: Option[Int],
  targetSchemaVersion: Option[Int],
  derivedStepCount: Int,
  migrateEnvelope: (NativeLocalSnapshotEnvelope, NativeLocalSerde) => IO[EclipseStoreError, Root],
):
  def matches(envelope: NativeLocalSnapshotEnvelope): Boolean =
    envelope.rootId == rootId &&
    envelope.schemaFingerprint == fromFingerprint &&
    sourceSchemaVersion.forall(expected => envelope.schemaVersion.contains(expected))

  def migrate(
    envelope: NativeLocalSnapshotEnvelope,
    serde: NativeLocalSerde,
  ): IO[EclipseStoreError, NativeLocalMigrationOutcome[Root]] =
    for
      root <- migrateEnvelope(envelope, serde)
      now  <- Clock.instant
    yield NativeLocalMigrationOutcome(
      value = root,
      targetSchemaVersion = targetSchemaVersion,
      provenance = NativeLocalMigrationProvenance(
        fromFingerprint = fromFingerprint,
        fromSchemaVersion = envelope.schemaVersion,
        migratedAtEpochMillis = now.toEpochMilli,
      ),
    )

object NativeLocalMigrationPlan:
  def fromPlan[Old: Schema, Root: Schema](
    rootId: String,
    plan: MigrationPlan[Old, Root],
    sourceSchemaVersion: Option[Int] = None,
    targetSchemaVersion: Option[Int] = None,
  ): NativeLocalMigrationPlan[Root] =
    fromFunction[Old, Root](
      rootId = rootId,
      sourceSchemaVersion = sourceSchemaVersion,
      targetSchemaVersion = targetSchemaVersion,
      derivedStepCount = 0,
    ) { old =>
      plan.validate *> plan.migrate(old)
    }

  def fromDerived[Old: Schema, Root: Schema](
    rootId: String,
    plan: DerivedMigrationPlan[Old, Root],
    sourceSchemaVersion: Option[Int] = None,
    targetSchemaVersion: Option[Int] = None,
  ): NativeLocalMigrationPlan[Root] =
    fromFunction[Old, Root](
      rootId = rootId,
      sourceSchemaVersion = sourceSchemaVersion,
      targetSchemaVersion = targetSchemaVersion,
      derivedStepCount = plan.autoDerived.size,
    ) { old =>
      plan.validate *> plan.migrate(old)
    }

  def fromFunction[Old: Schema, Root: Schema](
    rootId: String,
    sourceSchemaVersion: Option[Int] = None,
    targetSchemaVersion: Option[Int] = None,
    derivedStepCount: Int = 0,
  )(
    f: Old => IO[EclipseStoreError, Root]
  ): NativeLocalMigrationPlan[Root] =
    NativeLocalMigrationPlan(
      rootId = rootId,
      fromFingerprint = SchemaIntrospection.fingerprint(summon[Schema[Old]]),
      targetFingerprint = SchemaIntrospection.fingerprint(summon[Schema[Root]]),
      sourceSchemaVersion = sourceSchemaVersion,
      targetSchemaVersion = targetSchemaVersion,
      derivedStepCount = derivedStepCount,
      migrateEnvelope = (envelope, serde) =>
        SnapshotCodec.decodePayload[Old](envelope.payload, serde).flatMap(f),
    )
