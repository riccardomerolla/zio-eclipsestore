package io.github.riccardomerolla.zio.eclipsestore.service

import zio.*
import zio.schema.Schema

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.schema.{ DerivedMigrationPlan, MigrationPlan, SchemaIntrospection }

trait NativeLocalSnapshotMigration[Root]:
  def matches(envelope: NativeLocalSnapshotEnvelope): Boolean
  def migrate(envelope: NativeLocalSnapshotEnvelope, serde: NativeLocalSerde): IO[EclipseStoreError, Root]

final case class NativeLocalSnapshotMigrationRegistry[Root](entries: Chunk[NativeLocalSnapshotMigration[Root]]):
  def migrateOrFail(envelope: NativeLocalSnapshotEnvelope, serde: NativeLocalSerde): IO[EclipseStoreError, Root] =
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

  def single[Root](entry: NativeLocalSnapshotMigration[Root]): NativeLocalSnapshotMigrationRegistry[Root] =
    NativeLocalSnapshotMigrationRegistry(Chunk(entry))

object NativeLocalSnapshotMigration:
  def fromPlan[Old: Schema, Root](
    rootId: String,
    plan: MigrationPlan[Old, Root],
    schemaVersion: Option[Int] = None,
  ): NativeLocalSnapshotMigration[Root] =
    fromFunction[Old, Root](rootId, schemaVersion) { old =>
      plan.validate *> plan.migrate(old)
    }

  def fromDerived[Old: Schema, Root](
    rootId: String,
    plan: DerivedMigrationPlan[Old, Root],
    schemaVersion: Option[Int] = None,
  ): NativeLocalSnapshotMigration[Root] =
    fromFunction[Old, Root](rootId, schemaVersion) { old =>
      plan.validate *> plan.migrate(old)
    }

  def fromFunction[Old: Schema, Root](
    rootId: String,
    schemaVersion: Option[Int] = None,
  )(
    f: Old => IO[EclipseStoreError, Root]
  ): NativeLocalSnapshotMigration[Root] =
    new NativeLocalSnapshotMigration[Root]:
      private val fromFingerprint = SchemaIntrospection.fingerprint(summon[Schema[Old]])

      override def matches(envelope: NativeLocalSnapshotEnvelope): Boolean =
        envelope.rootId == rootId &&
        envelope.schemaFingerprint == fromFingerprint &&
        schemaVersion.forall(expected => envelope.schemaVersion.contains(expected))

      override def migrate(envelope: NativeLocalSnapshotEnvelope, serde: NativeLocalSerde)
        : IO[EclipseStoreError, Root] =
        SnapshotCodec.decodePayload[Old](envelope.payload, serde).flatMap(f)
