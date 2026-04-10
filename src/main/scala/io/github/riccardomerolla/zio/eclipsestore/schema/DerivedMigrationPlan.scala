package io.github.riccardomerolla.zio.eclipsestore.schema

import zio.*
import zio.json.ast.Json
import zio.schema.Schema

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

final case class DerivedMigrationPlan[Old, New](
  autoDerived: Chunk[MigrationStep],
  overrides: Chunk[MigrationStep],
  delegate: MigrationPlan[Old, New],
):
  def steps: Chunk[MigrationStep] =
    autoDerived ++ overrides

  def validate: IO[EclipseStoreError, Unit] =
    delegate.validate

  def migrate(value: Old): IO[EclipseStoreError, New] =
    delegate.migrate(value)

  def migratePayload(payload: String): IO[EclipseStoreError, New] =
    delegate.migratePayload(payload)

object DerivedMigrationPlan:
  def derive[Old: Schema, New: Schema](
    newFieldDefaults: Map[String, Json] = Map.empty,
    overrides: MigrationStep*
  ): DerivedMigrationPlan[Old, New] =
    val overrideChunk   = Chunk.fromIterable(overrides)
    val coveredFields   = overrideChunk.flatMap(coveredFieldNames).toSet
    val oldFields       = SchemaIntrospection.topLevelFields(summon[Schema[Old]])
    val newFields       = SchemaIntrospection.topLevelFields(summon[Schema[New]])
    val removedFields   =
      Chunk.fromIterable(oldFields.keySet.diff(newFields.keySet).diff(coveredFields)).map(Migration.removeField)
    val autoAddedFields =
      Chunk.fromIterable(newFields.keySet.diff(oldFields.keySet).diff(coveredFields))
        .flatMap(name => newFieldDefaults.get(name).map(Migration.addField(name, _)))
    val delegate        = Migration[Old, New](removedFields ++ autoAddedFields ++ overrideChunk).plan

    DerivedMigrationPlan(
      autoDerived = removedFields ++ autoAddedFields,
      overrides = overrideChunk,
      delegate = delegate,
    )

  private def coveredFieldNames(step: MigrationStep): Chunk[String] =
    step match
      case MigrationStep.AddField(name, _)         => Chunk(name)
      case MigrationStep.RemoveField(name)         => Chunk(name)
      case MigrationStep.RenameField(from, to)     => Chunk(from, to)
      case MigrationStep.ChangeField(name, _)      => Chunk(name)
      case MigrationStep.SplitField(name, targets) => Chunk(name) ++ targets.map(_._1)
      case MigrationStep.MergeFields(names, to, _) => names ++ Chunk(to)
