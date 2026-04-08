package io.github.riccardomerolla.zio.eclipsestore.schema

import scala.collection.immutable.ListMap

import zio.*
import zio.json.*
import zio.json.ast.Json
import zio.schema.Schema
import zio.schema.codec.JsonCodec

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

type JsonFieldMap = ListMap[String, Json]

final case class Migration[Old, New](steps: Chunk[MigrationStep]):
  def andThen[Next](next: Migration[New, Next]): Migration[Old, Next] =
    Migration(steps ++ next.steps)

  def plan(using oldSchema: Schema[Old], newSchema: Schema[New]): MigrationPlan[Old, New] =
    MigrationPlan(this, oldSchema, newSchema)

object Migration:
  def empty[A]: Migration[A, A] =
    Migration(Chunk.empty)

  def define[Old, New](steps: MigrationStep*): Migration[Old, New] =
    Migration(Chunk.fromIterable(steps))

  def addField(name: String, defaultValue: Json): MigrationStep =
    MigrationStep.AddField(name, defaultValue)

  def removeField(name: String): MigrationStep =
    MigrationStep.RemoveField(name)

  def renameField(from: String, to: String): MigrationStep =
    MigrationStep.RenameField(from, to)

  def changeField(name: String)(convert: Json => IO[EclipseStoreError, Json]): MigrationStep =
    MigrationStep.ChangeField(name, convert)

  def splitField(name: String)(targets: (String, Json => IO[EclipseStoreError, Json])*): MigrationStep =
    MigrationStep.SplitField(name, Chunk.fromIterable(targets))

  def mergeFields(names: String*)(to: String)(combine: Chunk[Json] => IO[EclipseStoreError, Json]): MigrationStep =
    MigrationStep.MergeFields(Chunk.fromIterable(names), to, combine)

sealed trait MigrationStep:
  def summary: String

  private[schema] def apply(fields: JsonFieldMap): IO[EclipseStoreError, JsonFieldMap]

object MigrationStep:
  final case class AddField(name: String, defaultValue: Json) extends MigrationStep:
    override val summary: String = s"add-field($name)"

    override private[schema] def apply(fields: JsonFieldMap): IO[EclipseStoreError, JsonFieldMap] =
      if fields.contains(name) then ZIO.succeed(fields)
      else ZIO.succeed(fields.updated(name, defaultValue))

  final case class RemoveField(name: String) extends MigrationStep:
    override val summary: String = s"remove-field($name)"

    override private[schema] def apply(fields: JsonFieldMap): IO[EclipseStoreError, JsonFieldMap] =
      if fields.contains(name) then ZIO.succeed(fields.removed(name))
      else missingField(name, summary)

  final case class RenameField(from: String, to: String) extends MigrationStep:
    override val summary: String = s"rename-field($from->$to)"

    override private[schema] def apply(fields: JsonFieldMap): IO[EclipseStoreError, JsonFieldMap] =
      for
        value <- requireField(fields, from, summary)
      yield fields.removed(from).updated(to, value)

  final case class ChangeField(name: String, convert: Json => IO[EclipseStoreError, Json]) extends MigrationStep:
    override val summary: String = s"change-field($name)"

    override private[schema] def apply(fields: JsonFieldMap): IO[EclipseStoreError, JsonFieldMap] =
      for
        value   <- requireField(fields, name, summary)
        updated <- convert(value).mapError(wrap(summary))
      yield fields.updated(name, updated)

  final case class SplitField(name: String, targets: Chunk[(String, Json => IO[EclipseStoreError, Json])])
    extends MigrationStep:
    override val summary: String =
      s"split-field($name->${targets.map(_._1).mkString(",")})"

    override private[schema] def apply(fields: JsonFieldMap): IO[EclipseStoreError, JsonFieldMap] =
      for
        source  <- requireField(fields, name, summary)
        created <- ZIO.foreach(targets) {
                     case (targetName, convert) =>
                       convert(source).mapBoth(wrap(summary), targetName -> _)
                   }
      yield created.foldLeft(fields.removed(name)) {
        case (acc, (targetName, value)) =>
          acc.updated(targetName, value)
      }

  final case class MergeFields(
    names: Chunk[String],
    to: String,
    combine: Chunk[Json] => IO[EclipseStoreError, Json],
  ) extends MigrationStep:
    override val summary: String =
      s"merge-fields(${names.mkString(",")}->$to)"

    override private[schema] def apply(fields: JsonFieldMap): IO[EclipseStoreError, JsonFieldMap] =
      for
        values <- ZIO.foreach(names)(requireField(fields, _, summary))
        merged <- combine(values).mapError(wrap(summary))
      yield names.foldLeft(fields)((acc, fieldName) => acc.removed(fieldName)).updated(to, merged)

  private def requireField(fields: JsonFieldMap, name: String, step: String): IO[EclipseStoreError, Json] =
    fields.get(name) match
      case Some(value) => ZIO.succeed(value)
      case None        => missingField(name, step)

  private def missingField(name: String, step: String): IO[EclipseStoreError, Nothing] =
    ZIO.fail(EclipseStoreError.MigrationError(s"Migration step $step requires field '$name'", None))

  private def wrap(step: String)(error: EclipseStoreError): EclipseStoreError =
    EclipseStoreError.MigrationError(s"Migration step $step failed: $error", None)

final case class MigrationPlan[Old, New](
  migration: Migration[Old, New],
  oldSchema: Schema[Old],
  newSchema: Schema[New],
):
  def steps: Chunk[MigrationStep] =
    migration.steps

  def validate: IO[EclipseStoreError, Unit] =
    ZIO.foreachDiscard(validationErrors)(error => ZIO.fail(error))

  def migrate(value: Old): IO[EclipseStoreError, New] =
    for
      payload <- encodeOld(value)
      result  <- migratePayload(payload)
    yield result

  def migratePayload(payload: String): IO[EclipseStoreError, New] =
    for
      _        <- validate
      fieldMap <- decodeFieldMap(payload)
      migrated <- ZIO.foldLeft(steps)(fieldMap)((current, step) => step.apply(current))
      result   <- decodeNew(migrated.toJson)
    yield result

  private def validationErrors: Chunk[EclipseStoreError] =
    val oldFields        = SchemaIntrospection.topLevelFields(oldSchema)
    val newFields        = SchemaIntrospection.topLevelFields(newSchema)
    val stepCoverage     = ValidationCoverage.from(steps)
    val untouchedCommon  = oldFields.keySet.intersect(newFields.keySet)
    val incompatibleSame = Chunk.fromIterable(untouchedCommon.collect {
      case name
           if oldFields(name) != newFields(name) && !stepCoverage.changedFields.contains(name) =>
        EclipseStoreError.IncompatibleSchemaError(
          s"Field '$name' changed type without a converter in the migration plan",
          None,
        )
    })
    val missingNewFields =
      Chunk.fromIterable(newFields.keySet.diff(oldFields.keySet).diff(stepCoverage.createdFields)).map(name =>
        EclipseStoreError.IncompatibleSchemaError(
          s"New field '$name' is not covered by add/rename/split/merge in the migration plan",
          None,
        )
      )
    val removedOldFields =
      Chunk.fromIterable(oldFields.keySet.diff(newFields.keySet).diff(stepCoverage.consumedFields)).map(name =>
        EclipseStoreError.IncompatibleSchemaError(
          s"Old field '$name' would be dropped without an explicit remove/rename/split/merge step",
          None,
        )
      )
    val invalidSteps     = steps.flatMap(validateStep(_, oldFields, newFields))
    incompatibleSame ++ missingNewFields ++ removedOldFields ++ invalidSteps

  private def validateStep(
    step: MigrationStep,
    oldFields: Map[String, String],
    newFields: Map[String, String],
  ): Chunk[EclipseStoreError] =
    step match
      case MigrationStep.AddField(name, _) if !newFields.contains(name)                                 =>
        Chunk(EclipseStoreError.IncompatibleSchemaError(
          s"Add-field target '$name' is absent from the new schema",
          None,
        ))
      case MigrationStep.RemoveField(name) if !oldFields.contains(name)                                 =>
        Chunk(EclipseStoreError.IncompatibleSchemaError(
          s"Remove-field source '$name' is absent from the old schema",
          None,
        ))
      case MigrationStep.RenameField(from, to)                                                          =>
        Chunk(
          Option.when(!oldFields.contains(from))(
            EclipseStoreError.IncompatibleSchemaError(s"Rename source '$from' is absent from the old schema", None)
          ),
          Option.when(!newFields.contains(to))(
            EclipseStoreError.IncompatibleSchemaError(s"Rename target '$to' is absent from the new schema", None)
          ),
          Option.when(oldFields.get(from).exists(oldField => newFields.get(to).exists(_ != oldField)))(
            EclipseStoreError.IncompatibleSchemaError(
              s"Rename '$from' -> '$to' changes the field type; use changeField or split/merge instead",
              None,
            )
          ),
        ).flatten
      case MigrationStep.ChangeField(name, _) if !oldFields.contains(name) || !newFields.contains(name) =>
        Chunk(
          EclipseStoreError.IncompatibleSchemaError(
            s"Change-field '$name' requires the field to exist in both old and new schemas",
            None,
          )
        )
      case MigrationStep.SplitField(name, targets)                                                      =>
        val targetErrors = targets.collect {
          case (targetName, _) if !newFields.contains(targetName) =>
            EclipseStoreError.IncompatibleSchemaError(
              s"Split-field target '$targetName' is absent from the new schema",
              None,
            )
        }
        val sourceErrors =
          if oldFields.contains(name) then Chunk.empty
          else
            Chunk(EclipseStoreError.IncompatibleSchemaError(
              s"Split-field source '$name' is absent from the old schema",
              None,
            ))
        sourceErrors ++ targetErrors
      case MigrationStep.MergeFields(names, to, _)                                                      =>
        val sourceErrors = names.collect {
          case name if !oldFields.contains(name) =>
            EclipseStoreError.IncompatibleSchemaError(
              s"Merge-field source '$name' is absent from the old schema",
              None,
            )
        }
        val targetErrors =
          if newFields.contains(to) then Chunk.empty
          else
            Chunk(EclipseStoreError.IncompatibleSchemaError(
              s"Merge-field target '$to' is absent from the new schema",
              None,
            ))
        sourceErrors ++ targetErrors
      case _                                                                                            =>
        Chunk.empty

  private def encodeOld(value: Old): IO[EclipseStoreError, String] =
    ZIO
      .attempt(JsonCodec.jsonCodec(oldSchema).encodeJson(value, None).toString)
      .mapError(cause => EclipseStoreError.MigrationError("Failed to encode source value for migration", Some(cause)))

  private def decodeFieldMap(payload: String): IO[EclipseStoreError, JsonFieldMap] =
    ZIO
      .fromEither(payload.fromJson[JsonFieldMap])
      .mapError(message =>
        EclipseStoreError.MigrationError(s"Failed to decode source payload for migration: $message", None)
      )

  private def decodeNew(payload: String): IO[EclipseStoreError, New] =
    ZIO
      .attempt(JsonCodec.jsonCodec(newSchema).decodeJson(payload))
      .mapError(cause => EclipseStoreError.MigrationError("Failed to decode migrated payload", Some(cause)))
      .flatMap {
        case Right(value) => ZIO.succeed(value)
        case Left(error)  =>
          ZIO.fail(EclipseStoreError.MigrationError(s"Failed to decode migrated payload: $error", None))
      }

private object SchemaIntrospection:
  def topLevelFields[A](schema: Schema[A]): Map[String, String] =
    schema match
      case record: Schema.Record[A] =>
        record.fields.map(field => field.name -> stableShape(field.schema.asInstanceOf[Schema[?]])).toMap
      case _                        =>
        Map.empty

  private def stableShape(schema: Schema[?]): String =
    schema.ast.toString

final private case class ValidationCoverage(
  createdFields: Set[String],
  consumedFields: Set[String],
  changedFields: Set[String],
)

private object ValidationCoverage:
  val empty: ValidationCoverage =
    ValidationCoverage(Set.empty, Set.empty, Set.empty)

  def from(steps: Chunk[MigrationStep]): ValidationCoverage =
    steps.foldLeft(empty) {
      case (acc, MigrationStep.AddField(name, _))         =>
        acc.copy(createdFields = acc.createdFields + name)
      case (acc, MigrationStep.RemoveField(name))         =>
        acc.copy(consumedFields = acc.consumedFields + name)
      case (acc, MigrationStep.RenameField(from, to))     =>
        acc.copy(
          createdFields = acc.createdFields + to,
          consumedFields = acc.consumedFields + from,
        )
      case (acc, MigrationStep.ChangeField(name, _))      =>
        acc.copy(changedFields = acc.changedFields + name)
      case (acc, MigrationStep.SplitField(from, targets)) =>
        acc.copy(
          createdFields = acc.createdFields ++ targets.map(_._1),
          consumedFields = acc.consumedFields + from,
        )
      case (acc, MigrationStep.MergeFields(from, to, _))  =>
        acc.copy(
          createdFields = acc.createdFields + to,
          consumedFields = acc.consumedFields ++ from,
        )
    }
