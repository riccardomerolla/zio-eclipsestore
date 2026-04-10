package io.github.riccardomerolla.zio.eclipsestore.service

import zio.Chunk
import zio.schema.{ DeriveSchema, Schema }

import io.github.riccardomerolla.zio.eclipsestore.schema.SchemaIntrospection

final case class NativeLocalSnapshotEnvelope(
  formatVersion: Int,
  rootId: String,
  schemaFingerprint: String,
  schemaVersion: Option[Int],
  payload: Chunk[Byte],
)

object NativeLocalSnapshotEnvelope:
  val CurrentFormatVersion: Int = 1

  given Schema[NativeLocalSnapshotEnvelope] = DeriveSchema.gen[NativeLocalSnapshotEnvelope]

  def current[Root: Schema](
    rootId: String,
    payload: Chunk[Byte],
    schemaVersion: Option[Int] = None,
  ): NativeLocalSnapshotEnvelope =
    NativeLocalSnapshotEnvelope(
      formatVersion = CurrentFormatVersion,
      rootId = rootId,
      schemaFingerprint = SchemaIntrospection.fingerprint(summon[Schema[Root]]),
      schemaVersion = schemaVersion,
      payload = payload,
    )
