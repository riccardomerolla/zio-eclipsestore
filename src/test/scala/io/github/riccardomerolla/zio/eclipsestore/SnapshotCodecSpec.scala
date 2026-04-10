package io.github.riccardomerolla.zio.eclipsestore

import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.SnapshotCodec

object SnapshotCodecSpec extends ZIOSpecDefault:

  final case class Nested(value: Int, label: String)
  final case class SnapshotRoot(items: Chunk[Nested])

  given Schema[Nested]       = DeriveSchema.gen[Nested]
  given Schema[SnapshotRoot] = DeriveSchema.gen[SnapshotRoot]

  private val root =
    SnapshotRoot(
      Chunk(
        Nested(1, "alpha"),
        Nested(2, "beta"),
      )
    )

  override def spec: Spec[TestEnvironment, Any] =
    suite("SnapshotCodec")(
      test("JSON codec round-trips a whole root") {
        for
          encoded <- SnapshotCodec.json[SnapshotRoot].encode(root)
          decoded <- SnapshotCodec.json[SnapshotRoot].decode(encoded)
        yield assertTrue(decoded == root)
      },
      test("protobuf codec round-trips a whole root") {
        for
          encoded <- SnapshotCodec.protobuf[SnapshotRoot].encode(root)
          decoded <- SnapshotCodec.protobuf[SnapshotRoot].decode(encoded)
        yield assertTrue(decoded == root)
      },
      test("protobuf codec fails with a typed schema error on invalid bytes") {
        SnapshotCodec.protobuf[SnapshotRoot].decode(Chunk.fromArray("not-protobuf".getBytes)).either.map {
          case Left(EclipseStoreError.IncompatibleSchemaError(message, _)) =>
            assertTrue(message.contains("Failed to decode NativeLocal protobuf snapshot payload"))
          case other                                                       =>
            assertTrue(other.isLeft)
        }
      },
    )
