package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Path, StandardCopyOption, StandardOpenOption }

import zio.*
import zio.schema.Schema
import zio.schema.codec.{ BinaryCodec, JsonCodec, ProtobufCodec }

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.schema.SchemaIntrospection

/** Schema-driven whole-root snapshot codec used by the NativeLocal backend. */
trait SnapshotCodec[A]:
  def encode(value: A): IO[EclipseStoreError, Chunk[Byte]]
  def decode(bytes: Chunk[Byte]): IO[EclipseStoreError, A]

final case class JsonSnapshotCodec[A: Schema]() extends SnapshotCodec[A]:
  private val codec = JsonCodec.jsonCodec(summon[Schema[A]])

  override def encode(value: A): IO[EclipseStoreError, Chunk[Byte]] =
    ZIO
      .attempt(Chunk.fromArray(codec.encodeJson(value, None).toString.getBytes(StandardCharsets.UTF_8)))
      .mapError(cause => EclipseStoreError.StorageError("Failed to encode NativeLocal snapshot", Some(cause)))

  override def decode(bytes: Chunk[Byte]): IO[EclipseStoreError, A] =
    ZIO
      .attempt(new String(bytes.toArray, StandardCharsets.UTF_8))
      .mapError(cause => EclipseStoreError.StorageError("Failed to read NativeLocal snapshot bytes", Some(cause)))
      .flatMap { payload =>
        ZIO
          .fromEither(codec.decodeJson(payload))
          .mapError(error =>
            EclipseStoreError.IncompatibleSchemaError(
              s"Failed to decode NativeLocal snapshot payload: $error",
              None,
            )
          )
      }

object SnapshotCodec:
  final case class SnapshotLoadResult[A](value: A, rewriteRequired: Boolean)

  def json[A: Schema]: SnapshotCodec[A] =
    JsonSnapshotCodec[A]()

  def protobuf[A: Schema]: SnapshotCodec[A] =
    ProtobufSnapshotCodec[A]()

  def forSerde[A: Schema](serde: NativeLocalSerde): SnapshotCodec[A] =
    serde match
      case NativeLocalSerde.Json     => json[A]
      case NativeLocalSerde.Protobuf => protobuf[A]

  def encodePayload[A: Schema](value: A, serde: NativeLocalSerde): IO[EclipseStoreError, Chunk[Byte]] =
    forSerde[A](serde).encode(value)

  def decodePayload[A: Schema](bytes: Chunk[Byte], serde: NativeLocalSerde): IO[EclipseStoreError, A] =
    forSerde[A](serde).decode(bytes)

  def saveEnveloped[A: Schema](
    path: Path,
    value: A,
    rootId: String,
    serde: NativeLocalSerde,
    schemaVersion: Option[Int] = None,
  ): IO[EclipseStoreError, Unit] =
    for
      payload <- encodePayload(value, serde)
      envelope = NativeLocalSnapshotEnvelope.current[A](rootId, payload, schemaVersion)
      bytes   <- encodePayload(envelope, serde)
      _       <- writeBytes(path, bytes)
    yield ()

  def save[A](path: Path, value: A)(using codec: SnapshotCodec[A]): IO[EclipseStoreError, Unit] =
    for
      bytes <- codec.encode(value)
      _     <- writeBytes(path, bytes)
    yield ()

  def load[A](path: Path)(using codec: SnapshotCodec[A]): IO[EclipseStoreError, A] =
    ZIO
      .attemptBlocking(Chunk.fromArray(Files.readAllBytes(path)))
      .mapError(cause => EclipseStoreError.StorageError(s"Failed to read NativeLocal snapshot from $path", Some(cause)))
      .flatMap(codec.decode)

  def loadOrElse[A](path: Path, orElse: => A)(using codec: SnapshotCodec[A]): IO[EclipseStoreError, A] =
    ZIO
      .attemptBlocking(Files.exists(path))
      .mapError(cause =>
        EclipseStoreError.StorageError(s"Failed to inspect NativeLocal snapshot path $path", Some(cause))
      )
      .flatMap {
        case true  => load(path)
        case false => ZIO.succeed(orElse)
      }

  def copy(source: Path, target: Path): IO[EclipseStoreError, Unit] =
    ZIO
      .attemptBlocking {
        Option(target.getParent).foreach(Files.createDirectories(_))
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
      }
      .unit
      .mapError(cause =>
        EclipseStoreError.StorageError(s"Failed to copy NativeLocal snapshot from $source to $target", Some(cause))
      )

  def loadEnvelopedOrElse[A: Schema](
    path: Path,
    rootId: String,
    serde: NativeLocalSerde,
    orElse: => A,
    migrationRegistry: NativeLocalSnapshotMigrationRegistry[A] = NativeLocalSnapshotMigrationRegistry.none[A],
  ): IO[EclipseStoreError, SnapshotLoadResult[A]] =
    ZIO
      .attemptBlocking(Files.exists(path))
      .mapError(cause =>
        EclipseStoreError.StorageError(s"Failed to inspect NativeLocal snapshot path $path", Some(cause))
      )
      .flatMap {
        case false => ZIO.succeed(SnapshotLoadResult(orElse, rewriteRequired = false))
        case true  =>
          for
            bytes           <- ZIO
                                 .attemptBlocking(Chunk.fromArray(Files.readAllBytes(path)))
                                 .mapError(cause =>
                                   EclipseStoreError.StorageError(
                                     s"Failed to read NativeLocal snapshot from $path",
                                     Some(cause),
                                   )
                                 )
            envelopeAttempt <- decodePayload[NativeLocalSnapshotEnvelope](bytes, serde).either
            loaded          <- envelopeAttempt match
                                 case Right(envelope) =>
                                   if envelope.rootId != rootId then
                                     ZIO.fail(
                                       EclipseStoreError.IncompatibleSchemaError(
                                         s"NativeLocal snapshot root id '${envelope.rootId}' does not match expected root '$rootId'",
                                         None,
                                       )
                                     )
                                   else if envelope.schemaFingerprint == SchemaIntrospection.fingerprint(summon[Schema[A]]) then
                                     decodePayload[A](
                                       envelope.payload,
                                       serde,
                                     ).map(SnapshotLoadResult(_, rewriteRequired = false))
                                   else
                                     migrationRegistry
                                       .migrateOrFail(envelope, serde)
                                       .map(SnapshotLoadResult(_, rewriteRequired = true))
                                 case Left(_)         =>
                                   decodePayload[A](bytes, serde).map(SnapshotLoadResult(_, rewriteRequired = true))
          yield loaded
      }

  private def writeBytes(path: Path, bytes: Chunk[Byte]): IO[EclipseStoreError, Unit] =
    ZIO
      .attemptBlocking {
        Option(path.getParent).foreach(Files.createDirectories(_))
        Files.write(
          path,
          bytes.toArray,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE,
        )
      }
      .unit
      .mapError(cause =>
        EclipseStoreError.StorageError(s"Failed to write NativeLocal snapshot to $path", Some(cause))
      )

final case class ProtobufSnapshotCodec[A: Schema]() extends SnapshotCodec[A]:
  private val codec: BinaryCodec[A] = ProtobufCodec.protobufCodec(summon[Schema[A]])

  override def encode(value: A): IO[EclipseStoreError, Chunk[Byte]] =
    ZIO
      .attempt(codec.encode(value))
      .mapError(cause => EclipseStoreError.StorageError("Failed to encode NativeLocal protobuf snapshot", Some(cause)))

  override def decode(bytes: Chunk[Byte]): IO[EclipseStoreError, A] =
    ZIO
      .fromEither(codec.decode(bytes))
      .mapError(error =>
        EclipseStoreError.IncompatibleSchemaError(
          s"Failed to decode NativeLocal protobuf snapshot payload: $error",
          None,
        )
      )
