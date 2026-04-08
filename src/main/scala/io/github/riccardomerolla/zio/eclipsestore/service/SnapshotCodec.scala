package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Path, StandardCopyOption, StandardOpenOption }

import zio.*
import zio.schema.Schema
import zio.schema.codec.JsonCodec

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

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
  def json[A: Schema]: SnapshotCodec[A] =
    JsonSnapshotCodec[A]()

  def save[A](path: Path, value: A)(using codec: SnapshotCodec[A]): IO[EclipseStoreError, Unit] =
    for
      bytes <- codec.encode(value)
      _     <- ZIO
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
