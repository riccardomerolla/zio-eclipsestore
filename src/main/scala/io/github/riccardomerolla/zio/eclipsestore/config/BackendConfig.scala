package io.github.riccardomerolla.zio.eclipsestore.config

import java.nio.file.Path

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

/** Typed backend-selection surface for layer-based storage acquisition. */
sealed trait BackendConfig:
  def name: String

  def toStorageTarget: Either[EclipseStoreError, StorageTarget] =
    this match
      case nativeLocal: BackendConfig.NativeLocal =>
        Left(
          EclipseStoreError.BackendError(
            s"NativeLocal backend at ${nativeLocal.snapshotPath} does not map to an EclipseStore StorageTarget",
            None,
          )
        )
      case BackendConfig.FileSystem(path)         =>
        Right(StorageTarget.FileSystem(path))
      case BackendConfig.MemoryMapped(path)       =>
        Right(StorageTarget.MemoryMapped(path))
      case BackendConfig.InMemory(prefix)         =>
        Right(StorageTarget.InMemory(prefix))
      case sqlite: BackendConfig.Sqlite           =>
        Right(
          StorageTarget.Sqlite(
            path = sqlite.path,
            storageName = sqlite.storageName,
            connectionString = sqlite.connectionString,
            busyTimeoutMs = sqlite.busyTimeoutMs,
            cacheSizeKb = sqlite.cacheSizeKb,
            pageSizeBytes = sqlite.pageSizeBytes,
            journalMode = sqlite.journalMode,
            synchronousMode = sqlite.synchronousMode,
          )
        )
      case s3: BackendConfig.S3                   =>
        if s3.accessKey.isEmpty || s3.secretKey.isEmpty then
          Left(EclipseStoreError.AuthError(s"S3 backend '${s3.bucket}' requires accessKey and secretKey", None))
        else Left(EclipseStoreError.BackendError("S3 backend wiring is not implemented yet", None))
      case azure: BackendConfig.AzureBlob         =>
        if azure.connectionString.isEmpty && (azure.accountName.isEmpty || azure.accountKey.isEmpty) then
          Left(
            EclipseStoreError.AuthError(
              s"Azure backend '${azure.container}' requires connectionString or accountName/accountKey",
              None,
            )
          )
        else Left(EclipseStoreError.BackendError("Azure Blob backend wiring is not implemented yet", None))
      case redis: BackendConfig.Redis             =>
        if redis.uri.trim.isEmpty then Left(EclipseStoreError.AuthError("Redis backend requires a non-empty URI", None))
        else Left(EclipseStoreError.BackendError("Redis backend wiring is not implemented yet", None))

object BackendConfig:
  final case class NativeLocal(
    snapshotPath: Path,
    serde: NativeLocalSerde = NativeLocalSerde.Json,
    startupPolicy: NativeLocalStartupPolicy = NativeLocalStartupPolicy.default,
  ) extends BackendConfig:
    override val name: String = "native-local"

  final case class FileSystem(path: Path) extends BackendConfig:
    override val name: String = "filesystem"

  final case class MemoryMapped(path: Path) extends BackendConfig:
    override val name: String = "memory-mapped"

  final case class InMemory(prefix: String = "eclipsestore") extends BackendConfig:
    override val name: String = "in-memory"

  final case class Sqlite(
    path: Path,
    storageName: String = "eclipsestore.db",
    connectionString: Option[String] = None,
    busyTimeoutMs: Option[Int] = None,
    cacheSizeKb: Option[Int] = None,
    pageSizeBytes: Option[Int] = None,
    journalMode: Option[String] = None,
    synchronousMode: Option[String] = None,
  ) extends BackendConfig:
    override val name: String = "sqlite"

  final case class S3(
    bucket: String,
    prefix: Option[String] = None,
    region: Option[String] = None,
    accessKey: Option[String] = None,
    secretKey: Option[String] = None,
  ) extends BackendConfig:
    override val name: String = "s3"

  final case class AzureBlob(
    container: String,
    prefix: Option[String] = None,
    connectionString: Option[String] = None,
    accountName: Option[String] = None,
    accountKey: Option[String] = None,
  ) extends BackendConfig:
    override val name: String = "azure-blob"

  final case class Redis(
    uri: String,
    namespace: Option[String] = None,
  ) extends BackendConfig:
    override val name: String = "redis"

  def fromStorageTarget(target: StorageTarget): Either[EclipseStoreError, BackendConfig] =
    target match
      case StorageTarget.FileSystem(path)   =>
        Right(FileSystem(path))
      case StorageTarget.MemoryMapped(path) =>
        Right(MemoryMapped(path))
      case StorageTarget.InMemory(prefix)   =>
        Right(InMemory(prefix))
      case sqlite: StorageTarget.Sqlite     =>
        Right(
          Sqlite(
            path = sqlite.path,
            storageName = sqlite.storageName,
            connectionString = sqlite.connectionString,
            busyTimeoutMs = sqlite.busyTimeoutMs,
            cacheSizeKb = sqlite.cacheSizeKb,
            pageSizeBytes = sqlite.pageSizeBytes,
            journalMode = sqlite.journalMode,
            synchronousMode = sqlite.synchronousMode,
          )
        )
      case _: StorageTarget.Custom          =>
        Left(EclipseStoreError.BackendError("Custom storage targets cannot be decoded into BackendConfig", None))
