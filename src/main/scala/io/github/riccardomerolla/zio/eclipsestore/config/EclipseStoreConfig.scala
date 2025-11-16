package io.github.riccardomerolla.zio.eclipsestore.config

import zio.{ Chunk, Duration, ZLayer }

import java.nio.file.{ Files, Path }

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor.concurrentMap
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation

/** Storage target supported by EclipseStore */
sealed trait StorageTarget:
  def storagePath: Option[Path]
  def buildFoundation(): EmbeddedStorageFoundation[?]

object StorageTarget:
  final case class FileSystem(path: Path) extends StorageTarget:
    override val storagePath: Option[Path]                       = Some(path)
    override def buildFoundation(): EmbeddedStorageFoundation[?] =
      org.eclipse.store.storage.embedded.types.EmbeddedStorage.Foundation(path)

  final case class MemoryMapped(path: Path) extends StorageTarget:
    override val storagePath: Option[Path]                       = Some(path)
    override def buildFoundation(): EmbeddedStorageFoundation[?] =
      org.eclipse.store.storage.embedded.types.EmbeddedStorage.Foundation(path)

  final case class InMemory(prefix: String = "eclipsestore") extends StorageTarget:
    override val storagePath: Option[Path]                       = None
    override def buildFoundation(): EmbeddedStorageFoundation[?] =
      val directory = Files.createTempDirectory(prefix)
      org.eclipse.store.storage.embedded.types.EmbeddedStorage.Foundation(directory)

  final case class Custom(
      build: () => EmbeddedStorageFoundation[?],
      override val storagePath: Option[Path],
    ) extends StorageTarget:
    override def buildFoundation(): EmbeddedStorageFoundation[?] = build()

enum CompressionSetting:
  case Disabled, LZ4, Gzip

final case class StoragePerformanceConfig(
    channelCount: Int = 4,
    pageCacheSizeBytes: Option[Long] = None,
    objectCacheSizeBytes: Option[Long] = None,
    useOffHeapPageStore: Boolean = false,
    compression: CompressionSetting = CompressionSetting.Disabled,
    encryptionKey: Option[Array[Byte]] = None,
  )

/** Configuration for EclipseStore instance */
final case class EclipseStoreConfig(
    storageTarget: StorageTarget,
    maxParallelism: Int = 10,
    batchSize: Int = 100,
    queryTimeout: Duration = Duration.fromSeconds(30),
    rootDescriptors: Chunk[RootDescriptor[?]] = Chunk.single(concurrentMap[Any, Any]("kv-root")),
    performance: StoragePerformanceConfig = StoragePerformanceConfig(),
    healthCheckInterval: Duration = Duration.fromSeconds(5),
    autoCheckpointInterval: Option[Duration] = None,
  )

object EclipseStoreConfig:
  def make(storagePath: Path): EclipseStoreConfig =
    EclipseStoreConfig(storageTarget = StorageTarget.FileSystem(storagePath))

  def temporary: EclipseStoreConfig =
    EclipseStoreConfig(StorageTarget.InMemory())

  val temporaryLayer: ZLayer[Any, Nothing, EclipseStoreConfig] =
    ZLayer.succeed(temporary)

  def layer(storagePath: Path): ZLayer[Any, Nothing, EclipseStoreConfig] =
    ZLayer.succeed(make(storagePath))
