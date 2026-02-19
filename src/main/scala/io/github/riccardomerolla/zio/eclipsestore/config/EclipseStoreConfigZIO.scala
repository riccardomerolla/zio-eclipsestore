package io.github.riccardomerolla.zio.eclipsestore.config

import java.nio.file.Path

import zio.*
import zio.config.magnolia.{ deriveConfig, deriveConfigFromConfig }
import zio.config.typesafe.TypesafeConfigProvider

import com.typesafe.config.{ Config as HoconConfig, ConfigFactory }

object EclipseStoreConfigZIO:

  final private case class ConfigInput(
    maxParallelism: Option[Int],
    batchSize: Option[Int],
    queryTimeout: Option[zio.Duration],
    performance: Option[StoragePerformanceConfig],
    healthCheckInterval: Option[zio.Duration],
    autoCheckpointInterval: Option[zio.Duration],
    backupDirectory: Option[Path],
    backupTruncationDirectory: Option[Path],
    backupDeletionDirectory: Option[Path],
    backupExternalProperties: Option[Map[String, String]],
  )

  private given zio.Config[Path] =
    zio.Config.string.map(Path.of(_))

  private val compressionConfig: zio.Config[CompressionSetting] =
    zio.Config.string("performance.compression").map {
      case "lz4" | "LZ4"   => CompressionSetting.LZ4
      case "gzip" | "Gzip" => CompressionSetting.Gzip
      case _               => CompressionSetting.Disabled
    }

  private val performanceConfig: zio.Config[StoragePerformanceConfig] =
    (
      zio.Config.int("performance.channelCount").withDefault(4) ++
        zio.Config.long("performance.pageCacheSizeBytes").optional ++
        zio.Config.long("performance.objectCacheSizeBytes").optional ++
        zio.Config.boolean("performance.useOffHeapPageStore").withDefault(false) ++
        compressionConfig.optional ++
        zio.Config.string("performance.encryptionKey").optional.map(_.map(_.getBytes()))
    ).map {
      case (channels, pageCache, objectCache, offHeap, compression, key) =>
        StoragePerformanceConfig(
          channelCount = channels,
          pageCacheSizeBytes = pageCache,
          objectCacheSizeBytes = objectCache,
          useOffHeapPageStore = offHeap,
          compression = compression.getOrElse(CompressionSetting.Disabled),
          encryptionKey = key,
        )
    }

  private given zio.config.magnolia.DeriveConfig[StoragePerformanceConfig] =
    deriveConfigFromConfig(performanceConfig)

  private val defaults = EclipseStoreConfig.temporary

  private val config: zio.Config[ConfigInput] =
    deriveConfig[ConfigInput].nested("eclipsestore")

  private def resolveStorageTarget(config: HoconConfig): StorageTarget =
    val base   = "eclipsestore.storageTarget"
    val sqlite = s"$base.sqlite"
    val fsPath = s"$base.fileSystem.path"
    val mmPath = s"$base.memoryMapped.path"
    val inMem  = s"$base.inMemory.prefix"

    if config.hasPath(fsPath) then StorageTarget.FileSystem(Path.of(config.getString(fsPath)))
    else if config.hasPath(s"$sqlite.path") then
      StorageTarget.Sqlite(
        path = Path.of(config.getString(s"$sqlite.path")),
        storageName =
          if config.hasPath(s"$sqlite.storageName") then config.getString(s"$sqlite.storageName")
          else "eclipsestore.db",
        connectionString =
          if config.hasPath(s"$sqlite.connectionString") then Some(config.getString(s"$sqlite.connectionString"))
          else None,
        busyTimeoutMs =
          if config.hasPath(s"$sqlite.busyTimeoutMs") then Some(config.getInt(s"$sqlite.busyTimeoutMs")) else None,
        cacheSizeKb =
          if config.hasPath(s"$sqlite.cacheSizeKb") then Some(config.getInt(s"$sqlite.cacheSizeKb")) else None,
        pageSizeBytes =
          if config.hasPath(s"$sqlite.pageSizeBytes") then Some(config.getInt(s"$sqlite.pageSizeBytes")) else None,
        journalMode =
          if config.hasPath(s"$sqlite.journalMode") then Some(config.getString(s"$sqlite.journalMode")) else None,
        synchronousMode =
          if config.hasPath(s"$sqlite.synchronousMode") then Some(config.getString(s"$sqlite.synchronousMode"))
          else None,
      )
    else if config.hasPath(mmPath) then StorageTarget.MemoryMapped(Path.of(config.getString(mmPath)))
    else
      val prefix = if config.hasPath(inMem) then config.getString(inMem) else "eclipsestore"
      StorageTarget.InMemory(prefix)

  private def loadConfig(provider: zio.ConfigProvider, hocon: HoconConfig)
    : ZIO[Any, zio.Config.Error, EclipseStoreConfig] =
    for in <- provider.load(config)
    yield EclipseStoreConfig(
      storageTarget = resolveStorageTarget(hocon),
      maxParallelism = in.maxParallelism.getOrElse(defaults.maxParallelism),
      batchSize = in.batchSize.getOrElse(defaults.batchSize),
      queryTimeout = in.queryTimeout.getOrElse(defaults.queryTimeout),
      rootDescriptors = defaults.rootDescriptors,
      performance = in.performance.getOrElse(defaults.performance),
      healthCheckInterval = in.healthCheckInterval.getOrElse(defaults.healthCheckInterval),
      autoCheckpointInterval = in.autoCheckpointInterval,
      backupDirectory = in.backupDirectory,
      backupTruncationDirectory = in.backupTruncationDirectory,
      backupDeletionDirectory = in.backupDeletionDirectory,
      backupExternalProperties = in.backupExternalProperties.getOrElse(Map.empty),
      backupTarget = defaults.backupTarget,
      customTypeHandlers = defaults.customTypeHandlers,
      eagerStoringEvaluator = defaults.eagerStoringEvaluator,
    )

  val fromResourcePath: ZLayer[Any, zio.Config.Error, EclipseStoreConfig] =
    ZLayer.fromZIO(loadConfig(TypesafeConfigProvider.fromResourcePath(), ConfigFactory.load()))

  def fromFile(path: Path): ZLayer[Any, zio.Config.Error, EclipseStoreConfig] =
    ZLayer.fromZIO(
      loadConfig(
        TypesafeConfigProvider.fromHoconFile(path.toFile),
        ConfigFactory.parseFile(path.toFile).resolve(),
      )
    )
