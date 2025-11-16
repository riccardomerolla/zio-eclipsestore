package io.github.riccardomerolla.zio.eclipsestore.config

import java.nio.file.Path
import zio.{Duration, ZLayer}

/** Configuration for EclipseStore instance */
final case class EclipseStoreConfig(
  /** Storage directory path */
  storagePath: Path,
  
  /** Maximum number of parallel operations for un-batchable queries */
  maxParallelism: Int = 10,
  
  /** Batch size for query batching */
  batchSize: Int = 100,
  
  /** Timeout for query execution */
  queryTimeout: Duration = Duration.fromSeconds(30),
  
  /** Number of channels for storing data */
  channelCount: Int = 4
)

object EclipseStoreConfig:
  /** Creates a default configuration with the specified storage path */
  def make(storagePath: Path): EclipseStoreConfig =
    EclipseStoreConfig(storagePath = storagePath)
  
  /** Creates a default configuration with a temporary storage path */
  def temporary: EclipseStoreConfig =
    val tempPath = java.nio.file.Files.createTempDirectory("eclipsestore")
    EclipseStoreConfig(storagePath = tempPath)
  
  /** ZLayer that provides a default temporary configuration */
  val temporaryLayer: ZLayer[Any, Nothing, EclipseStoreConfig] =
    ZLayer.succeed(temporary)
  
  /** ZLayer that provides a configuration with the specified storage path */
  def layer(storagePath: Path): ZLayer[Any, Nothing, EclipseStoreConfig] =
    ZLayer.succeed(make(storagePath))
