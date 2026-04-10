package io.github.riccardomerolla.zio.eclipsestore.service

import java.nio.file.Path
import java.time.Instant

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ NativeLocalSerde, NativeLocalStartupPolicy }

final case class NativeLocalStatus(
  lifecycle: LifecycleStatus,
  snapshotPath: Path,
  serde: NativeLocalSerde,
  startupPolicy: NativeLocalStartupPolicy,
  lastSuccessfulCheckpointAt: Option[Instant],
  lastLoadedSchemaFingerprint: String,
  snapshotBytes: Long,
)

trait NativeLocalInspector[Root]:
  def status: UIO[NativeLocalStatus]

object NativeLocalInspector:
  def status[Root: Tag]: ZIO[NativeLocalInspector[Root], Nothing, NativeLocalStatus] =
    ZIO.serviceWithZIO[NativeLocalInspector[Root]](_.status)
