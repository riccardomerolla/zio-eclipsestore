package io.github.riccardomerolla.zio.eclipsestore.config

enum MissingSnapshotPolicy:
  case InitializeFromDescriptor
  case RequireExistingSnapshot

enum CorruptSnapshotPolicy:
  case Fail
  case StartFromEmpty

final case class NativeLocalStartupPolicy(
  missingSnapshot: MissingSnapshotPolicy = MissingSnapshotPolicy.InitializeFromDescriptor,
  corruptSnapshot: CorruptSnapshotPolicy = CorruptSnapshotPolicy.Fail,
)

object NativeLocalStartupPolicy:
  val default: NativeLocalStartupPolicy =
    NativeLocalStartupPolicy()

  val startFromEmptyOnMissingSnapshot: NativeLocalStartupPolicy =
    NativeLocalStartupPolicy(missingSnapshot = MissingSnapshotPolicy.InitializeFromDescriptor)

  val requireExistingSnapshot: NativeLocalStartupPolicy =
    NativeLocalStartupPolicy(missingSnapshot = MissingSnapshotPolicy.RequireExistingSnapshot)

  val failOnCorruptSnapshot: NativeLocalStartupPolicy =
    NativeLocalStartupPolicy(corruptSnapshot = CorruptSnapshotPolicy.Fail)

  val startFromEmptyOnCorruptSnapshot: NativeLocalStartupPolicy =
    NativeLocalStartupPolicy(corruptSnapshot = CorruptSnapshotPolicy.StartFromEmpty)
