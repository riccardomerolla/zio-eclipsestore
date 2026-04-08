package io.github.riccardomerolla.zio.eclipsestore.bench

import java.nio.file.{ Files, Path }
import java.util.concurrent.TimeUnit

import scala.compiletime.uninitialized
import scala.jdk.CollectionConverters.*

import org.openjdk.jmh.annotations.{ Scope as JmhScope, * }

import zio.*
import zio.schema.{ DeriveSchema, Schema }

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ NativeLocal, ObjectStore, StorageOps }

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class NativeLocalBenchmark:

  import NativeLocalBenchmark.*

  @Benchmark
  def startupFresh(state: StartupState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe
        .run(startupProgram(state.freshSnapshotPath))
        .getOrThrowFiberFailure()
    }

  @Benchmark
  def startupFromSnapshot(state: StartupState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe
        .run(startupProgram(state.seededSnapshotPath))
        .getOrThrowFiberFailure()
    }

  @Benchmark
  def modifyWarm(state: WarmState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe
        .run(
          ObjectStore.modify[BenchRoot, Int](root =>
            ZIO.succeed {
              val next = root.copy(values = root.values :+ root.values.size)
              (next.values.size, next)
            }
          ).provideEnvironment(state.environment)
        )
        .getOrThrowFiberFailure()
    }

  @Benchmark
  def checkpointWarm(state: WarmState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe
        .run(
          StorageOps
            .checkpoint[BenchRoot]
            .provideEnvironment(state.environment)
            .as(state.snapshotSize())
        )
        .getOrThrowFiberFailure()
    }

  @Benchmark
  def restoreFromSnapshot(state: RestoreState): Int =
    Unsafe.unsafe { implicit unsafe =>
      state.runtime.unsafe
        .run(restoreProgram(state.baseSnapshotPath, state.importSnapshotPath))
        .getOrThrowFiberFailure()
    }

object NativeLocalBenchmark:
  final case class BenchRoot(values: Chunk[Int])

  given Schema[BenchRoot] = DeriveSchema.gen[BenchRoot]
  given Tag[BenchRoot]    = Tag.derived

  private val descriptor =
    RootDescriptor.fromSchema[BenchRoot]("native-local-bench-root", () => BenchRoot(Chunk.empty))

  @State(JmhScope.Benchmark)
  class StartupState:
    val runtime: Runtime[Any] = Runtime.default

    private var directory: Path = uninitialized

    @Setup(Level.Trial)
    def setup(): Unit =
      directory = Files.createTempDirectory("native-local-startup-bench")
      Unsafe.unsafe { implicit unsafe =>
        runtime.unsafe.run(seedSnapshot(seededSnapshotPath, 1024)).getOrThrowFiberFailure()
      }

    @TearDown(Level.Trial)
    def tearDown(): Unit =
      deleteDirectory(directory)

    def freshSnapshotPath: Path =
      directory.resolve("fresh.snapshot.json")

    def seededSnapshotPath: Path =
      directory.resolve("seeded.snapshot.json")

  @State(JmhScope.Benchmark)
  class WarmState:
    val runtime: Runtime[Any] = Runtime.default

    private var directory0: Path = uninitialized
    private var scope0: zio.Scope.Closeable = uninitialized
    private var environment0: ZEnvironment[ObjectStore[BenchRoot] & StorageOps[BenchRoot]] = uninitialized

    @Setup(Level.Trial)
    def setup(): Unit =
      directory0 = Files.createTempDirectory("native-local-warm-bench")
      Unsafe.unsafe { implicit unsafe =>
        runtime.unsafe.run(seedSnapshot(snapshotPath, 1024)).getOrThrowFiberFailure()
        scope0 = runtime.unsafe.run(zio.Scope.make).getOrThrowFiberFailure()
        environment0 = runtime.unsafe
          .run(
            NativeLocal
              .live(snapshotPath, descriptor)
              .build
              .provideEnvironment(ZEnvironment(scope0))
              .mapError(toRuntimeException)
          )
          .getOrThrowFiberFailure()
      }

    @TearDown(Level.Trial)
    def tearDown(): Unit =
      Unsafe.unsafe { implicit unsafe =>
        runtime.unsafe.run(scope0.close(Exit.unit)).getOrThrowFiberFailure()
      }
      deleteDirectory(directory0)

    def environment: ZEnvironment[ObjectStore[BenchRoot] & StorageOps[BenchRoot]] =
      environment0

    def snapshotPath: Path =
      directory0.resolve("warm.snapshot.json")

    def snapshotSize(): Int =
      if Files.exists(snapshotPath) then Files.size(snapshotPath).toInt else 0

  @State(JmhScope.Benchmark)
  class RestoreState:
    val runtime: Runtime[Any] = Runtime.default

    private var directory: Path = uninitialized

    @Setup(Level.Trial)
    def setup(): Unit =
      directory = Files.createTempDirectory("native-local-restore-bench")
      Unsafe.unsafe { implicit unsafe =>
        runtime.unsafe.run(seedSnapshot(importSnapshotPath, 2048)).getOrThrowFiberFailure()
      }

    @TearDown(Level.Trial)
    def tearDown(): Unit =
      deleteDirectory(directory)

    def baseSnapshotPath: Path =
      directory.resolve("base.snapshot.json")

    def importSnapshotPath: Path =
      directory.resolve("import.snapshot.json")

  private def startupProgram(path: Path): ZIO[Any, RuntimeException, Int] =
    ZIO.scoped {
      for
        env  <- NativeLocal.live(path, descriptor).build
        root <- ObjectStore.load[BenchRoot].provideEnvironment(env)
      yield root.values.size
    }.mapError(toRuntimeException)

  private def restoreProgram(basePath: Path, importPath: Path): ZIO[Any, RuntimeException, Int] =
    ZIO.scoped {
      for
        env      <- NativeLocal.live(basePath, descriptor).build
        _        <- StorageOps.importFrom[BenchRoot](importPath).provideEnvironment(env)
        _        <- StorageOps.restart[BenchRoot].provideEnvironment(env)
        restored <- ObjectStore.load[BenchRoot].provideEnvironment(env)
      yield restored.values.size
    }.mapError(toRuntimeException)

  private def seedSnapshot(path: Path, size: Int): ZIO[Any, RuntimeException, Unit] =
    ZIO.scoped {
      for
        env <- NativeLocal.live(path, descriptor).build
        _   <- ObjectStore.replace(BenchRoot(Chunk.fromIterable(0 until size))).provideEnvironment(env)
        _   <- StorageOps.checkpoint[BenchRoot].provideEnvironment(env)
      yield ()
    }.mapError(toRuntimeException)

  private def toRuntimeException(error: EclipseStoreError): RuntimeException =
    RuntimeException(error.toString)

  private def deleteDirectory(path: Path): Unit =
    if path != null && Files.exists(path) then
      Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
