package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.Files

import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.stm.STM
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ NativeLocal, NativeLocalSTM, ObjectStore, StorageOps }

object NativeLocalSTMSpec extends ZIOSpecDefault:

  final case class CounterRoot(total: Int, labels: Chunk[String])

  given Schema[CounterRoot] = DeriveSchema.gen[CounterRoot]
  given Tag[CounterRoot]    = Tag.derived

  private val descriptor =
    RootDescriptor.fromSchema[CounterRoot](
      id = "native-local-stm-root",
      initializer = () => CounterRoot(0, Chunk.empty),
    )

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("NativeLocalSTM")(
      test("commits STM updates into the shared NativeLocal root and survives restart") {
        ZIO.scoped {
          for
            path <- ZIO.attemptBlocking(Files.createTempFile("native-local-stm", ".json"))
            _    <- ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore
            _    <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
            layer = NativeLocal.liveWithSTM(path, descriptor)
            out  <- (for
                      _ <- NativeLocalSTM.atomically[CounterRoot, Unit](_.update(root =>
                             root.copy(total = root.total + 1, labels = root.labels :+ "stm-1")
                           ))
                      _ <- NativeLocalSTM.atomically[CounterRoot, Unit](_.update(root =>
                             root.copy(total = root.total + 2, labels = root.labels :+ "stm-2")
                           ))
                      _ <- StorageOps.checkpoint[CounterRoot]
                      _ <- StorageOps.restart[CounterRoot]
                      s <- NativeLocalSTM.snapshot[CounterRoot]
                    yield s).provideLayer(layer)
          yield assertTrue(
            out.total == 3,
            out.labels == Chunk("stm-1", "stm-2"),
          )
        }
      },
      test("failed STM transactions leave the NativeLocal root unchanged") {
        ZIO.scoped {
          for
            path              <- ZIO.attemptBlocking(Files.createTempFile("native-local-stm-failure", ".json"))
            _                 <- ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore
            _                 <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
            layer              = NativeLocal.liveWithSTM(path, descriptor)
            out               <- (for
                                   _      <- NativeLocalSTM.atomically[CounterRoot, Unit](_.update(root =>
                                               root.copy(total = 1, labels = Chunk("ok"))
                                             ))
                                   failed <- NativeLocalSTM
                                               .atomically[CounterRoot, Unit] { ref =>
                                                 for
                                                   _ <- ref.update(root => root.copy(total = 999, labels = root.labels :+ "boom"))
                                                   _ <- STM.fail(
                                                          EclipseStoreError.QueryError("synthetic STM failure", None)
                                                        )
                                                 yield ()
                                               }
                                               .either
                                   s      <- NativeLocalSTM.snapshot[CounterRoot]
                                 yield (failed, s)).provideLayer(layer)
            (failed, snapshot) = out
          yield assertTrue(
            failed == Left(EclipseStoreError.QueryError("synthetic STM failure", None)),
            snapshot == CounterRoot(1, Chunk("ok")),
          )
        }
      },
      test("ObjectStore and NativeLocalSTM observe the same shared root state") {
        ZIO.scoped {
          for
            path                    <- ZIO.attemptBlocking(Files.createTempFile("native-local-stm-shared-root", ".json"))
            _                       <- ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore
            _                       <- ZIO.addFinalizer(ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
            layer                    = NativeLocal.liveWithSTM(path, descriptor)
            out                     <- (for
                                         _ <- ObjectStore.replace[CounterRoot](CounterRoot(2, Chunk("object-store")))
                                         a <- NativeLocalSTM.snapshot[CounterRoot]
                                         _ <- NativeLocalSTM.atomically[CounterRoot, Unit](_.update(root =>
                                                root.copy(total = root.total + 5, labels = root.labels :+ "stm")
                                              ))
                                         b <- ObjectStore.load[CounterRoot]
                                       yield (a, b)).provideLayer(layer)
            (afterReplace, afterStm) = out
          yield assertTrue(
            afterReplace == CounterRoot(2, Chunk("object-store")),
            afterStm == CounterRoot(7, Chunk("object-store", "stm")),
          )
        }
      },
    )
