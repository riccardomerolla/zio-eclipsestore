package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.{ Files, Path }
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.stm.STM
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.schema.TypedStore
import io.github.riccardomerolla.zio.eclipsestore.service.{
  EclipseStoreService,
  NativeLocalSTM,
  ObjectStore,
  StorageOps,
  Transaction,
}
import io.github.riccardomerolla.zio.eclipsestore.testkit.*

object TestkitSpec extends ZIOSpecDefault:
  final case class NativeEventLog(events: Chunk[PersistenceGenerators.MessageEvent])

  given Schema[NativeEventLog] = DeriveSchema.gen[NativeEventLog]
  given Tag[NativeEventLog]    = Tag.derived

  private val mapDescriptor =
    RootDescriptor.concurrentMap[String, String]("testkit-root")

  private val nativeEventLogDescriptor =
    RootDescriptor.fromSchema[NativeEventLog]("native-event-log", () => NativeEventLog(Chunk.empty))

  private def deleteDirectory(path: Path): Unit =
    if Files.exists(path) then Files.walk(path).sorted(Comparator.reverseOrder()).forEach(Files.delete)

  private def liveObjectStoreLayer(path: Path) =
    ZLayer.succeed(EclipseStoreConfig.make(path).copy(rootDescriptors = Chunk.single(mapDescriptor))) >>>
      EclipseStoreService.live >>>
      ObjectStore.live(mapDescriptor)

  private def inMemoryObjectStoreLayer =
    InMemoryObjectStore.layer(mapDescriptor)

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Testkit")(
      BddScenario
        .scenario("PersistenceSpec parity")
        .`given`("a live and in-memory object store contract")(
          ZIO.unit
        )
        .when("the same read/write program runs through both implementations") {
          ZIO.scoped {
            for
              path <- ZIO.attemptBlocking(Files.createTempDirectory("testkit-live-store")).orDie
              _    <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
              live <- ObjectStore
                        .transact[ConcurrentHashMap[String, String], Map[String, String]](
                          Transaction.effect(root =>
                            ZIO.succeed {
                              root.put("alpha", "1")
                              root.put("beta", "2")
                              root.asScala.toMap
                            }
                          )
                        )
                        .provideLayer(liveObjectStoreLayer(path))
              mem  <- ObjectStore
                        .transact[ConcurrentHashMap[String, String], Map[String, String]](
                          Transaction.effect(root =>
                            ZIO.succeed {
                              root.put("alpha", "1")
                              root.put("beta", "2")
                              root.asScala.toMap
                            }
                          )
                        )
                        .provideLayer(inMemoryObjectStoreLayer)
            yield (live, mem)
          }
        }
        .thenAssert("data persists according to the same behavioral contract") {
          case (live, mem) =>
            assertTrue(
              live == mem,
              live == Map("alpha" -> "1", "beta" -> "2"),
            )
        }
        .spec,
      test("fault injector cleans up and reports configured failures") {
        ZIO.scoped {
          for
            injector  <- FaultInjector.inMemory.build.map(_.get[FaultInjector])
            cleaned   <- Ref.make(false)
            _         <- FaultInjector
                           .failNext(
                             operation = "backup",
                             error = EclipseStoreError.ResourceError("boom", None),
                             cleanup = cleaned.set(true),
                           )
                           .provideEnvironment(ZEnvironment(injector))
            result    <- FaultInjector
                           .inject("backup")(ZIO.succeed("ok"))
                           .provideEnvironment(ZEnvironment(injector))
                           .either
            triggered <- FaultInjector.triggeredOperations.provideEnvironment(ZEnvironment(injector))
            cleanuped <- cleaned.get
          yield assertTrue(
            result == Left(EclipseStoreError.ResourceError("boom", None)),
            cleanuped,
            triggered == Chunk("backup"),
          )
        }
      },
      test("schema-derived fixtures satisfy the same typed store contract in-memory and live") {
        check(PersistenceGenerators.messageEvent) { event =>
          ZIO.scoped {
            for
              path      <- ZIO.attemptBlocking(Files.createTempDirectory("testkit-typed-store")).orDie
              _         <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
              liveLayer  =
                ZLayer.succeed(EclipseStoreConfig.make(path)) >>>
                  TypedStore.handlersFor[String, PersistenceGenerators.MessageEvent] >>>
                  EclipseStoreService.live >>>
                  TypedStore.live
              memLayer   =
                EclipseStoreService.inMemory >>> TypedStore.live
              liveSpec   =
                for {
                  _      <- TypedStore.store("evt", event)
                  loaded <- TypedStore.fetch[String, PersistenceGenerators.MessageEvent]("evt")
                } yield loaded
              memorySpec =
                for {
                  _      <- TypedStore.store("evt", event)
                  loaded <- TypedStore.fetch[String, PersistenceGenerators.MessageEvent]("evt")
                } yield loaded
              live      <- liveSpec.provideLayer(liveLayer)
              inMemory  <- memorySpec.provideLayer(memLayer)
            yield assertTrue(live == inMemory, live.contains(event))
          }
        }
      },
      PersistenceSpec.parity("generic parity helper supports shared contract suites")(
        ZIO.succeed(Chunk("live", "contract")),
        ZIO.succeed(Chunk("live", "contract")),
      ) { (live, inMemory) =>
        assertTrue(live == inMemory)
      },
      test("NativeLocal temp layer supports model-vs-engine immutable root sequences") {
        check(Gen.chunkOfBounded(1, 8)(PersistenceGenerators.messageEvent)) { events =>
          val expected = NativeEventLog(events)

          PersistenceSpec.compare(
            live = ZIO.scoped {
              for
                env <- NativeLocalObjectStore.tempLayer(nativeEventLogDescriptor).build
                r   <- (for
                         _ <- ZIO.foreachDiscard(events) { event =>
                                ObjectStore.modify[NativeEventLog, Unit](root =>
                                  ZIO.succeed(((), root.copy(events = root.events :+ event)))
                                )
                              }
                         _ <- StorageOps.checkpoint[NativeEventLog]
                         _ <- StorageOps.restart[NativeEventLog]
                         r <- ObjectStore.load[NativeEventLog]
                       yield r).provideEnvironment(env)
              yield r
            },
            baseline = ZIO.succeed(expected),
          ) { (live, model) =>
            assertTrue(live == model, live.events == events)
          }
        }
      },
      test("NativeLocal STM temp layer supports model-vs-engine immutable root sequences") {
        check(Gen.chunkOfBounded(1, 8)(PersistenceGenerators.messageEvent)) { events =>
          val expected = NativeEventLog(events)

          PersistenceSpec.compare(
            live = ZIO.scoped {
              for
                env <- NativeLocalObjectStore.tempLayerWithSTM(nativeEventLogDescriptor).build
                r   <- (for
                         _ <- ZIO.foreachDiscard(events) { event =>
                                NativeLocalSTM.atomically[NativeEventLog, Unit] { ref =>
                                  ref.update(root => root.copy(events = root.events :+ event)) *>
                                    STM.unit
                                }
                              }
                         _ <- StorageOps.checkpoint[NativeEventLog]
                         _ <- StorageOps.restart[NativeEventLog]
                         r <- ObjectStore.load[NativeEventLog]
                       yield r).provideEnvironment(env)
              yield r
            },
            baseline = ZIO.succeed(expected),
          ) { (live, model) =>
            assertTrue(live == model, live.events == events)
          }
        }
      },
    )
