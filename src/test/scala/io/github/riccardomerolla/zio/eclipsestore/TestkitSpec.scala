package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.{ Files, Path }
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.Schema
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.schema.TypedStore
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, ObjectStore, Transaction }
import io.github.riccardomerolla.zio.eclipsestore.testkit.{
  BddScenario,
  FaultInjector,
  InMemoryObjectStore,
  PersistenceGenerators,
  PersistenceSpec,
}

object TestkitSpec extends ZIOSpecDefault:
  private val mapDescriptor =
    RootDescriptor.concurrentMap[String, String]("testkit-root")

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
            injector <- FaultInjector.inMemory.build.map(_.get[FaultInjector])
            cleaned  <- Ref.make(false)
            _        <- FaultInjector
                          .failNext(
                            operation = "backup",
                            error = EclipseStoreError.ResourceError("boom", None),
                            cleanup = cleaned.set(true),
                          )
                          .provideEnvironment(ZEnvironment(injector))
            result   <- FaultInjector
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
              path <- ZIO.attemptBlocking(Files.createTempDirectory("testkit-typed-store")).orDie
              _    <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
              liveLayer =
                ZLayer.succeed(EclipseStoreConfig.make(path)) >>>
                  TypedStore.handlersFor[String, PersistenceGenerators.MessageEvent] >>>
                  EclipseStoreService.live >>>
                  TypedStore.live
              memLayer =
                EclipseStoreService.inMemory >>> TypedStore.live
              liveSpec =
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
    )
