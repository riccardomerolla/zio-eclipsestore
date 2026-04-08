package io.github.riccardomerolla.zio.eclipsestore

import java.nio.file.{ Files, Path }
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap
import java.util.{ ArrayList, List as JList }

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, ObjectStore, Transaction }

object ObjectStoreSpec extends ZIOSpecDefault:

  final class EmployeeDirectory(val employees: JList[String]) extends Serializable
  final class OrganizationRoot(val departments: JList[EmployeeDirectory]) extends Serializable

  given Tag[EmployeeDirectory] = Tag.derived
  given Tag[OrganizationRoot]  = Tag.derived
  given Tag[ConcurrentHashMap[Int, String]] = Tag.derived

  private val organizationDescriptor =
    RootDescriptor(
      id = "organization-root",
      initializer = () =>
        OrganizationRoot(
          new ArrayList(
            List(
              EmployeeDirectory(new ArrayList(List("alice").asJava)),
              EmployeeDirectory(new ArrayList(List("carol").asJava)),
            ).asJava
          )
        ),
    )

  private val counterDescriptor =
    RootDescriptor.concurrentMap[Int, String]("counter-root")

  private def deleteDirectory(path: Path): Unit =
    if Files.exists(path) then Files.walk(path).sorted(Comparator.reverseOrder()).forEach(Files.delete)

  private def objectStoreLayer[A: Tag](path: Path, descriptor: RootDescriptor[A]) =
    ZLayer.succeed(
      EclipseStoreConfig.make(path).copy(rootDescriptors = Chunk.single(descriptor))
    ) >>> EclipseStoreService.live >>> ObjectStore.live(descriptor)

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("ObjectStore")(
      test("fresh store loads typed root, persists mutation, and reloads after checkpoint") {
        ZIO.scoped {
          for
            path <- ZIO.attemptBlocking(Files.createTempDirectory("object-store-root"))
            _    <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer = objectStoreLayer(path, organizationDescriptor)
            _    <- (for
                      root <- ObjectStore.load[OrganizationRoot]
                      _    <- ZIO.succeed(root.departments.getFirst.employees.add("bob"))
                      _    <- ObjectStore.storeRoot[OrganizationRoot]
                      _    <- ObjectStore.checkpoint[OrganizationRoot]
                    yield ()).provideLayer(layer)
            out  <- (for
                      root <- ObjectStore.load[OrganizationRoot]
                    yield root.departments.getFirst.employees.asScala.toList).provideLayer(layer)
          yield assertTrue(out == List("alice", "bob"))
        }
      },
      test("storing a subgraph persists only the targeted branch update across restart") {
        ZIO.scoped {
          for
            path <- ZIO.attemptBlocking(Files.createTempDirectory("object-store-subgraph"))
            _    <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer = objectStoreLayer(path, organizationDescriptor)
            _    <- (for
                      root      <- ObjectStore.load[OrganizationRoot]
                      department = root.departments.getFirst
                      _         <- ZIO.succeed(department.employees.add("bob"))
                      _         <- ObjectStore.storeSubgraph[OrganizationRoot](department)
                    yield ()).provideLayer(layer)
            out  <- (for
                      root <- ObjectStore.load[OrganizationRoot]
                    yield root.departments.asScala.toList.map(_.employees.asScala.toList)).provideLayer(layer)
          yield assertTrue(
            out.head == List("alice", "bob"),
            out(1) == List("carol"),
          )
        }
      },
      test("serialized transactions keep concurrent mutations consistent across restart") {
        ZIO.scoped {
          for
            path <- ZIO.attemptBlocking(Files.createTempDirectory("object-store-concurrent"))
            _    <- ZIO.addFinalizer(ZIO.attemptBlocking(deleteDirectory(path)).orDie)
            layer = objectStoreLayer(path, counterDescriptor)
            _    <- ZIO
                      .foreachPar(1 to 100) { i =>
                        ObjectStore.transact[ConcurrentHashMap[Int, String], Unit](
                          Transaction.effect(root =>
                            ZIO
                              .attempt(root.put(i, s"value-$i"))
                              .unit
                              .mapError(cause =>
                                EclipseStoreError.StorageError(
                                  s"Failed to persist concurrent mutation for key $i",
                                  Some(cause),
                                )
                              )
                          )
                        )
                      }
                      .withParallelism(16)
                      .provideLayer(layer)
            out  <- (for
                      root <- ObjectStore.load[ConcurrentHashMap[Int, String]]
                    yield root.asScala.toMap).provideLayer(layer)
          yield assertTrue(
            out.size == 100,
            out.get(1).contains("value-1"),
            out.get(100).contains("value-100"),
          )
        }
      },
    )
