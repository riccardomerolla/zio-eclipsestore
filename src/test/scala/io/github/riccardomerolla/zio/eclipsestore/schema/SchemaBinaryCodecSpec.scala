package io.github.riccardomerolla.zio.eclipsestore.schema

import java.nio.file.{ Files, Path }

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.{ Schema, derived }
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object SchemaBinaryCodecSpec extends ZIOSpecDefault:
  final case class Address(city: String, zip: String) derives Schema
  final case class Customer(
    id: String,
    nickname: Option[String],
    address: Address,
    tags: Chunk[String],
  ) derives Schema

  private val customerSchema: Schema[Customer] = summon[Schema[Customer]]

  private def withService[A](dir: Path)(use: EclipseStoreService => ZIO[Any, EclipseStoreError, A]) =
    val cfg = EclipseStoreConfig(
      storageTarget = StorageTarget.FileSystem(dir),
      customTypeHandlers = Chunk.single(SchemaBinaryCodec.handler(customerSchema)),
    )
    ZIO.scoped {
      val layer = ZLayer.succeed(cfg) >>> EclipseStoreService.live
      for
        env <- layer.build
        svc  = env.get[EclipseStoreService]
        out <- use(svc)
      yield out
    }

  private def tempDirLayer: ZLayer[Any, Throwable, Path] =
    ZLayer.scoped {
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory("schema-codec")))(path =>
        ZIO.attempt {
          if Files.exists(path) then Files.walk(path).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
        }.orDie
      )
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("SchemaBinaryCodec")(
      test("stores and reloads immutable case class values via schema-derived handler") {
        val customer =
          Customer(
            id = "cust-1",
            nickname = Some("rick"),
            address = Address(city = "Milan", zip = "20100"),
            tags = Chunk("vip", "newsletter"),
          )

        ZIO.serviceWithZIO[Path] { dir =>
          for
            stored <- withService(dir) { svc =>
                        for
                          _       <- svc.put("customer", customer)
                          fetched <- svc.get[String, Customer]("customer")
                        yield fetched
                      }
          yield assertTrue(stored.contains(customer))
        }
      }.provideLayerShared(tempDirLayer),
      test("generates deterministic handler type id from schema structure") {
        val first  = SchemaBinaryCodec.handler(customerSchema).typeId()
        val second = SchemaBinaryCodec.handler(customerSchema).typeId()
        assertTrue(first == second, first > 0L)
      },
    )
