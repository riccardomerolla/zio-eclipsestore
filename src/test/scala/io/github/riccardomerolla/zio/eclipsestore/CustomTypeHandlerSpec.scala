package io.github.riccardomerolla.zio.eclipsestore

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.nio.file.{ Files, Path }
import java.time.Instant
import javax.imageio.ImageIO

import scala.jdk.CollectionConverters.*

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import org.eclipse.serializer.persistence.binary.types.{ Binary, BinaryTypeHandler }

object CustomTypeHandlerSpec extends ZIOSpecDefault:

  final private class Employee(
    var id: String,
    var salary: Double,
    var dateOfBirth: Instant,
  ) // scalafix:ok DisableSyntax.var

  private object Employee:
    val typeHandler: BinaryTypeHandler[Employee] =
      Binary.TypeHandler(
        classOf[Employee],
        Binary.Field_long("id", (e: Employee) => e.id.toLong, (e: Employee, v: Long) => e.id = v.toString),
        Binary.Field_long(
          "dateOfBirth",
          (e: Employee) => e.dateOfBirth.toEpochMilli,
          (e: Employee, v: Long) => e.dateOfBirth = Instant.ofEpochMilli(v),
        ),
        Binary.Field_double("salary", (e: Employee) => e.salary, (e: Employee, v: Double) => e.salary = v),
      )

  final private class ImageBlob(var image: BufferedImage) // scalafix:ok DisableSyntax.var

  private object ImageBlob:
    private def toBytes(image: BufferedImage): Array[Byte] =
      val out = new ByteArrayOutputStream()
      ImageIO.write(image, "png", out)
      out.toByteArray

    private def fromBytes(bytes: Array[Byte]): BufferedImage =
      ImageIO.read(new java.io.ByteArrayInputStream(bytes))

    val typeHandler: BinaryTypeHandler[ImageBlob] =
      Binary.TypeHandler(
        classOf[ImageBlob],
        Binary.Field(
          classOf[Array[Byte]],
          "imageData",
          (b: ImageBlob) => toBytes(b.image),
          (b: ImageBlob, data: Array[Byte]) => b.image = fromBytes(data),
        ),
      )

  private def withService[A](dir: Path)(use: EclipseStoreService => ZIO[Any, EclipseStoreError, A]) =
    val cfg = EclipseStoreConfig(
      storageTarget = StorageTarget.FileSystem(dir),
      customTypeHandlers = Chunk(Employee.typeHandler, ImageBlob.typeHandler),
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
      ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory("custom-handlers")))(path =>
        ZIO.attempt {
          if Files.exists(path) then Files.walk(path).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
        }.orDie
      )
    }

  private def newImage(): BufferedImage =
    val img = new BufferedImage(2, 2, BufferedImage.TYPE_INT_RGB)
    img.setRGB(0, 0, Color.RED.getRGB)
    img.setRGB(1, 0, Color.GREEN.getRGB)
    img.setRGB(0, 1, Color.BLUE.getRGB)
    img.setRGB(1, 1, Color.YELLOW.getRGB)
    img

  override def spec: Spec[Environment & (TestEnvironment & Scope), Any] =
    suite("Custom type handlers & blobs")(
      test("stores and reloads employee with custom type handler and buffered image blob") {
        val employee = new Employee("42", 99.5d, Instant.ofEpochMilli(1_000L))
        val image    = new ImageBlob(newImage())

        ZIO.serviceWithZIO[Path] { dir =>
          for {
            results         <- withService(dir) { svc =>
                                 for {
                                   _      <- svc.put("employee", employee)
                                   _      <- svc.put("logo", image)
                                   empOpt <- svc.get[String, Employee]("employee")
                                   imgOpt <- svc.get[String, ImageBlob]("logo")
                                 } yield (empOpt, imgOpt)
                               }
            (empOpt, imgOpt) = results
            _               <- ZIO.logInfo(s"image result: $imgOpt")
            empOk            =
              empOpt.exists(e =>
                e.id == "42" && math.abs(e.salary - 99.5d) < 0.0001 && e.dateOfBirth == Instant.ofEpochMilli(1_000L)
              )
            imgOk            = imgOpt.exists(_.image != null) // scalafix:ok DisableSyntax.null
          } yield assertTrue(empOk, imgOk)
        }
      }.provideLayerShared(tempDirLayer)
    )
