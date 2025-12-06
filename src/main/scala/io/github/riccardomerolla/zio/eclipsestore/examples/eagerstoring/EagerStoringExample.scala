package io.github.riccardomerolla.zio.eclipsestore.examples.eagerstoring

import zio.*
import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import org.eclipse.serializer.persistence.types.PersistenceEagerStoringFieldEvaluator

import java.lang.reflect.Field
import java.nio.file.Path
import java.time.LocalDateTime

final case class EagerRoot(
    @StoreEager private var nums: List[Int],
    private var dates: List[LocalDateTime],
):
  def numbers: List[Int] = nums
  def datesList: List[LocalDateTime] = dates
  def appendNext(): EagerRoot =
    val next = nums.size + 1
    nums = nums :+ next
    dates = dates :+ LocalDateTime.now()
    this

object EagerStoringExample:

  private final class StoreEagerEvaluator extends PersistenceEagerStoringFieldEvaluator:
    override def isEagerStoring(clazz: Class[?], field: Field): Boolean =
      field.isAnnotationPresent(classOf[StoreEager])

  private val rootDescriptor =
    io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor[EagerRoot](
      id = "eager-root",
      initializer = () => EagerRoot(List(1), List(LocalDateTime.now())),
      migrate = identity,
      onLoad = identity,
    )

  private def configuredLayer(path: Path): ZLayer[Any, EclipseStoreError, EclipseStoreService] =
    val cfg = EclipseStoreConfig(
      storageTarget = StorageTarget.FileSystem(path),
      rootDescriptors = zio.Chunk(rootDescriptor),
      eagerStoringEvaluator = Some(new StoreEagerEvaluator),
    )
    ZLayer.succeed(cfg) >>> EclipseStoreService.live

  def runStep(path: Path): ZIO[Any, EclipseStoreError, (List[Int], List[LocalDateTime])] =
    ZIO.scoped {
      configuredLayer(path).build.flatMap { env =>
        val svc = env.get[EclipseStoreService]
        for {
          root <- svc.root(rootDescriptor)
          _    <- ZIO.attempt(root.appendNext()).mapError(e => EclipseStoreError.StorageError("append", Some(e)))
          _    <- svc.persist(root)
        } yield (root.numbers, root.datesList)
      }
    }
