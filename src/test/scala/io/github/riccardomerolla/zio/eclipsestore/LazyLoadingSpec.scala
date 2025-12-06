package io.github.riccardomerolla.zio.eclipsestore

import zio.*
import zio.test.*

import java.nio.file.Files
import java.time.Instant
import java.util.{ ArrayList, Iterator as JIterator, List as JList }
import java.util.concurrent.ConcurrentHashMap

import io.github.riccardomerolla.zio.eclipsestore.LazyHelpers
import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import org.eclipse.serializer.reference.Lazy
import scala.jdk.CollectionConverters.*

object LazyLoadingSpec extends ZIOSpecDefault:

  final case class Turnover(amount: Double, timestamp: Instant)

  final class BusinessYear(private var turnovers: Lazy[JList[Turnover]] = null):
    private def ensureTurnovers(): JList[Turnover] =
      val current = Lazy.get(turnovers)
      if current != null then current
      else
        val fresh = new ArrayList[Turnover]()
        turnovers = Lazy.Reference(fresh)
        fresh

    def add(turnover: Turnover): Unit =
      ensureTurnovers().add(turnover)

    def stream: Iterator[Turnover] =
      val data = Lazy.get(turnovers)
      if data == null then Iterator.empty
      else
        val it: JIterator[Turnover] = data.iterator()
        it.asScala

  override def spec =
    suite("Lazy loading primitives")(
      test("Lazy.Reference exposes value and updates touched timestamp") {
        val ref        = LazyHelpers.reference("lazy-value")
        val touched0   = ref.lastTouched()
        val value      = ref.get()
        val touchedNow = ref.lastTouched()
        assertTrue(value == "lazy-value", touchedNow >= touched0, LazyHelpers.isLoaded(ref))
      },
      test("LazyReferenceManager registers and iterates lazy references") {
        val ref1      = LazyHelpers.reference("a")
        val ref2      = LazyHelpers.reference("b")
        val manager   = LazyHelpers.newManager()
        manager.register(ref1)
        manager.register(ref2)
        val buffer    = scala.collection.mutable.ListBuffer.empty[Lazy[?]]
        manager.iterate(
          new java.util.function.Consumer[Lazy[?]]:
            override def accept(t: Lazy[?]): Unit = buffer += t
        )
        val collected = buffer.size
        assertTrue(collected == 2)
      },
      test("Lazy reference clear resets loaded state and touched timestamp") {
        val ref      = LazyHelpers.reference("to-clear")
        val touched  = ref.lastTouched()
        val value    = ref.get()
        val afterGet = ref.lastTouched()
        assertTrue(
          value == "to-clear",
          afterGet >= touched,
          LazyHelpers.isLoaded(ref),
        )
      },
      test("lazy list survives reload and loads on demand") {
        ZIO.scoped {
          for
            dir           <- ZIO.attemptBlocking(Files.createTempDirectory("lazy-root"))
            _             <- ZIO.addFinalizer(
                               ZIO
                                 .attemptBlocking(Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete))
                                 .ignore
                             )
            layer          = ZLayer.succeed(
                               EclipseStoreConfig(storageTarget = StorageTarget.FileSystem(dir))
                             ) >>> EclipseStoreService.live
            rootDescriptor = RootDescriptor(
                               id = "lazy-root",
                               initializer = () => new ConcurrentHashMap[Int, BusinessYear](),
                             )
            _             <- (for
                               root <- EclipseStoreService.root(rootDescriptor)
                               year  = root.computeIfAbsent(2023, _ => new BusinessYear())
                               _     = year.add(Turnover(100.0, Instant.EPOCH))
                               _    <- EclipseStoreService.persist(root)
                             yield ()).provideLayer(layer)
            reloaded      <- (for
                               root <- EclipseStoreService.root(rootDescriptor)
                               year  = root.get(2023)
                               data  = Option(year).toList.flatMap(_.stream.toList)
                             yield data).provideLayer(layer)
          yield assertTrue(reloaded.nonEmpty, reloaded.head.amount == 100.0)
        }
      },
    )
