package io.github.riccardomerolla.zio.eclipsestore.lazyloading

import zio.*
import zio.test.*

import java.nio.file.{ Files, Path }
import java.time.Instant
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.lazyloading.model.{ BusinessApp, Turnover }
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object LazyLoadingExampleSpec extends ZIOSpecDefault:

  private def deleteDirectory(path: Path): UIO[Unit] =
    ZIO.attemptBlocking {
      if Files.exists(path) then
        Files.walk(path).sorted(Comparator.reverseOrder()).forEach(Files.delete)
    }.ignore

  override def spec =
    suite("Lazy loading example (BusinessYear)")(
      test("persists, reloads, and clears lazy list") {
        ZIO.scoped {
          for
            dir <- ZIO.attemptBlocking(Files.createTempDirectory("lazy-example"))
            _   <- ZIO.addFinalizer(deleteDirectory(dir))
            layer = ZLayer.succeed(EclipseStoreConfig(StorageTarget.FileSystem(dir))) >>> EclipseStoreService.live
            rootDescriptor = RootDescriptor(
              id = "lazy-app",
              initializer = () => new ConcurrentHashMap[Int, io.github.riccardomerolla.zio.eclipsestore.lazyloading.model.BusinessYear](),
            )
            // persist root with lazy turnovers
            _ <- (for
                    root <- EclipseStoreService.root(rootDescriptor)
                    app   = new BusinessApp(root)
                    year  = app.ensureYear(2023)
                    touchedBefore = year.touchedTimestamp
                    _     = year.add(Turnover(42.0, Instant.EPOCH))
                    _    <- EclipseStoreService.persist(root)
                  yield touchedBefore).provideLayer(layer)
            // reload and verify data is available lazily
            reloaded <- (for
                          root <- EclipseStoreService.root(rootDescriptor)
                          year  = root.get(2023)
                          data  = Option(year).toList.flatMap(_.stream.toList)
                          touched = Option(year).map(_.touchedTimestamp).getOrElse(0L)
                        yield (data, touched)).provideLayer(layer)
            // clear lazy ref if stored
            _ <- (for
                    root <- EclipseStoreService.root(rootDescriptor)
                    year  = root.get(2023)
                    _     = if year != null then year.clearIfStored() else ()
                  yield ()).provideLayer(layer)
          yield
            val (data, touched) = reloaded
            assertTrue(data.nonEmpty, data.head.amount == 42.0, touched > 0)
        }
      }
    )
