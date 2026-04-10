# Getting Started

This guide takes you from zero to a running `zio-eclipsestore` app in minutes.

## 1) Add dependencies

Use the latest published version for your modules.

```scala
libraryDependencies ++= Seq(
  "io.github.riccardomerolla" %% "zio-eclipsestore" % "<latest>",
  "io.github.riccardomerolla" %% "zio-eclipsestore-gigamap" % "<latest>", // optional
  "io.github.riccardomerolla" %% "zio-eclipsestore-storage-sqlite" % "<latest>" // optional
)
```

## 2) Minimal app

```scala
import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import zio.*

object HelloStore extends ZIOAppDefault:
  def run =
    (for
      _      <- EclipseStoreService.put("user:1", "Alice")
      stored <- EclipseStoreService.get[String, String]("user:1")
      _      <- ZIO.logInfo(s"Stored value: ${stored.getOrElse("missing")}")
    yield ())
      .provide(
        EclipseStoreConfig.temporaryLayer,
        EclipseStoreService.live,
      )
```

## 3) Typed root example

```scala
import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import scala.collection.mutable.ListBuffer
import zio.*

val favorites = RootDescriptor(
  id = "favorite-users",
  initializer = () => ListBuffer.empty[String],
)

val program =
  for
    root <- EclipseStoreService.root(favorites)
    _    <- ZIO.succeed(root.addOne("user:1"))
    _    <- EclipseStoreService.storeRoot(root)
  yield ()
```

## 4) Configuration from HOCON

```scala
import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfigZIO

val configLayer = EclipseStoreConfigZIO.fromResourcePath
```

```hocon
eclipsestore {
  storageTarget {
    fileSystem {
      path = "./data/store"
    }
  }
  performance {
    channelCount = 8
  }
}
```

## 5) Run examples

```bash
sbt "runMain io.github.riccardomerolla.zio.eclipsestore.examples.gettingstarted.GettingStartedApp"
sbt "runMain io.github.riccardomerolla.zio.eclipsestore.examples.nativelocal.TodoNativeLocalApp"
```

For NativeLocal deep dive, see [native-local-guide.md](native-local-guide.md).
