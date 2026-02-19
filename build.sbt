ThisBuild / scalaVersion := "3.5.2"
ThisBuild / organization := "io.github.riccardomerolla"
ThisBuild / organizationName := "Riccardo Merolla"
ThisBuild / organizationHomepage := Some(url("https://github.com/riccardomerolla"))

lazy val zioVersion = "2.1.24"
lazy val zioSchemaVersion = "1.8.0"
lazy val zioJsonVersion = "0.9.0"
lazy val zioHttpVersion = "3.8.1"
lazy val zioConfigVersion = "4.0.6"
lazy val eclipseStoreVersion = "3.0.1"

inThisBuild(List(
  organization := "io.github.riccardomerolla",
  homepage := Some(url("https://github.com/riccardomerolla/zio-eclipsestore")),
  licenses := Seq(
    "MIT" -> url("https://opensource.org/license/mit")
  ),
  developers := List(
    Developer(
      id = "riccardomerolla",
      name = "Riccardo Merolla",
      email = "riccardo.merolla@gmail.com",
      url = url("https://github.com/riccardomerolla")
    )
  ),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/riccardomerolla/zio-eclipsestore"),
      "scm:git@github.com:riccardomerolla/zio-eclipsestore.git"
    )
  ),
  versionScheme := Some("early-semver")
))

lazy val root = (project in file("."))
  .settings(
    name := "zio-eclipsestore",
    description := "ZIO-based library for type-safe, efficient, and boilerplate-free access to EclipseStore",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-schema" % zioSchemaVersion,
      "dev.zio" %% "zio-schema-derivation" % zioSchemaVersion,
      "dev.zio" %% "zio-schema-json" % zioSchemaVersion,
      "dev.zio" %% "zio-json" % zioJsonVersion,
      "org.eclipse.store" % "storage-embedded" % eclipseStoreVersion,
      "org.eclipse.store" % "storage-embedded-configuration" % eclipseStoreVersion,
      "org.eclipse.serializer" % "persistence-binary-jdk8" % eclipseStoreVersion,
      "org.eclipse.serializer" % "base" % eclipseStoreVersion,
      "dev.zio" %% "zio-config" % zioConfigVersion,
      "dev.zio" %% "zio-config-magnolia" % zioConfigVersion,
      "dev.zio" %% "zio-config-typesafe" % zioConfigVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val gigamap = (project in file("gigamap"))
  .settings(
    name := "zio-eclipsestore-gigamap",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .dependsOn(root)

lazy val lazyLoadingExample = (project in file("examples/lazy-loading"))
  .settings(
    name := "zio-eclipsestore-lazy-loading",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .dependsOn(root)

lazy val gigamapCli = (project in file("examples/gigamap-cli"))
  .settings(
    name := "zio-eclipsestore-gigamap-cli",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-cli" % "0.7.4",
      "dev.zio" %% "zio-json" % zioJsonVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    Compile / run / mainClass := Some("io.github.riccardomerolla.zio.eclipsestore.examples.gigamap.ReplApp"),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .dependsOn(gigamap)

lazy val bookstore = (project in file("examples/bookstore"))
  .settings(
    name := "zio-eclipsestore-bookstore",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-http" % zioHttpVersion,
      "dev.zio" %% "zio-json" % zioJsonVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    Compile / run / mainClass := Some("io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.BookstoreServer"),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .dependsOn(root)

lazy val storageSqlite = (project in file("storage-sqlite"))
  .settings(
    name := "zio-eclipsestore-storage-sqlite",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "org.xerial" % "sqlite-jdbc" % "3.46.0.1",
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .dependsOn(root)
