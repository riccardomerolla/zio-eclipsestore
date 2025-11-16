ThisBuild / scalaVersion := "3.3.1"
ThisBuild / organization := "io.github.riccardomerolla"
ThisBuild / organizationName := "Riccardo Merolla"
ThisBuild / organizationHomepage := Some(url("https://github.com/riccardomerolla"))

lazy val zioVersion = "2.1.22"
lazy val zioSchemaVersion = "1.7.5"
lazy val zioJsonVersion = "0.7.45"
lazy val zioHttpVersion = "3.5.1"

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
      "org.eclipse.store" % "storage-embedded" % "3.0.1",
      "org.eclipse.store" % "storage-embedded-configuration" % "3.0.1",
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val bookstore = (project in file("examples/bookstore"))
  .settings(
    name := "zio-eclipsestore-bookstore",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-http" % zioHttpVersion,
      "dev.zio" %% "zio-json" % zioJsonVersion
    ),
    Compile / run / mainClass := Some("io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.BookstoreServer")
  )
  .dependsOn(root)
