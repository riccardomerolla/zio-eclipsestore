package io.github.riccardomerolla.zio.eclipsestore.sqlite

import java.nio.file.{ Files, Path }
import java.sql.DriverManager
import java.util.Comparator

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.StorageTarget
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object SQLiteStorageSpec extends ZIOSpecDefault:

  private def deleteDirectory(path: Path): UIO[Unit] =
    ZIO.attemptBlocking {
      if Files.exists(path) then
        Files
          .walk(path)
          .sorted(Comparator.reverseOrder())
          .forEach(Files.delete)
    }.ignore

  override def spec: Spec[Environment & (TestEnvironment & Scope), Any] =
    suite("SQLiteStorage")(
      test("jdbc driver can create and read sqlite database") {
        ZIO.scoped {
          for
            dbDir <- ZIO.attemptBlocking(Files.createTempDirectory("sqlite-jdbc"))
            dbPath = dbDir.resolve("test.db")
            _     <- ZIO.addFinalizer(deleteDirectory(dbDir))
            _     <- ZIO.attempt {
                       Class.forName("org.sqlite.JDBC")
                       val url  = s"jdbc:sqlite:$dbPath"
                       val conn = DriverManager.getConnection(url)
                       val stmt = conn.createStatement()
                       stmt.executeUpdate("create table if not exists kv (k text primary key, v text)")
                       stmt.executeUpdate("insert into kv(k, v) values ('a','b')")
                       val rs   = stmt.executeQuery("select v from kv where k='a'")
                       assertTrue(rs.next() && rs.getString(1) == "b")
                       conn.close()
                     }
          yield assertTrue(true)
        }
      },
      test("persists and reloads values using SQLite storage target") {
        ZIO.scoped {
          for
            dataDir <- ZIO.attemptBlocking(Files.createTempDirectory("sqlite-storage"))
            _       <- ZIO.addFinalizer(deleteDirectory(dataDir))
            layer1   = SQLiteStorage.live(dataDir, "books.db")
            _       <- (for _ <- EclipseStoreService.put("book:1", "ZIO")
                       yield ()).provideLayer(layer1)
            layer2   = SQLiteStorage.live(dataDir, "books.db")
            loaded  <- (for value <- EclipseStoreService.get[String, String]("book:1")
                       yield value).provideLayer(layer2)
          yield assertTrue(loaded.contains("ZIO"))
        }
      },
      test("accepts custom connection string in config") {
        val config =
          SQLiteStorage.config(Path.of("/tmp/sqlite"), "data.db", Some("jdbc:sqlite:/tmp/sqlite/data.db?cache=shared"))
        assertTrue(
          config.storageTarget match
            case s: StorageTarget.Sqlite =>
              s.connectionString.contains("jdbc:sqlite:/tmp/sqlite/data.db?cache=shared")
            case _                       => false
        )
      },
    )
