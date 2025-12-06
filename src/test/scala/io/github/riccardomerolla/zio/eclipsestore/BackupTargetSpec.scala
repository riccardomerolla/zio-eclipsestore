package io.github.riccardomerolla.zio.eclipsestore

import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object BackupTargetSpec extends ZIOSpecDefault:

  private final class FakeFoundation:
    var received: List[(String, String)] = Nil
    def setBackupConfigurationProperty(key: String, value: String): Unit =
      received = received :+ (key -> value)

  override def spec =
    suite("Backup configuration adapter")(
      test("applies backup configuration properties to foundations that support it") {
        val fake = FakeFoundation()
        EclipseStoreService.applyBackupConfiguration(fake, Map("backup-filesystem.sql.url" -> "jdbc:sqlite:bk.db"))
        assertTrue(fake.received.contains("backup-filesystem.sql.url" -> "jdbc:sqlite:bk.db"))
      }
    )
