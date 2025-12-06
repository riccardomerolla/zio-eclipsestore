package io.github.riccardomerolla.zio.eclipsestore

import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.BackupTarget
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object BackupTargetSpec extends ZIOSpecDefault:

  final private class FakeFoundation:
    var received: List[(String, String)]                                 = Nil
    def setBackupConfigurationProperty(key: String, value: String): Unit =
      received = received :+ (key -> value)

  override def spec =
    suite("Backup configuration adapter")(
      test("applies backup configuration properties to foundations that support it") {
        val fake = FakeFoundation()
        EclipseStoreService.applyBackupConfiguration(fake, Map("backup-filesystem.sql.url" -> "jdbc:sqlite:bk.db"))
        assertTrue(fake.received.contains("backup-filesystem.sql.url" -> "jdbc:sqlite:bk.db"))
      },
      test("builds properties for sqlite backup target") {
        val target = BackupTarget.SqliteBackup(
          url = "jdbc:sqlite:eclipsestore_bkup_db",
          dataSourceProvider = Some("com.sample.MyDataSourceProvider"),
          catalog = Some("mycatalog"),
          schema = Some("myschema"),
        )
        val props  = target.toProperties
        assertTrue(
          props("backup-filesystem.sql.sqlite.url") == "jdbc:sqlite:eclipsestore_bkup_db",
          props("backup-filesystem.sql.sqlite.data-source-provider") == "com.sample.MyDataSourceProvider",
          props("backup-filesystem.sql.sqlite.catalog") == "mycatalog",
          props("backup-filesystem.sql.sqlite.schema") == "myschema",
        )
      },
      test("builds properties for S3 backup target") {
        val target = BackupTarget.S3Backup(
          accessKeyId = "id",
          secretAccessKey = "secret",
          region = "us-east-1",
          sessionToken = Some("token"),
        )
        val props  = target.toProperties
        assertTrue(
          props("backup-filesystem.aws.s3.credentials.type") == "static",
          props("backup-filesystem.aws.s3.credentials.access-key-id") == "id",
          props("backup-filesystem.aws.s3.credentials.secret-access-key") == "secret",
          props("backup-filesystem.aws.s3.credentials.region") == "us-east-1",
          props("backup-filesystem.aws.s3.credentials.session-token") == "token",
        )
      },
      test("builds properties for generic SQL backup target (postgres)") {
        val target = BackupTarget.SqlBackup(
          provider = "postgres",
          url = "jdbc:postgresql://localhost/db",
          dataSourceProvider = Some("com.foo.DSProvider"),
          catalog = Some("cat"),
          schema = Some("public"),
          extra = Map("ssl" -> "true"),
        )
        val props  = target.toProperties
        assertTrue(
          props("backup-filesystem.sql.postgres.url") == "jdbc:postgresql://localhost/db",
          props("backup-filesystem.sql.postgres.data-source-provider") == "com.foo.DSProvider",
          props("backup-filesystem.sql.postgres.catalog") == "cat",
          props("backup-filesystem.sql.postgres.schema") == "public",
          props("backup-filesystem.sql.postgres.ssl") == "true",
        )
      },
      test("builds properties for FTP backup target") {
        val target = BackupTarget.FtpBackup(
          host = "ftp.example.com",
          user = Some("user"),
          password = Some("pass"),
          port = Some(21),
          basePath = Some("/backups"),
          secure = true,
        )
        val props  = target.toProperties
        assertTrue(
          props("backup-filesystem.ftp.host") == "ftp.example.com",
          props("backup-filesystem.ftp.user") == "user",
          props("backup-filesystem.ftp.password") == "pass",
          props("backup-filesystem.ftp.port") == "21",
          props("backup-filesystem.ftp.base-path") == "/backups",
          props("backup-filesystem.ftp.secure") == "true",
        )
      },
    )
