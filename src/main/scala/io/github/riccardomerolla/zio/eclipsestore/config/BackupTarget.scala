package io.github.riccardomerolla.zio.eclipsestore.config

/** Backup target adapters that produce configuration properties for EclipseStore foundations. */
sealed trait BackupTarget:
  def toProperties: Map[String, String]

object BackupTarget:
  /** SQLite backup target using JDBC URL, optional catalog/schema and custom data-source-provider class. */
  final case class SqliteBackup(
      url: String,
      dataSourceProvider: Option[String] = None,
      catalog: Option[String] = None,
      schema: Option[String] = None,
    ) extends BackupTarget:
    override def toProperties: Map[String, String] =
      Map(
        "backup-filesystem.sql.sqlite.url" -> url
      ) ++ dataSourceProvider.map("backup-filesystem.sql.sqlite.data-source-provider" -> _) ++
        catalog.map("backup-filesystem.sql.sqlite.catalog" -> _) ++
        schema.map("backup-filesystem.sql.sqlite.schema" -> _)

  /** AWS S3 backup target using static credentials. Bucket/path selection is left to higher-level config. */
  final case class S3Backup(
      accessKeyId: String,
      secretAccessKey: String,
      region: String,
      sessionToken: Option[String] = None,
    ) extends BackupTarget:
    override def toProperties: Map[String, String] =
      Map(
        "backup-filesystem.aws.s3.credentials.type"          -> "static",
        "backup-filesystem.aws.s3.credentials.access-key-id" -> accessKeyId,
        "backup-filesystem.aws.s3.credentials.secret-access-key" -> secretAccessKey,
        "backup-filesystem.aws.s3.credentials.region"        -> region,
      ) ++ sessionToken.map("backup-filesystem.aws.s3.credentials.session-token" -> _)

  /** Generic SQL backup target allowing provider prefix (e.g., postgres, mysql). */
  final case class SqlBackup(
      provider: String,
      url: String,
      dataSourceProvider: Option[String] = None,
      catalog: Option[String] = None,
      schema: Option[String] = None,
      extra: Map[String, String] = Map.empty,
    ) extends BackupTarget:
    private val prefix = s"backup-filesystem.sql.$provider."
    override def toProperties: Map[String, String] =
      Map(prefix + "url" -> url) ++
        dataSourceProvider.map(prefix + "data-source-provider" -> _) ++
        catalog.map(prefix + "catalog" -> _) ++
        schema.map(prefix + "schema" -> _) ++
        extra.map { case (k, v) => prefix + k -> v }

  /** Simple FTP/FTPS backup target. */
  final case class FtpBackup(
      host: String,
      user: Option[String] = None,
      password: Option[String] = None,
      port: Option[Int] = None,
      basePath: Option[String] = None,
      secure: Boolean = false,
    ) extends BackupTarget:
    override def toProperties: Map[String, String] =
      Map(
        "backup-filesystem.ftp.host"        -> host,
        "backup-filesystem.ftp.secure"      -> secure.toString,
      ) ++ user.map("backup-filesystem.ftp.user" -> _) ++
        password.map("backup-filesystem.ftp.password" -> _) ++
        port.map(p => "backup-filesystem.ftp.port" -> p.toString) ++
        basePath.map("backup-filesystem.ftp.base-path" -> _)
