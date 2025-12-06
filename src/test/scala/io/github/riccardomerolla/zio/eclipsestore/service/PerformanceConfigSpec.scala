package io.github.riccardomerolla.zio.eclipsestore.service

import zio.*
import zio.test.*

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import io.github.riccardomerolla.zio.eclipsestore.config.*
import org.eclipse.serializer.persistence.binary.types.Binary
import org.eclipse.serializer.persistence.types.PersistenceTypeHandler
import scala.jdk.CollectionConverters.*

/** Verifies that performance-related configuration knobs are propagated to the underlying foundation. */
object PerformanceConfigSpec extends ZIOSpecDefault:

  final private class CapturingFoundation:
    var channelCount: Option[Int]                      = None
    var pageCache: Option[Long]                        = None
    var objectCache: Option[Long]                      = None
    var offHeap: Boolean                               = false
    var compression: Option[String]                    = None
    var encryption: Option[Array[Byte]]                = None
    var backupDir: Option[String]                      = None
    var truncationDir: Option[String]                  = None
    var deletionDir: Option[String]                    = None
    val backupProps: ConcurrentHashMap[String, String] = new ConcurrentHashMap()
    val handlerRegistered: AtomicBoolean               = new AtomicBoolean(false)
    val eagerEvaluatorRegistered: AtomicBoolean        = new AtomicBoolean(false)

    def setChannelCount(count: Int): Unit                          = channelCount = Some(count)
    def setPageCacheSizeBytes(value: Long): Unit                   = pageCache = Some(value)
    def setObjectCacheSizeBytes(value: Long): Unit                 = objectCache = Some(value)
    def enableOffHeapPageStore(): Unit                             = offHeap = true
    def setCompression(value: String): Unit                        = compression = Some(value)
    def setEncryptionKey(value: Array[Byte]): Unit                 = encryption = Some(value)
    def setBackupDirectory(path: String): Unit                     = backupDir = Some(path)
    def setBackupTruncationDirectory(path: String): Unit           = truncationDir = Some(path)
    def setBackupDeletionDirectory(path: String): Unit             = deletionDir = Some(path)
    def setBackupConfigurationProperty(k: String, v: String): Unit =
      backupProps.put(k, v)

    // Signature used by configureFoundation when custom handlers/evaluators are present.
    def onConnectionFoundation(consumer: java.util.function.Consumer[AnyRef]): Unit =
      consumer.accept(
        new Object:
          def registerCustomTypeHandlers(handler: PersistenceTypeHandler[Binary, ?]): Unit =
            handlerRegistered.set(true)
          def setReferenceFieldEagerEvaluator(
              eval: org.eclipse.serializer.persistence.types.PersistenceEagerStoringFieldEvaluator
            ): Unit =
            eagerEvaluatorRegistered.set(true)
      )

  override def spec: Spec[TestEnvironment, Any] =
    suite("PerformanceConfig wiring")(
      test("applies performance and backup settings to foundation") {
        val perf       =
          StoragePerformanceConfig(
            channelCount = 8,
            pageCacheSizeBytes = Some(1024L),
            objectCacheSizeBytes = Some(2048L),
            useOffHeapPageStore = true,
            compression = CompressionSetting.LZ4,
            encryptionKey = Some(Array[Byte](1, 2, 3)),
          )
        val config     =
          EclipseStoreConfig(
            storageTarget = StorageTarget.InMemory("perf-spec"),
            performance = perf,
            backupDirectory = Some(java.nio.file.Paths.get("/tmp/backup")),
            backupTruncationDirectory = Some(java.nio.file.Paths.get("/tmp/backup/truncate")),
            backupDeletionDirectory = Some(java.nio.file.Paths.get("/tmp/backup/delete")),
            backupExternalProperties = Map("foo" -> "bar"),
            customTypeHandlers = Chunk.single(Dummy.handler),
          )
        val foundation = new CapturingFoundation

        for
          _ <- EclipseStoreService.configureFoundation(foundation, perf, config)
          _ <- ZIO.succeed(foundation.onConnectionFoundation(_ => ())) // trigger handler/evaluator wiring
        yield assertTrue(
          foundation.channelCount.contains(8),
          foundation.pageCache.contains(1024L),
          foundation.objectCache.contains(2048L),
          foundation.offHeap,
          foundation.compression.contains("LZ4"),
          foundation.encryption.exists(arr => arr.sameElements(Array[Byte](1, 2, 3))),
          foundation.backupDir.contains("/tmp/backup"),
          foundation.truncationDir.contains("/tmp/backup/truncate"),
          foundation.deletionDir.contains("/tmp/backup/delete"),
          foundation.backupProps.get("foo") == "bar",
          foundation.handlerRegistered.get(),
        )
      },
      test("merges backup target properties") {
        val foundation = new CapturingFoundation
        val config     = EclipseStoreConfig(
          storageTarget = StorageTarget.InMemory(),
          backupTarget = Some(
            BackupTarget.SqliteBackup(
              url = "jdbc:sqlite:/tmp/backup.db",
              catalog = Some("main"),
              schema = Some("public"),
            )
          ),
        )
        for _ <- EclipseStoreService.configureFoundation(foundation, config.performance, config)
        yield assertTrue(
          foundation.backupProps.asScala.get("backup-filesystem.sql.sqlite.url").exists(_.contains("jdbc:sqlite")),
          foundation.backupProps.asScala.get("backup-filesystem.sql.sqlite.catalog").contains("main"),
          foundation.backupProps.asScala.get("backup-filesystem.sql.sqlite.schema").contains("public"),
        )
      },
    )

  final private class Dummy(var value: String)
  private object Dummy:
    val handler: org.eclipse.serializer.persistence.binary.types.BinaryTypeHandler[Dummy] =
      org
        .eclipse
        .serializer
        .persistence
        .binary
        .types
        .Binary
        .TypeHandler(
          classOf[Dummy],
          org
            .eclipse
            .serializer
            .persistence
            .binary
            .types
            .Binary
            .Field(
              classOf[String],
              "value",
              (d: Dummy) => d.value,
              (d: Dummy, v: String) => d.value = v,
            ),
        )

  import scala.jdk.CollectionConverters.*
