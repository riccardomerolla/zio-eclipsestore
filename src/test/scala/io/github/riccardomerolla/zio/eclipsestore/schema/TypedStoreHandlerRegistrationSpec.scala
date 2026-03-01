package io.github.riccardomerolla.zio.eclipsestore.schema

import java.nio.file.{ Files, Path }

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.{ Schema, derived }
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.config.{ EclipseStoreConfig, StorageTarget }
import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object TypedStoreHandlerRegistrationSpec extends ZIOSpecDefault:

  // Domain types that exercise the bug: a case class containing a Scala 3 enum with fields.
  // Without handlersFor, EclipseStore uses its native reflective serialiser for IssueState,
  // which breaks Scala pattern matching after a store restart.
  final case class TrackedIssue(
    id: String,
    state: IssueState,       // Scala 3 enum with fields — from existing fixtures
    severity: IssueSeverity, // Scala 3 enum — from existing fixtures
    summary: String,
  ) derives Schema

  // Helper: build a scoped TypedStore-over-filesystem layer for a given directory,
  // using handlersFor[String, TrackedIssue] to pre-register handlers.
  // @nowarn: live is deprecated but the canonical pattern here is exactly the documented migration target.
  @annotation.nowarn("cat=deprecation")
  private def layerFor(dir: Path): ZLayer[Any, EclipseStoreError, TypedStore] =
    val configLayer = ZLayer.succeed(
      EclipseStoreConfig(storageTarget = StorageTarget.FileSystem(dir))
    )
    configLayer >>>
      TypedStore.handlersFor[String, TrackedIssue] >>>
      EclipseStoreService.live >>>
      TypedStore.live

  // Helper: write then close, reopen then read — simulates a JVM restart.
  private def restartRoundtrip(key: String, value: TrackedIssue)
    : ZIO[Any, EclipseStoreError, Option[TrackedIssue]] =
    ZIO.scoped {
      for
        dir <- ZIO.acquireRelease(
                 ZIO
                   .attempt(Files.createTempDirectory("typed-store-handler-reg"))
                   .mapError(e => EclipseStoreError.InitializationError("tmpdir", Some(e)))
               )(path =>
                 ZIO.attempt {
                   if Files.exists(path) then
                     Files.walk(path).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
                 }.orDie
               )
        _   <- TypedStore.store(key, value).provideLayer(layerFor(dir))
        out <- TypedStore.fetch[String, TrackedIssue](key).provideLayer(layerFor(dir))
      yield out
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("TypedStore handlersFor — handler auto-registration")(
      test("TrackedIssue with Scala 3 enum fields survives store restart via handlersFor") {
        val issue = TrackedIssue(
          id = "iss-1",
          state = IssueState.Closed(reason = Some("resolved")),
          severity = IssueSeverity.High(),
          summary = "Test issue for handler registration",
        )
        restartRoundtrip("issue:1", issue).map(out => assertTrue(out.contains(issue)))
      },
      test("IssueState.Open() variant also survives restart") {
        val issue = TrackedIssue(
          id = "iss-2",
          state = IssueState.Open(),
          severity = IssueSeverity.Low(),
          summary = "Open issue",
        )
        restartRoundtrip("issue:2", issue).map(out => assertTrue(out.contains(issue)))
      },
    )
