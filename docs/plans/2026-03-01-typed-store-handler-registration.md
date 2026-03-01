# TypedStore Handler Auto-Registration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `TypedStore.handlersFor[K, V]` — a config-enriching ZLayer that auto-derives and pre-registers EclipseStore binary type handlers for a K/V type pair before the store starts, fixing the silent serialization corruption described in issue #26.

**Architecture:** `handlersFor[K: Schema: ClassTag, V: Schema: ClassTag]` is a `ZLayer[EclipseStoreConfig, Nothing, EclipseStoreConfig]` that reads the config, calls `SchemaBinaryCodec.handlers` for both types, appends the resulting handlers to `config.customTypeHandlers`, and re-provides the enriched config. It is chained before `EclipseStoreService.live` in the wiring graph so handlers are registered at store startup. `TypedStore.live` is deprecated (annotation only, no code removal) so the existing `TypedStoreLive` implementation is untouched.

**Tech Stack:** Scala 3.5, ZIO 2.x, ZIO Schema 1.8.x, ZIO Test, SBT. Build and test with `sbt test`.

---

### Task 1: Write the failing integration test

The integration test is the specification. Write it first; it will fail to compile until Task 2 is done (because `handlersFor` doesn't exist yet). This test proves the end-to-end guarantee: store a value containing a Scala 3 enum via `TypedStore.store`, close and reopen the store using `handlersFor`, fetch the value, and assert it survives.

**Files:**
- Create: `src/test/scala/io/github/riccardomerolla/zio/eclipsestore/schema/TypedStoreHandlerRegistrationSpec.scala`

**Step 1: Write the failing test**

Create the file with this exact content:

```scala
package io.github.riccardomerolla.zio.eclipsestore.schema

import java.nio.file.{ Files, Path }
import java.time.Instant

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
```

**Step 2: Verify it fails to compile (handlersFor doesn't exist yet)**

```bash
sbt "test:compile" 2>&1 | grep -E "error|not a member|not found"
```

Expected output contains something like:
```
error: value handlersFor is not a member of object TypedStore
```

---

### Task 2: Implement `TypedStore.handlersFor[K, V]` and deprecate `live`

**Files:**
- Modify: `src/main/scala/io/github/riccardomerolla/zio/eclipsestore/schema/TypedStore.scala`

**Step 1: Add the import for ClassTag and SchemaBinaryCodec, add `handlersFor`, deprecate `live`**

Open `TypedStore.scala`. Make these changes:

1. Add `import scala.reflect.ClassTag` after the existing imports.
2. Add `import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig` after the existing imports.
3. Add `import io.github.riccardomerolla.zio.eclipsestore.schema.SchemaBinaryCodec` — note: this is in the same package so no import needed.
4. Add `@deprecated` annotation to `live`.
5. Add `handlersFor` after `live`.

The updated `object TypedStore` block (replace the entire `object TypedStore:` block from line 82 onward):

```scala
object TypedStore:
  import scala.reflect.ClassTag

  import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig

  /**
   * Layer constructor for `TypedStore`.
   *
   * @deprecated
   *   Prefer composing with [[handlersFor]] to ensure all stored types have a registered
   *   `BinaryTypeHandler` before the EclipseStore storage manager starts. Without handler
   *   registration, values containing Scala 3 enums or sealed traits may be serialised
   *   correctly on the first run but fail to deserialise correctly after a JVM restart.
   *
   *   Migration:
   *   {{{
   *   // Before
   *   EclipseStoreService.live >>> TypedStore.live
   *
   *   // After
   *   ZLayer.succeed(config) >>>
   *     TypedStore.handlersFor[String, MyValue] >>>
   *     EclipseStoreService.live >>>
   *     TypedStore.live
   *   }}}
   */
  @deprecated(
    "Use TypedStore.handlersFor[K, V] >>> EclipseStoreService.live >>> TypedStore.live to ensure correct binary handler registration before store startup.",
    since = "0.x",
  )
  val live: ZLayer[EclipseStoreService, Nothing, TypedStore] =
    ZLayer.fromFunction(TypedStoreLive.apply)

  /**
   * Config-enriching layer that derives and pre-registers `BinaryTypeHandler`s for key type `K`
   * and value type `V` before `EclipseStoreService.live` starts.
   *
   * Chain this layer between your `EclipseStoreConfig` provider and `EclipseStoreService.live`:
   *
   * {{{
   * val layer =
   *   ZLayer.succeed(EclipseStoreConfig.make(storagePath)) >>>
   *   TypedStore.handlersFor[String, AgentIssue] >>>
   *   EclipseStoreService.live >>>
   *   TypedStore.live
   * }}}
   *
   * For multiple value types, chain `handlersFor` calls:
   *
   * {{{
   * val layer =
   *   ZLayer.succeed(config) >>>
   *   TypedStore.handlersFor[String, AgentIssue] >>>
   *   TypedStore.handlersFor[String, ChatConversation] >>>
   *   EclipseStoreService.live >>>
   *   TypedStore.live
   * }}}
   *
   * EclipseStore deduplicates handlers by runtime class at startup, so overlapping handlers from
   * multiple `handlersFor` calls are harmless.
   */
  def handlersFor[K: Schema: ClassTag, V: Schema: ClassTag]
    : ZLayer[EclipseStoreConfig, Nothing, EclipseStoreConfig] =
    ZLayer.fromFunction { (config: EclipseStoreConfig) =>
      val keyHandlers   = SchemaBinaryCodec.handlers(Schema[K])
      val valueHandlers = SchemaBinaryCodec.handlers(Schema[V])
      config.copy(customTypeHandlers = config.customTypeHandlers ++ keyHandlers ++ valueHandlers)
    }

  def store[K: Schema, V: Schema](key: K, value: V): ZIO[TypedStore, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[TypedStore](_.store(key, value))

  def fetch[K: Schema, V: Schema](key: K): ZIO[TypedStore, EclipseStoreError, Option[V]] =
    ZIO.serviceWithZIO[TypedStore](_.fetch(key))

  def remove[K: Schema](key: K): ZIO[TypedStore, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[TypedStore](_.remove(key))

  def fetchAll[V: Schema]: ZIO[TypedStore, EclipseStoreError, List[V]] =
    ZIO.serviceWithZIO[TypedStore](_.fetchAll[V])

  def streamAll[V: Schema]: ZStream[TypedStore, EclipseStoreError, V] =
    ZStream.serviceWithStream[TypedStore](_.streamAll[V])

  def typedRoot[A: Schema](descriptor: RootDescriptor[A]): ZIO[TypedStore, EclipseStoreError, A] =
    ZIO.serviceWithZIO[TypedStore](_.typedRoot(descriptor))

  def storePersist[A: Schema](value: A): ZIO[TypedStore, EclipseStoreError, Unit] =
    ZIO.serviceWithZIO[TypedStore](_.storePersist(value))
```

**Step 2: Compile the main sources**

```bash
sbt compile 2>&1 | tail -5
```

Expected: `[success] Total time: ...`

---

### Task 3: Migrate `TypedStoreSpec` to `handlersFor`

The existing `TypedStoreSpec` uses `EclipseStoreService.inMemory >>> TypedStore.live`. Since `live` is now deprecated, migrate it to `handlersFor`. The inMemory service doesn't actually serialize, so `handlersFor` is a no-op for handler registration there — but the API migration is correct and removes the deprecation warning.

**Files:**
- Modify: `src/test/scala/io/github/riccardomerolla/zio/eclipsestore/schema/TypedStoreSpec.scala`

**Step 1: Update the layer and add needed imports**

Replace the `private val layer = EclipseStoreService.inMemory >>> TypedStore.live` line.

The test suite stores `User` and `AgentIssueRow` values under `String` keys, so the layer needs `handlersFor[String, User]`. The suite also has a mixed test storing `AgentIssueRow` — that's fine because `handlersFor` just enriches the config (which inMemory ignores anyway). Use `handlersFor[String, User]` for the primary layer; `AgentIssueRow` tests will also pass because inMemory is schema-agnostic.

Add this import at the top of the file:

```scala
import io.github.riccardomerolla.zio.eclipsestore.config.EclipseStoreConfig
```

Replace:

```scala
private val layer = EclipseStoreService.inMemory >>> TypedStore.live
```

With:

```scala
@annotation.nowarn("msg=deprecated")
private val layer =
  ZLayer.succeed(EclipseStoreConfig.temporary) >>>
    TypedStore.handlersFor[String, User] >>>
    TypedStore.handlersFor[String, AgentIssueRow] >>>
    EclipseStoreService.inMemory.fresh >>>
    TypedStore.live
```

Wait — `EclipseStoreService.inMemory` doesn't take `EclipseStoreConfig` as input. It's a `ULayer[EclipseStoreService]`. The `handlersFor` layer produces `EclipseStoreConfig` but `inMemory` doesn't consume it. They can coexist using `++` or by keeping `inMemory` separate.

The cleanest migration: keep `inMemory` for the service (it's the test double), and provide the config just for `handlersFor`'s type derivation side. The resulting config output is unused since `inMemory` ignores it, but the layer still compiles.

Use this instead:

```scala
@annotation.nowarn("msg=deprecated")
private val layer =
  (ZLayer.succeed(EclipseStoreConfig.temporary) >>>
    TypedStore.handlersFor[String, User] >>>
    TypedStore.handlersFor[String, AgentIssueRow]).orDie ++
    EclipseStoreService.inMemory >>>
    TypedStore.live
```

Actually the simplest correct migration that removes the deprecation warning is:

```scala
// Suppress deprecation: inMemory is used in tests where serialization is irrelevant.
// For production use, compose with TypedStore.handlersFor[K, V] instead.
@annotation.nowarn("msg=deprecated")
private val layer: ULayer[TypedStore] =
  EclipseStoreService.inMemory >>> TypedStore.live
```

This is honest: the test uses in-memory storage which has no serialization at all. The `@nowarn` suppresses the deprecation warning while the comment explains why `live` is still acceptable here.

**Step 2: Compile**

```bash
sbt "test:compile" 2>&1 | grep -E "error|warning|deprecated" | head -10
```

Expected: No `error:` lines. The `deprecated` warning on `TypedStore.live` should be suppressed by `@nowarn`.

---

### Task 4: Run all tests

**Step 1: Run the new integration test**

```bash
sbt "testOnly io.github.riccardomerolla.zio.eclipsestore.schema.TypedStoreHandlerRegistrationSpec" 2>&1 | tail -15
```

Expected:
```
+ TypedStore handlersFor — handler auto-registration
  + TrackedIssue with Scala 3 enum fields survives store restart via handlersFor
  + IssueState.Open() variant also survives restart
2 tests passed. 0 tests failed.
```

**Step 2: Run the migrated TypedStoreSpec**

```bash
sbt "testOnly io.github.riccardomerolla.zio.eclipsestore.schema.TypedStoreSpec" 2>&1 | tail -10
```

Expected: All 5 existing tests pass.

**Step 3: Run the full test suite**

```bash
sbt test 2>&1 | tail -5
```

Expected: All tests pass, 0 failures.

---

### Task 5: Commit

```bash
git add \
  src/main/scala/io/github/riccardomerolla/zio/eclipsestore/schema/TypedStore.scala \
  src/test/scala/io/github/riccardomerolla/zio/eclipsestore/schema/TypedStoreSpec.scala \
  src/test/scala/io/github/riccardomerolla/zio/eclipsestore/schema/TypedStoreHandlerRegistrationSpec.scala \
  docs/plans/2026-03-01-typed-store-handler-registration-design.md \
  docs/plans/2026-03-01-typed-store-handler-registration.md

git commit -m "$(cat <<'EOF'
feat: add TypedStore.handlersFor[K,V] to pre-register binary type handlers (#26)

Adds TypedStore.handlersFor[K: Schema: ClassTag, V: Schema: ClassTag], a
ZLayer[EclipseStoreConfig, Nothing, EclipseStoreConfig] that derives and appends
SchemaBinaryCodec.handlers for both K and V before EclipseStoreService.live starts.

Without this, TypedStore.store silently used EclipseStore's native reflective serialiser
for types without a registered BinaryTypeHandler, causing values containing Scala 3 enums
or sealed traits to deserialise incorrectly after a JVM restart (None.get / MatchError).

TypedStore.live is deprecated; production code should compose:
  ZLayer.succeed(config) >>>
    TypedStore.handlersFor[String, MyValue] >>>
    EclipseStoreService.live >>>
    TypedStore.live

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Notes for Implementer

- `SchemaBinaryCodec.handlers(Schema[K])` already handles both the primary handler and enum-case subtype handlers (including the issue #25 fix for `transformOrFail` schemas). No extra work needed there.
- The `config.copy(customTypeHandlers = ...)` append is safe: `EclipseStoreService`'s `mergedTypeHandlers` deduplicates by runtime class, so registering a handler twice is harmless.
- The `@deprecated` annotation uses `since = "0.x"` as a placeholder; adjust to the actual release version when cutting a release.
- `EclipseStoreConfig.temporary` is already defined in `EclipseStoreConfig.scala` as `EclipseStoreConfig(StorageTarget.InMemory())`.
