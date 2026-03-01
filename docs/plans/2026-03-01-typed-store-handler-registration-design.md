# Design: TypedStore Handler Auto-Registration (Issue #26)

**Date:** 2026-03-01
**Status:** Approved

## Problem

`TypedStore.store[K, V]` validates values via JSON round-trip but delegates to `underlying.put(key, value)`, which goes through EclipseStore's native binary serialiser unless a `BinaryTypeHandler` is registered for `V`'s runtime class. Handlers are registered once at store startup via `EclipseStoreConfig`. For types stored via `TypedStore`, no handler is registered by default, causing silent data corruption that only manifests after a JVM restart.

## Root Cause

The `kv-root` `RootDescriptor` has no associated schema, so `autoRegisterSchemaHandlers` produces no handlers for values stored through the key-value API. Users must manually add `customTypeHandlers` to `EclipseStoreConfig` — an easy-to-miss step with no compile-time or runtime warning.

## Chosen Direction

**Option A: Auto-derive handlers at config time.**
A new `TypedStore.handlersFor[K: Schema: ClassTag, V: Schema: ClassTag]` layer reads `EclipseStoreConfig`, appends `SchemaBinaryCodec.handlers` for both K and V, and re-provides the enriched config. This runs before `EclipseStoreService.live`, ensuring handlers are registered at store startup.

## API

### New: `TypedStore.handlersFor[K, V]`

```scala
def handlersFor[K: Schema: ClassTag, V: Schema: ClassTag]
  : ZLayer[EclipseStoreConfig, Nothing, EclipseStoreConfig]
```

Derives `BinaryTypeHandler`s for `K` and `V` (including enum case subtype handlers) and appends them to `config.customTypeHandlers`. EclipseStore deduplicates by type at startup, so calling `handlersFor` multiple times for overlapping types is harmless.

### Canonical wiring pattern

```scala
val layer =
  ZLayer.succeed(EclipseStoreConfig.make(storagePath)) >>>
  TypedStore.handlersFor[String, AgentIssue] >>>
  TypedStore.handlersFor[String, ChatConversation] >>>  // chain for multiple types
  EclipseStoreService.live >>>
  TypedStore.live
```

### Deprecated: `TypedStore.live`

`TypedStore.live` is deprecated. Production code should use `handlersFor[K, V] >>> EclipseStoreService.live >>> TypedStore.live`. The `live` implementation is unchanged — only the documentation and deprecation annotation change.

## Files to Change

| File | Change |
|------|--------|
| `TypedStore.scala` | Add `handlersFor[K, V]`; deprecate `live` |
| `TypedStoreSpec.scala` | Migrate layer to use `handlersFor` pattern |
| New: `TypedStoreHandlerRegistrationSpec.scala` | Restart-roundtrip integration test proving handler auto-registration works |

## Testing Strategy

1. **Unit tests (`TypedStoreSpec`)** — migrate existing layer to `handlersFor`. Since they use `EclipseStoreService.inMemory` (no serialization), this is primarily an API migration exercise.
2. **Integration test (`TypedStoreHandlerRegistrationSpec`)** — uses `EclipseStoreService.live` with a temp directory, stores a value containing a Scala 3 enum via `TypedStore.store`, restarts the store, and verifies the value round-trips correctly. This is the definitive regression test for issue #26.

## Non-Goals

- Dynamic (post-startup) handler registration — EclipseStore seals its handler registry at startup.
- Changing `TypedStore`'s trait or method signatures.
- Auto-inferring types without explicit `handlersFor` call (would require macro/implicit magic).
