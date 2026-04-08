# Scala-Native Engine Contract

This document captures the baseline API contract for evolving `zio-eclipsestore` from a wrapper-oriented library into a Scala-native object-graph persistence engine.

## Public API Boundary Rules

The public Scala surface must follow these rules:

- Public storage APIs return `ZIO[R, E, A]`, `IO[E, A]`, `UIO[A]`, or `ZStream[R, E, A]`.
- Typed failures belong to the `PersistenceError` hierarchy or a narrower subsystem ADT that extends it.
- Java-centric types do not leak into userland APIs:
  - no `java.util.List`
  - no raw `Object`
  - no `null`-based API contracts
  - no throws-based domain APIs
- `Schema[A]` remains the userland serialization boundary.
- `ZLayer` and `Scope` remain the wiring and lifecycle primitives.

## Current Module Topology

The repository currently implements these modules:

- `zio-eclipsestore` (root): core service, config, schema integration, lifecycle APIs
- `zio-eclipsestore-gigamap`: indexed map and vector-search support
- `zio-eclipsestore-storage-sqlite`: SQLite storage adapter
- `examples/bookstore`
- `examples/gigamap-cli`
- `examples/lazy-loading`

This is the implemented topology in `build.sbt` today and is the starting point for the roadmap.

## Target Module Topology

The roadmap expands the current topology toward these subsystem boundaries:

- `core`
- `serializer`
- `gigamap`
- storage backends (`filesystem`, `sqlite`, `s3`, `azure`, `redis`, and related targets)
- `observability`
- `testkit`
- `examples`
- `docs`

The current repository layout does not yet split all of these into separate sbt subprojects. This document defines the intended direction so later milestones can refine the build without changing the public contract ad hoc.

## Version Compatibility Matrix

| Line | EclipseStore | Scala | ZIO | Status |
|---|---|---|---|---|
| Current main branch | 4.0.1 | 3.5.2 | 2.1.24 | Active implementation baseline |
| Scala-native engine roadmap | 4.x | 3.5+ | 2.1+ | Target contract for milestones `#35`-`#49` |

Compatibility should be updated intentionally when one of these version anchors changes.
