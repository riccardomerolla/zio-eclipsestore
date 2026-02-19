# Gap Tracking: ZIO EclipseStore Wrapper vs Upstream EclipseStore

Status legend: ✅ done · 🔄 in progress · ⏳ planned

## Priority 1 — Custom type handlers & Blobs (✅)
- [x] Port custom type handler example (e.g., Employee, Order with Binary handlers) into ZIO test/example.
- [x] Port blob handling example (binary/image) into ZIO test/example.
- [x] ZIO layer/wiring for registering custom `BinaryTypeHandler`/custom value handlers.
- [x] Tests covering store/load roundtrips for custom handlers and blob data.

## Priority 2 — Filesystems & backup target adapters (✅)
- [x] Add adapters/layers for S3 and SQLite backup targets (property generation + wiring).
- [x] Add generic SQL + FTP backup targets.
- [x] Configuration coverage for backup properties; property injection tested.

## Priority 3 — Reloader / eager-storing / deleting examples (✅)
- [x] Port reloader example (restart-safe reload) into ZIO form with tests.
- [x] Port eager-storing behavior example.
- [x] Port deleting example semantics and tests.

## Priority 4 — Gigamap advanced scenarios (🔄 in progress)
- [x] Add advanced Gigamap indexed query scenario (basic example parity) with tests.
- [x] Add restart/resilience scenario with large dataset + parallel updates/removals.
- [ ] (Optional) Extend CLI/tests to cover hedging/partitioning/very-large datasets.

## Priority 5 — Performance tuning config & tests (✅)
- [x] Expose and validate performance knobs (channel count, caches, compression/encryption, off-heap).
- [x] Tests asserting tuning + backup properties are applied to the underlying foundation.

## Already covered
- Lazy loading primitives and lazy collections.
- Backup/import-export basics.
- SQLite storage target and zio-config loading.
- Gigamap core CRUD/queries + CLI/bookstore demos.
- Base EclipseStoreService CRUD/batch/query support.
