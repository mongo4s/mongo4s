# Changelog

[← back to the README](README.md)

Notable changes per release. Breaking changes and how to migrate are in
[COMPATIBILITY.md](COMPATIBILITY.md); what is not covered yet is in [ROADMAP.md](ROADMAP.md).

This file starts at `2.0.0`. `1.0.0` was released without one, and `1.1.0` was never published — its changes are
part of `2.0.0` below.

## 3.0.0

### Added

- `TransactionOptions`, carrying a transaction's `readConcern`, `writeConcern`, `readPreference` and
  `maxCommitTime` — and the retry window below. Both `withTransaction` methods take one.
- `FindQuery.selectAs[K]`, a partial read into a named tuple: `find(...).selectAs[(name: String, age: Int)]` builds
  the projection and the decoder from one shape, so they cannot drift. A label that is not a field of the entity, or
  one asked for at the wrong type, is a compile error. It works on a direct collection too, where a partial read was
  not possible before, because the entity's `WireCodec` demands every modelled field.
- Geospatial filters: `near`, `nearSphere`, `within` and `intersects` on a `Field`, over a GeoJSON `Geometry`
  vocabulary (`Point`, `LineString`, `Polygon`, and the `Multi*` forms) plus the legacy `2d` shapes in `GeoShape`.
  A polygon ring that does not close is refused rather than sent.
- `Stage.bucketBy`, `Stage.densify` and `Stage.setWindowFields` — `$bucket`, `$densify` and `$setWindowFields`,
  with `DensifyRange`/`DensifyBounds`/`DateUnit` and `Window`/`WindowBound`/`WindowOutput` for the vocabulary they
  need. `$bucket`'s boundaries are values of the field being grouped rather than raw BSON.
- `CreateCollectionOptions`, taken by `MongoDatabase.createCollection`: capped size and document cap, a validator
  with its level and action, time-series and clustered collections, `expireAfter`, collation and storage engine.
  Combinations the server accepts and then ignores — a document cap without `capped`, a validation level without a
  validator — are rejected rather than sent.
- `WatchOptions.withExpandedEvents` — the driver's `showExpandedEvents`, so DDL events arrive alongside document
  ones. Needs MongoDB 6.0, and the flag is sent only when asked for.
- `ChangeEvent.wallTime` and `ChangeEvent.splitEvent`: the server's wall clock for the change, and which fragment of
  how many an oversized event was split into.
- `Effect.monotonic`, with a default reading `System.nanoTime`, so implementors are unaffected; `mongo4s-cats` and
  `mongo4s-zio` override it with their runtime's own clock.

### Changed

- **`Scala 3.9 LTS` is now required**, and it is the only Scala version the build uses. `mongo4s-bson-calypso`,
  `mongo4s-kyo` and `mongo4s-rapid` were pinned to a fast-release `3.8` because their dependencies needed a
  compiler newer than `3.3 LTS`; they are now on the LTS line with every other module. It takes a major version
  because `TASTy` does not read forward, so a `3.3 LTS` project cannot consume these artifacts, and MiMa cannot see
  that break. `3.3 LTS` is no longer supported.
- `withTransaction` **retries the way the driver's own does**: a `TransientTransactionError` restarts the whole
  transaction, an `UnknownTransactionCommitResult` retries only the commit, both bounded by one 120-second deadline
  taken before the first attempt. `2.x` reported the first failure; `TransactionOptions.withoutRetries` restores
  that. This is the one change in the release with no compile error behind it.
- Compound `PrimaryKey`s are **named tuples**: `PrimaryKey.compound(o => (userId = o.userId, seq = o.seq))`
  replaces the positional tuple plus its parallel list of names and `_._1`/`_._2` extractors. Any width works, so
  `compound3` and `compound4` were removed.
- `MongoDatabase.createCollection` takes a `CreateCollectionOptions`, defaulted, so the no-options call is
  unchanged at the source level.
- `WatchOptions` is a `final class` with `withX` builders instead of a `case class` — same reason `Index` and
  `WireCodecConfig` already were. Build from `WatchOptions.default[E]`; reading its fields is unchanged.
- The accessors the compiler synthesizes for `Field.of` and the two `WireCodec` derivations are pinned with
  `@publicInBinary`. They were binary-unstable — a name MiMa cannot check because it is synthesized, not declared.
- `mongo4s-cats` builds against `fs2 3.14.0`. Its Reactive-Streams interop is unchanged from `3.13.0` — the module
  is identical between the two releases — so nothing about streaming behaviour moves with the bump.
- Internally, every `given` moved to the syntax SIP-64 introduced in `3.6` (`given name: [A] => (dep: D) => T`), so
  the whole codebase compiles under `-source:future`. This changes how instances are declared, not what they are,
  and nothing about it is visible to a caller.

### Fixed

- `RsBridge.unit` reported a publisher's failure wrapped in a `CompletionException` on backends that do not unwrap
  one themselves, which hid the driver's own exception type — and with it the error labels the new transaction
  retries depend on. It now reports the exception the publisher raised, on every backend.

## 2.0.0

### Added

- Read and write concerns per database and collection: `withReadConcern`, `withWriteConcern`, `withReadPreference`,
  each returning a new handle, so per-operation control is chaining.
- `mongo4s-testkit`, a new published module carrying `FakeMongoCollection` — previously unpublished test sources —
  and a new `FakeRepository`, a real `BaseMongoRepository` over a fake collection.
- `arrayFilters` on the update operations, with `$[]` and `$[identifier]` built as ordinary path segments.
- `collation`, `hint` and `comment` on every write; `bypassDocumentValidation` where a whole document is written;
  `limit`, `skip` and `maxTime` on `count`.
- `$each` modifiers on `pushAll`: `$slice`, `$sort`, `$position`.
- Index types `hashed`, `2dsphere` and `2d`, plus `hidden`, `collation` and `wildcardProjection`.
- `$out` and `$merge` take a target database, and `$merge` its `on`/`whenMatched`/`whenNotMatched` policies.
- `$lookup` in its sub-pipeline form with `let`, and `$graphLookup`.
- `WireCodecConfig.omitNoneFields` (default on): a `None` field is left out rather than written as `null`.
- `BsonTypeName` is public and an enum.

### Changed

Every item is a compile error rather than a silent behaviour change. See
[COMPATIBILITY.md](COMPATIBILITY.md) for the migration.

- Write options moved from method parameters into values: `UpdateOptions`, `ReplaceOptions`, `DeleteOptions`,
  `CountOptions`, `FindOneAndUpdateOptions[A]`, `FindOneAndReplaceOptions[A]`, `FindOneAndDeleteOptions[A]`.
  `deleteOne`, `deleteMany` and `count` gained an options parameter; `WriteCommand` and `Repository` follow.
- `Projection` split into inclusion and exclusion types — mixing them no longer compiles.
- `Index` and `WireCodecConfig` are `final class`es with `withX` builders instead of `case class`es.
- `Field.hasType` takes `BsonTypeName`; `BsonError.TypeMismatch` carries it in both fields.
- `MongoCollection` and `MongoDatabase` gained three abstract methods; `Stage` gained two cases.
- `FakeMongoCollection` moved to `mongo4s.testkit`.

### Fixed

- A sealed hierarchy stored as the root entity of a direct collection could be written but never read back: the
  decoder demanded `_type` first, and the server stores `_id` first. `Either` and `Ior` had the same defect.
- Every write threw `UnsupportedOperationException` under an unacknowledged write concern (`w=0`).
- `Decimal128` lost precision above 2^53; the `Long` range check admitted 2^63; a non-finite value threw past the
  `Either`.
- An explicit `BsonEncoder`/`BsonDecoder` pair was ambiguous with automatic `WireCodec` derivation, so a type
  carrying both resolved no codec.
- `cats.data.Ior` decoded its two sides by position, swapping them when the stored order differed.
- `Sort` and `Index` kept a repeated field's original position.
- `insertMany` returned ids in the driver's map iteration order rather than by command position.
- `aggregate(...).first` appended `$limit` after `$out`/`$merge`, which the server rejects.
- A mixed projection silently returned extra fields; `excludeId` followed by `include` lost the `_id` exclusion.
- `WatchOptions`' three starting points were not mutually exclusive.
- A bare discriminator for a case with fields raised `IndexOutOfBoundsException`; a subtype declaring its own
  `_type` field wrote duplicate BSON keys.
- rapid's `MongoClientResource` leaked the client when `use` threw while its task was being built.
- zio's `bracketCase` swallowed a finalizer error on success.
- kyo's `stream` failed as a `ClassCastException` when handed a `Streamable` it had not derived.
- Documentation: a broken repository example, the `Effect` member list, the module `WithId` belongs to, the
  `watchAsAttempting` naming, and the claim that nothing but the imports changes when swapping runtimes.
