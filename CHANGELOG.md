# Changelog

[← back to the README](README.md)

Notable changes per release. Breaking changes and how to migrate are in
[COMPATIBILITY.md](COMPATIBILITY.md); what is not covered yet is in [ROADMAP.md](ROADMAP.md).

This file starts at `2.0.0`. `1.0.0` was released without one, and `1.1.0` was never published — its changes are
part of `2.0.0` below.

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
