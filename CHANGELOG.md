# Changelog

[← back to the README](README.md)

Notable changes per release. Breaking changes and how to migrate are in
[COMPATIBILITY.md](COMPATIBILITY.md); what is not covered yet is in [ROADMAP.md](ROADMAP.md).

This file starts at `2.0.0`. `1.0.0` was released without one, and `1.1.0` was never published — its changes are
part of `2.0.0` below.

## 3.0.0

### Added

- `Repository.getByField`, `updateByField` and `deleteByField`, completing the grid the renames above opened up:
  every condition-based operation now has both a field-and-value and a whole-`Filter` spelling. Each is its
  `…ByFilter` counterpart over `field.equalTo(value)`.
- **`WireCodec` no longer derives itself for any case class it meets.** It comes from `derives WireCodec`, from a
  `given` you wrote, or from an existing `BsonEncoder`/`BsonDecoder` pair. A field whose type has no codec is a
  compile error naming the type, rather than a silently derived codec — and an `opaque type` keeps the
  representation you give it. The cases of an `enum` and the leaves of a sealed trait still derive with their
  parent, because the language gives them nowhere to write `derives`.
- `Filter`, `Update`, `Stage` and `Accumulator` expose `key`, the MongoDB operator each case renders as, declared on
  the case instead of repeated in `toBson`.
- `Sort.byTextScore` — `{"$meta": "textScore"}`, so a `$text` search can be ranked by relevance. `Sort` now carries a
  `SortOrder` per field instead of a `Boolean`.
- `Projection.slice` and `Projection.sliceFrom` — `$slice`, chainable onto a neutral, inclusive or exclusive
  projection, and refused at compile time on a field that is not an array. Simulated by the fake.
- `Index.text` and `Index.geo2D` on the companion, which had shortcuts for every other direction but these two.
- `ExplainSummary.of`, which reads the questions worth asking out of an explain document — indexes used, stages
  present, whether it scanned the collection or sorted in memory, and the execution counters. It walks for field
  names rather than a fixed path, so one summary covers a find, a pipeline the server optimised into a find, and a
  pipeline it kept as stages.
- `explain` on `FindQuery` and `AggregateQuery`, taking an optional `ExplainVerbosity` and returning the server's
  plan as a `BsonDocument`. It explains the query the builder already produced, and the whole pipeline rather than
  the `$limit`-ed form `first` sends. The fake refuses it by name.
- `MongoError`, a typed failure for everything the server reports: `DuplicateKey`, `WriteConflict`,
  `ExecutionTimeout`, `Unauthorized`, `Unavailable`, `BulkWriteFailed` and a `Failed` catch-all that keeps the
  server's code. Translation happens on the `Publisher`, so every runtime and every bridge method — `one`, `option`,
  `list`, `unit`, `stream` — reports the same type for the same failure, and the driver's exception stays reachable
  as `cause`.
- `FakeMongoCollection.distinct` is simulated, array fields flattened into their elements as the server does, and
  checked against a real server by a parity spec.
- `FakeMongoCollection.aggregate` now simulates `$match`, `$sort`, `$skip`, `$limit`, `$project`, `$count` and
  `$group` (with `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`) in memory, so service code that
  aggregates is unit-testable without Docker. Anything outside that subset still throws by name, `$addToSet`
  included — MongoDB leaves the order of its result undefined. A parity spec checks the subset against a real server,
  BSON types and all.
- `ScalarWireCodecBenchmark`, `AggregateBenchmark` and `ErrorTranslationBenchmark`, and the numbers they produced,
  in [BENCHMARKS.md](BENCHMARKS.md): what a bridged scalar costs, what `aggregateDirect` is worth against a real
  server at ten documents and at ten thousand, and what the `MongoError` wrapper costs on a publisher that never
  fails — nothing measurable, at one element and at ten thousand alike.
- Native `WireCodec` instances for `BigDecimal`, `Instant`, `UUID` and `ObjectId`. Before this, the AST-free path
  had them only through the `BsonEncoder`/`BsonDecoder` bridge — one `BsonValue` per field, on types almost every
  entity carries. The BSON written is unchanged, so existing collections read and write exactly as before.
- `TransactionOptions`, carrying a transaction's `readConcern`, `writeConcern`, `readPreference` and
  `maxCommitTime` — and the retry window below. Both `withTransaction` methods take one.
- `Repository.findPage(limit, after, filter)`: keyset paging over the primary key. The repository builds the sort
  and the comparison that steps past the cursor itself, lexicographically for a compound key. Unlike `skip` it does
  not re-scan, and a row deleted earlier does not shift the rows still to come.
- `MongoCollection.aggregateDirect[B]`, which decodes a pipeline's output through a `WireCodec[B]` rather than a
  `BsonDocumentCodec[B]`. On a direct collection that is AST-free, the same trip `find` makes there; on any other
  collection the codec is bridged, so the call means the same thing either way. It carries the direct path's
  numeric strictness with it.
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

- **`Repository`'s condition-based methods say what they take.** `findBy` is `findByField`, `updateBy` is
  `updateByFilter`, `deleteBy` is `deleteByFilter` and `getBy` is `getByFilter`. `By` used to mean a field in
  `findBy` and a filter in `updateBy`/`deleteBy`/`getBy`, while the filter-taking read was `findByFilter` — so the
  same idea had two names and one name had two meanings.
- **`Repository.insertOne` returns `K` and `insertMany` returns `List[K]`**, instead of the driver's
  `Option[org.bson.BsonValue]` and `List[org.bson.BsonValue]`. A repository is parameterised on its key type and can
  name the key itself through `PrimaryKey`, so it never had a reason to hand back raw BSON for the caller to decode;
  the keys come back in insertion order, across batches. `MongoCollection`, which has no key type, still returns
  `InsertOneResult`/`InsertManyResult` exactly as the driver reports them. Code that ignored the result — which is
  how the result was used everywhere it appeared — needs no change; code that decoded the `BsonValue` should drop the
  decode.
- **The direct path reads numbers the way the `BsonValue` path does.** A `Long` field stored as an `Int32`, a
  `Double` stored as a whole `Int64`, and every other lossless width now decode through `getDirectCollection` as
  they always did through `getCollection`; what would lose information is still refused on both. The rule lives in
  `BsonDecoder` alone and the wire codecs defer to it. A wrong type is now a `BsonError.TypeMismatch` rather than a
  raw `org.bson.BsonInvalidOperationException`, and `BsonError.fromThrowable` unwraps a `DecodingFailure` instead of
  re-wrapping it as `Thrown`.
- **Driver exceptions no longer reach your code from an operation.** `com.mongodb.MongoWriteException` and friends
  arrive as `MongoError` instead; code that caught the driver's types directly has to match on `MongoError`, or on
  `cause`, which still holds the original. Errors `mongo4s` raises itself — `BsonError.DecodingFailure`,
  `RsBridgeError` — are unchanged.
- `RsBridgeConfig` is a `final class` with a private constructor and `withX` builders, and `RsBridgeConfig.Default`
  is now `RsBridgeConfig.default` — the same shape every other options type already had, and the last one that could
  not gain a field without a major. `bufferSize` must now be positive.
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

- **`FakeMongoCollection` reported an upsert that inserted as if it had done nothing.** `replaceOne` with
  `upsert = true` stored the replacement without stamping an `_id`, so `upsertedId` came back `None` and
  `wasUpserted`/`wasApplied` came back `false` where the server returns the new id and `true` — a test asserting on
  `wasUpserted` passed against the fake and inverted against a real server. The bulk path was worse: it reported the
  entity's own `id` field, or the whole document as JSON when there was none, instead of the `_id`. Both now go
  through the same `_id` stamping and duplicate check `insertOne` uses, as does a `bulkWrite` insert, which was
  skipping it too. A parity spec pins all four against MongoDB 8.2.
- `Streamable.instance` is `private[mongo4s]`. It satisfied any `Streamable[S, A]`, so a hand-written `given` for
  kyo's stream type compiled and then threw at runtime, because kyo's bridge needs the instance it derives itself.
  A runtime module still supplies one for its own stream type; nothing else can forge one.
- `Effect.traverse` is `Effect.flatTraverse`: its function returns `F[List[B]]` and the result is flattened, which
  is not what `traverse` means anywhere else.
- `mongo4s.bson.DecodeResult` moved from `mongo4s-core` to `mongo4s-bson-core`, so the `mongo4s.bson` package lives
  in one artifact instead of being split across two.
- `$min` and `$max` were gated on `NumericOf`, so `lastSeen.max(now)` on an `Instant` field did not compile although
  MongoDB orders dates perfectly well. They take a new `ComparableOf` — numbers, `String`, `Instant`, `ObjectId`,
  `UUID`, `Boolean`, `BigDecimal`, and their `Option`s. `$inc` and `$mul` remain numeric.
- `aggregate[B]` asked for a `BsonDocumentCodec[B]` while only ever decoding, so a read model had to carry an
  encoder that nothing called. It takes a `BsonDocumentDecoder[B]` now, like `watchAs` beside it.
- A `snake_case` (or otherwise renamed) `WireCodec` and the queries against its collection could disagree about
  field names, and every query then matched nothing. `getDirectCollection` no longer takes a `naming` at all: a
  derived codec reports the `WireCodecConfig` it was built with, a hand-written one declares its own by overriding
  `fieldNaming`, and the queries follow whichever it is.
- `selectAs` reported `MissingField` for an `Option` label whose field the document did not carry, while reading the
  whole entity gave `None` for the very same document. The absent value now goes to the decoder, which is what makes
  the two agree; a field the entity requires is still reported as missing.
- `PublisherIterator.cancel` cancelled the subscription but left no terminal in the queue, so a consumer already
  parked in `hasNext` stayed parked — a leaked thread per cancelled or timed-out `rapid` stream. It now enqueues the
  end of the stream, and a spec asserts the parked consumer is released.
- `ReadmeReferencesSpec` reads `README.md` and fails if a Scala block names an `Object.member` the source does not
  define. The compiled snippets under `examples/` are re-typed copies, so they could be right while the README was
  wrong — which is exactly how `Index.geo2dsphere` survived beside the `geo2DSphere` that exists.
- `Repository.bulkWrite` had no `ordered` flag, although the collection underneath does, and it always batched — so
  an unordered bulk stopped at the first batch that failed rather than applying everything it could. It takes
  `ordered` now, batches only when ordered (where batching preserves the meaning), and sends an unordered bulk as
  one command.
- **`FakeMongoCollection` simulated four update operators out of eighteen** while the documentation said updates
  were simulated, so a unit test reaching for `push`, `pull`, `pop`, `rename`, `mul`, `min`, `max`, `addToSet` or
  `currentDate` failed with `UnsupportedOperationException`. All of them are simulated now, `$push`'s
  `$each`/`$position`/`$slice`/`$sort` modifiers included, and `FakeUpdateParityItSpec` checks each against a real
  server.
- **`FakeMongoCollection` answered several queries differently from the server.** A filter over an array field
  compared the whole array to the value, so `contains` never matched and `$ne` always did; a dotted path through an
  array of documents resolved to nothing; `$regex` anchored the whole string and dropped its options; `$inc`
  truncated its amount to a `Long` and rewrote the field as `Int64`, after which the fake could not find the row it
  had just updated; `$count` reported `0` where the server reports nothing; `findOneAndUpdate`/`Replace`/`Delete`
  ignored their `sort`; `$eq`/`$ne` against `null` disagreed with the server about a missing field; sorting or
  comparing a date, `ObjectId` or boolean threw; `$first`/`$last` skipped documents missing the field instead of
  taking the first or last; a bulk `UpdateOne`/`UpdateMany` silently discarded its `UpdateOptions`; inserts neither
  stamped an `_id` nor refused a duplicate one. Every one of these is now checked against a real MongoDB by
  `FakeFidelityParityItSpec`.

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
