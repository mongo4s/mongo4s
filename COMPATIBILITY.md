# Compatibility

[← back to the README](README.md)

## What the artifacts promise

* Binary compatibility within a major version is checked by [MiMa](https://github.com/lightbend/mima) on every
  build; `versionScheme := "semver-spec"` describes what the artifacts promise.
* New methods added to `Effect` and `RsBridge` carry default implementations, so implementing either typeclass
  yourself keeps working across minor releases.
* Deprecations get at least one minor release before removal.
* Every deliberate break inside a major version is listed here and carries a matching MiMa exclusion in
  `build.sbt`, so nothing is waived silently. `3.0.0` has no such exceptions: it is a major release, and everything
  below is covered by that.

## Migrating from 2.x to 3.0.0

`3.0.0` requires **`Scala 3.9 LTS`**, and changes three things in the API: compound `PrimaryKey`s are named tuples,
`withTransaction` retries the way the driver does, and `WatchOptions` became a builder. Nothing else moved.

### Why this is a major release

`Scala 3 TASTy` is backward but not forward compatible, and a Scala 3 artifact carries the same `_3` suffix whatever
minor compiled it. So artifacts built on `3.9` **cannot be read from a `3.3 LTS` project**, while nothing about the
bytecode changed. MiMa cannot see that break — it compares signatures, not TASTy versions — which is exactly why it
takes a major release rather than a silent minor.

`3.3 LTS` is no longer supported. The replacement is the current LTS, not a fast-release line.

### Compound keys are named tuples

`compound` used to pair a positional tuple with a separate list of names and extractors, and `compound3`/`compound4`
extended it one arity at a time. All three are replaced by one `compound` over a named tuple:

```scala
// 2.x
given PrimaryKey[Order, (String, Int)] =
  PrimaryKey.compound(o => (o.userId, o.seq))("user_id", _._1)("seq", _._2)

// 3.0
given PrimaryKey[Order, (userId: String, seq: Int)] =
  PrimaryKey.compound(o => (userId = o.userId, seq = o.seq), FieldNaming.snakeCase)
```

The labels are the field names, so the two lists that had to agree became one, and `_._1`/`_._2` — which type-check
just as happily when swapped, as long as the two fields share a type — are gone. There is no arity ceiling either,
so `compound3` and `compound4` have nothing left to do and were removed.

Field names are still *stored* names. The `FieldNaming` argument is what lets the Scala labels stay idiomatic while
the stored spelling differs; leave it out and the labels are used verbatim, exactly as the old string arguments were.

Key values are written with labels at every call site — `users.findOne((userId = "u1", seq = 3))` — and their order
is part of the key's type, so a call that writes them in another order does not compile.

### Transactions retry by default

`withTransaction` now does what the driver's own does: a `TransientTransactionError` restarts the whole transaction,
an `UnknownTransactionCommitResult` retries just the commit, both bounded by one 120-second deadline taken before
the first attempt. In `2.x` the first failure was reported and nothing was retried.

This is a **behaviour change with no compile error behind it**, so it is the one item here to read twice. It only
affects failures the server itself labelled — an error from your own code inside the body still fails the
transaction immediately. If you already retry around `withTransaction`, you now have two layers; drop yours, or:

```scala
client.withTransaction(body, TransactionOptions.withoutRetries) // exactly the 2.x behaviour
```

Both `withTransaction` methods gained an optional `TransactionOptions` parameter, which also carries the
transaction's `readConcern`, `writeConcern`, `readPreference` and `maxCommitTime`. The block form is unchanged:
`client.withTransaction { ... }` still compiles.

`Effect` gained `monotonic`, needed to bound the retries. It has a default implementation reading `System.nanoTime`,
so an `Effect` you implement yourself keeps compiling; override it if your runtime has a clock worth substituting in
tests, as `mongo4s-cats` and `mongo4s-zio` do.

### `WatchOptions` stopped being a `case class`

It is a `final class` with a private constructor, so `WatchOptions(...)`, `.copy(...)`, `unapply` and the `Product`
methods are gone. Build one from `WatchOptions.default[E]` and chain, exactly as before:

```scala
WatchOptions[User](pipeline = stages)                 // 2.x
WatchOptions.default[User].withPipeline(stages)       // 3.0
```

Reading a field — `options.pipeline`, `options.batchSize` — is unchanged.

The reason is the one `Index` and `WireCodecConfig` already carry: a `case class` cannot gain a field without
breaking `apply`/`copy` in every release, and change-stream options keep arriving. `withExpandedEvents` is the first
one that had to, so the treatment happened now rather than being restated as a hazard.

### `Sort` carries a `SortOrder`, and `Projection` carries slices

`Sort[E].fields` is `List[(FieldPath, SortOrder)]` rather than `List[(FieldPath, Boolean)]`, so a third direction —
`$meta textScore` — is expressible. `Sort.asc`/`Sort.desc` are unchanged; only code that read or built `fields` by
hand has to follow.

The three `Projection` cases each gained a `slices` list, so a pattern match on them needs the extra binder:

```scala
case Projection.Include(fields, withId)         => ...  // 2.x
case Projection.Include(fields, withId, slices) => ...  // 3.0
```

Constructing them is unchanged — the new parameter is defaulted.

### `FindQuery` and `AggregateQuery` gained `explain`

Both are public traits, so anything implementing one outside this library has to add the method. Calling code is
unaffected.

### The direct path stopped refusing narrower numbers

`getDirectCollection` used to demand the exact BSON width its encoder writes — a `Long` field had to be an `Int64`
on disk. It now follows the same rule `getCollection` always did: any numeric type is accepted as long as the value
survives the conversion whole and in range.

Nothing to change at the call site. What changes is that reads which used to fail now succeed, so a collection with
mixed widths for a field no longer has to be read through `getCollection`. If you were relying on the failure to
detect drifted data, `find(...).attempting` no longer reports it — compare BSON types explicitly instead.

A wrong *type* — a string where a number is modelled — is still an error, and is now a `BsonError.TypeMismatch`
rather than the driver's `BsonInvalidOperationException`.

### Operations raise `MongoError`, not driver exceptions

Anything the server reports now arrives as `mongo4s.MongoError`. Code that caught the driver's own types has to
change shape, and it gets shorter:

```scala
// 2.x
insert.recoverWith {
  case e: MongoWriteException if e.getError.getCategory == ErrorCategory.DUPLICATE_KEY => replace
}

// 3.0
insert.recoverWith { case MongoError.DuplicateKey(_) => replace }
```

The driver's exception is still there as `cause`, so a `case e: MongoError if e.cause.isInstanceOf[...]` escape
hatch exists for anything not modelled. `BsonError.DecodingFailure` and `RsBridgeError` are unchanged — only
failures that came from the server are translated.

This is the one change in this release that is a behaviour change rather than a compile error: code catching
`com.mongodb.MongoException` still compiles and stops matching. Search for `com.mongodb.Mongo*Exception` in your
`recover`/`catch` blocks.

### `RsBridgeConfig` stopped being a `case class`

Same treatment, same reason. `RsBridgeConfig(...)` and `.copy(...)` are gone, and `RsBridgeConfig.Default` is now
`RsBridgeConfig.default`, matching every other options type in the library:

```scala
given RsBridgeConfig = RsBridgeConfig(bufferSize = 512, strictSingleResult = true)          // 2.x
given RsBridgeConfig = RsBridgeConfig.default.withBufferSize(512).withStrictSingleResult    // 3.0
```

Reading a field — `config.bufferSize`, `config.timeout` — is unchanged, and the `given` in the companion still
supplies the default, so code that never configured the bridge is untouched.

It was the last public `case class` among the option types, which made it the one place where a new knob would have
cost a major. A `bufferSize` of zero is now rejected on construction instead of hanging the first stream that used
it.

### `createCollection` takes options

`MongoDatabase.createCollection(name)` gained a defaulted `CreateCollectionOptions` parameter, so calls are
unchanged at the source level — but a parameter is never binary-compatible, and `MongoDatabase` is a trait, so
anything implementing it outside this library has to follow.

### `ChangeEvent` gained two fields

`wallTime` and `splitEvent`. Reading an event is unaffected; constructing one by hand — which really only happens in
tests — needs the two extra arguments. It stays a `case class`, because an event is data a consumer destructures
rather than configuration a caller builds, and `copy`/`unapply` are worth having there.

### What it buys

Every module is now built with one Scala version. `mongo4s-bson-calypso`, `mongo4s-kyo` and `mongo4s-rapid` used to
be pinned to a fast-release `3.8` because their upstream dependencies required it, which put them outside what a
`3.3 LTS` project could use at all. They are now on the LTS line with everything else.

Internally the codebase moved to the `given` syntax introduced by SIP-64 in `3.6` — how instances are *declared*,
not what they are. Nothing about that is visible to a caller.

## Migrating from 1.x to 2.0.0

`2.0.0` is a breaking release. The compiler catches every item in this list — none of it changes behaviour silently.

### Operation options moved into values

Write operations used to take their options as parameters. Each family now takes one immutable options value,
built by chaining from `default`:

```scala
collection.updateOne(filter, update, upsert = true)          // 1.x
collection.updateOne(filter, update, UpdateOptions.upsert)   // 2.0

collection.findOneAndUpdate(filter, update, returnUpdated = false)                              // 1.x
collection.findOneAndUpdate(filter, update, FindOneAndUpdateOptions.default[A].returningPrevious) // 2.0
```

The new types are `UpdateOptions`, `ReplaceOptions`, `DeleteOptions`, `CountOptions`, and the entity-typed
`FindOneAndUpdateOptions[A]`, `FindOneAndReplaceOptions[A]`, `FindOneAndDeleteOptions[A]`. `deleteOne`,
`deleteMany` and `count` gained an options parameter they did not have before.

This is the change the rest of the release depends on. A parameter cannot be added to a published method without
breaking binary compatibility, so every option that arrived in `2.0.0` — and every one that arrives later — would
have forced another major release. Carried in a value, they are additive forever.

`WriteCommand.UpdateOne`/`UpdateMany`/`ReplaceOne` carry the same options instead of a bare `upsert`, and
`Repository.updateOne`/`findOneAndUpdate` follow.

### `Projection` is two types

`include`, `exclude` and `withoutId` moved off `Projection[E]` onto its cases, so a projection that mixes inclusion
with exclusion **no longer compiles** rather than silently discarding what you listed first:

```scala
Projection.empty[User].include(nameField).exclude(secretField) // 1.x: silently {"secret": 0}
                                                              // 2.0: does not compile
```

`Projection.empty` now returns `Projection.Everything[E]` and `Projection.excludeId` returns `Projection.Exclude[E]`.
Code that holds a projection as `Projection[E]` and builds on it must keep the narrower type instead. `_id` is still
the exception: `include(...).withoutId` gives `{"field": 1, "_id": 0}`.

### `Index` stopped being a `case class`

It is a `final class` with a private constructor, so `Index(...)`, `.copy(...)`, `unapply` and the `Product` methods
are gone. Build one from `Index.empty[E]` or the named constructors and chain the builders, exactly as before. The
reason is the same as `WireCodecConfig`'s in `1.1.0`: a `case class` cannot gain a field without breaking
`apply`/`copy` again in every release, and index options keep arriving.

### `FakeMongoCollection` moved to its own module

It now lives in the published `mongo4s-testkit` module, under `mongo4s.testkit`, alongside the new `FakeRepository`:

```scala
libraryDependencies += "org.mongo4s" %% "mongo4s-testkit" % "2.0.0" % Test
```

In `1.x` it sat in `mongo4s-repositories`' test sources and was not published at all, so consumers could not use it.

### `BsonTypeName` is a public enum

`Field.hasType` used to take a `String` — MongoDB's `$type` alias, which a typo turns into a filter that silently
matches nothing. It now takes `BsonTypeName`, which was internal before:

```scala
scoreField.hasType("int")                 // 1.x
scoreField.hasType(BsonTypeName.Int)      // 2.0
```

`BsonError.TypeMismatch` carries `BsonTypeName` for both `expected` and `actual` for the same reason; the rendered
message is unchanged.

### New methods on `MongoCollection` and `MongoDatabase`

`withReadConcern`, `withWriteConcern` and `withReadPreference` are abstract, so anything implementing those traits
outside this library has to provide them. `Stage` gained `LookupPipeline` and `GraphLookup` cases, which matters
only if you pattern-match a `Stage` exhaustively.

### Corrected behaviour

These fix results that were wrong before. No source change is needed, but the output differs:

* A sealed hierarchy stored as the root entity of a direct collection now **reads back**. The discriminator is
  located anywhere in the document instead of being required first, which the server never guaranteed.
* Writes under an unacknowledged write concern (`w=0`) return empty results instead of throwing
  `UnsupportedOperationException`.
* `Sort` and `Index` let the last mention of a field win its position, not just its direction.
* `insertMany` returns ids ordered by command position rather than by the driver's map iteration order.
* `aggregate(...).first` no longer appends `$limit` after a terminal `$out`/`$merge`, which the server rejected.
* `Decimal128` is read exactly rather than through `Double`; the `Long` upper bound rejects `2^63`; non-finite
  values are an error instead of a thrown `ArithmeticException`.
* An explicit `BsonEncoder`/`BsonDecoder` pair now outranks automatic `WireCodec` derivation. Previously the two
  were ambiguous and the type would not resolve at all.
* `cats.data.Ior` decodes its two sides by field name instead of by position.
* kyo's `stream` explains the missing `Tag` instead of failing as a `ClassCastException`.

## Scala versions

Every module is built with `Scala 3.9 LTS`. There is no longer a subset of the library that a project on an older
compiler has to do without — and, by the same token, no way to consume any of it from `3.3 LTS`, since `TASTy` does
not read forward.

`mongo4s-kyo` depends on a kyo release candidate. Until kyo reaches 1.0.0 final, that module sits outside the binary
compatibility promise the other artifacts make.

Compiling any module that touches kyo requires `JDK 25` — the `kyo.Frame` macro runs inside the compiler and its
class files target `Java 25`, so this is a compile-time requirement, not just a runtime one.
