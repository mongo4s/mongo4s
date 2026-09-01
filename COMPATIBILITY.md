# Compatibility

[← back to the README](README.md)

## What the artifacts promise

* Binary compatibility within a major version is checked by [MiMa](https://github.com/lightbend/mima) on every
  build; `versionScheme := "semver-spec"` describes what the artifacts promise.
* New methods added to `Effect` and `RsBridge` carry default implementations, so implementing either typeclass
  yourself keeps working across minor releases.
* Deprecations get at least one minor release before removal.
* Every deliberate break inside a major version is listed here and carries a matching filter in `mima.sbt`, so
  nothing is waived silently. `2.0.0` has no such exceptions: it is a major release, and everything below is
  covered by that.

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

`Scala 3 TASTy` is backward but not forward compatible, so `mongo4s-bson-calypso`, `mongo4s-kyo` and `mongo4s-rapid`
— the three modules built on `3.8` — **cannot be consumed from a Scala `3.3 LTS` project**, even though everything
else can. They are pinned there because their upstream dependencies require it.

`mongo4s-kyo` depends on a kyo release candidate. Until kyo reaches 1.0.0 final, that module sits outside the binary
compatibility promise the other artifacts make.

Compiling any module that touches kyo requires `JDK 25` — the `kyo.Frame` macro runs inside the compiler and its
class files target `Java 25`, so this is a compile-time requirement, not just a runtime one.
