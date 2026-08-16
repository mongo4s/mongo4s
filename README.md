<p align="center">
  <img src="logos/mongo4s.png" alt="mongo4s logo" width="240">
</p>

# mongo4s

Effect-agnostic MongoDB client and repository layer for Scala 3. No hardcoded `cats-effect` or `fs2` — the runtime
(`cats-effect` / ZIO / Kyo / rapid) and the BSON codec (your own derivation, or medeia / zio-bson / calypso) are
independent modules, each wired in through a `given` import. `core` depends on neither.

[![CI](https://github.com/mongo4s/mongo4s/actions/workflows/ci.yml/badge.svg)](https://github.com/mongo4s/mongo4s/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/org.mongo4s/mongo4s-core_3?color=blue)](https://central.sonatype.com/search?q=mongo4s)
[![Scala 3](https://img.shields.io/badge/Scala-3-blue)](https://www.scala-lang.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

mongo4s wraps the official `mongodb-driver-reactivestreams` directly — no `mongo4cats` underneath. A type-safe
`Field`/`Filter`/`Update` builder replaces string-keyed queries, `PrimaryKey` turns an entity into single- or
compound-key lookups, and `BaseMongoRepository` gives you CRUD/batch operations over a collection for free. Every
piece is interpretable against a real MongoDB **and** an in-memory `FakeMongoCollection`, so repositories are
unit-testable without a running database.

```mermaid
flowchart LR
  app["your application"] --> repositories
  app --> core

  subgraph core["mongo4s-core"]
    CC["MongoClient · MongoDatabase · MongoCollection"]
    QQ["Field · Filter · Update · PrimaryKey"]
  end

  repositories["mongo4s-repositories<br/>BaseMongoRepository"] --> core

  RT["runtime<br/>cats-effect · ZIO · Kyo · rapid"] -->|given Effect, RsBridge| core
  WC["bson-direct<br/>WireCodec (AST-free)"] -->|given WireCodec| core
  BR["bson bridges<br/>medeia · zio-bson · calypso"] -->|given BsonDocumentCodec| core
```

* [Quick start](#quick-start)
* [Core concepts](#core-concepts)
* [BSON codecs](#bson-codecs)
* [Repositories](#repositories)
* [Runtime backends](#runtime-backends)
* [Modules](#modules)
* [Benchmarks](#benchmarks)
* [Design notes](#design-notes)
* [Contributing](#contributing)

## Quick start

Pick a runtime. The default codec (`bson-direct`) derives straight from your case class — AST-free, no
third-party codec library, no extra dependency beyond `mongo4s-core` itself:

```scala
libraryDependencies ++= Seq(
  "org.mongo4s" %% "mongo4s-cats"           % "0.2.0", // mongo4s-core + cats-effect integration
  "org.mongo4s" %% "mongo4s-bson-direct"    % "0.2.0", // ast-free bson codecs
  "org.mongo4s" %% "mongo4s-bson-cats-data" % "0.2.0", // if you need NonEmptyList etc. codec instances
  "org.mongo4s" %% "mongo4s-repositories"   % "0.2.0", // if you need auto-generated CRUD repository ops for your model
)
```

```scala
import cats.effect.{IO, IOApp}

import mongo4s.cats.CatsStream
import mongo4s.bson.direct.WireCodec
import mongo4s.{Field, MongoClient, PrimaryKey}
import mongo4s.repositories.BaseMongoRepository

import mongo4s.cats.CatsInstances.given

final case class User(id: String, name: String, age: Int) derives WireCodec

object User:
  given PrimaryKey[User, String] = PrimaryKey.single("id")(_.id)

object Main extends IOApp.Simple:
  def run: IO[Unit] =
    for
      client     <- MongoClient.fromConnectionString[IO, CatsStream[IO][A]]("mongodb://localhost:27017")
      db         <- client.getDatabase("myapp")
      collection <- db.getDirectCollection[User]("users")
      users       = BaseMongoRepository(collection)
      _          <- users.insertOne(User("1", "Alice", 30))
      alice      <- users.findOne("1")
      adults     <- users.findByFilter(Field.of[User, Int](_.age).gte(18))
      _          <- client.close
    yield ()
```

Swap `mongo4s-cats` for `mongo4s-zio` / `mongo4s-kyo` / `mongo4s-rapid` and the matching `*Instances.given` import to
change runtime — nothing else in this snippet changes. `BsonEncoder`/`BsonDecoder` for built-in types (`String`,
`Int`, `Option`, `List`, `Vector`, `Set`, `Seq`, …) resolve with no import at all — no
`mongo4s.bson.BsonInstances.given` needed unless you're summoning one directly.

Already have a model on medeia, zio-schema, or calypso? Swap `derives WireCodec` + `getDirectCollection` for
`derives MedeiaDocumentCodec`/etc. + `getCollection` (and `BaseMongoRepository.create(db, "users")` instead of
constructing it from a collection directly) — see [BSON codecs](#bson-codecs) below for all four backends.

Already have a `MongoClientSettings` built elsewhere (connection pool tuning, a custom `CodecRegistry`, …)? Use
`MongoClient.fromSettings` instead of `fromConnectionString`:

```scala
val settings = MongoClientSettings.builder().applyConnectionString(ConnectionString("mongodb://localhost:27017")).build()
client <- MongoClient.fromSettings[IO, S](settings)
```

For more examples see: [examples/src/main/scala/mongo4s/examples](examples/src/main/scala/mongo4s/examples) — a
shared domain model (opaque types, enums, nested case classes) run through every runtime/codec combination
(cats+medeia, ZIO+zio-bson, kyo+medeia, rapid+calypso), a repository example covering all three
`BaseMongoRepository` construction styles against bson-direct, and a sessions/transactions + typed aggregation
pipeline example on cats+medeia.

## Core concepts

`MongoClient[F, S]` → `MongoDatabase[F, S]` → `MongoCollection[F, S, A]` mirror the driver's own hierarchy, wrapped in
your effect `F[_]` and stream type `S[_]`:

```scala
trait MongoCollection[F[*], S[*], A]:
  def insertOne(document: A)(using session: Option[ClientSession] = None): F[InsertOneResult]
  def find(filter: Filter[A] = Filter.all)(using session: Option[ClientSession] = None): FindQuery[F, S, A]
  def updateOne(filter: Filter[A], update: Update[A], upsert: Boolean = false)(using session: Option[ClientSession] = None): F[Long]
  def deleteOne(filter: Filter[A])(using session: Option[ClientSession] = None): F[Long]
  def aggregate[B](pipeline: Seq[Stage[A]])(using session: Option[ClientSession] = None)(using BsonDocumentCodec[B]): AggregateQuery[F, S, B]
  def distinct[C, B](field: Field[A, C], filter: Filter[A])(using session: Option[ClientSession] = None)(using BsonDecoder[B]): DistinctQuery[F, S, B]
  // count, insertMany, updateMany, deleteMany, bulkWrite, watch, ...
```

Every method takes an optional `ClientSession` — see [Sessions & transactions](#sessions--transactions) — and defaults
to `None`, so none of this is visible until you opt in.

`Field.of[E, A](_.someField)` is a macro that reads a field selector at compile time — no strings, no reflection —
and gives you a typed path to build filters, updates, and sorts:

```scala
val adults = Field.of[User, Int](_.age).gte(18)
val named  = Field.of[User, String](_.name).equalTo("Alice") && adults
val setAge = Field.of[User, Int](_.age).set(31)
val city   = Field.of[Order, String](_.address.city).equalTo("Berlin")  // dotted paths from nested selectors
```

`PrimaryKey[E, K]` turns an entity into a key-based filter — single field, a native `_id` (`ObjectId` or your own
encoder), or a compound key of up to four fields:

```scala
given PrimaryKey[User, String]         = PrimaryKey.single("id")(_.id)
given PrimaryKey[Note, ObjectId]       = PrimaryKey.objectId(_.id)          // entity has its own ObjectId field
given PrimaryKey[Order, (String, Int)] = PrimaryKey.make(o => (o.userId, o.seq), k => "user_id" -> k._1, k => "seq" -> k._2)
```

For entities that don't carry their own id field, `WithId[ObjectId, E]` (see [Repositories](#repositories)) ships a
ready-made `PrimaryKey` — no `given` needed.

`inFilter` on a compound key produces an `$or` of `$and`s; on a single field it's a plain `$in` — and an empty key
list always produces `Filter.none`, so `findMany(Nil)`/`deleteMany(Nil)` are safe no-ops instead of matching every
document.

### Sessions & transactions

`client.startSession` is fully manual — mongo4s hands you the driver `ClientSession` and steps back. You own the
whole lifecycle yourself: start the transaction, pass the session explicitly with `(using Some(session))` at each
call site you want inside it, commit or abort, and close the session when you're done — nothing here happens
automatically:

```scala
import mongo4s.MongoSession

for
  session <- client.startSession
  _       <- MongoSession.startTransaction[IO](session)
  _       <- users.insertOne(User("2", "Bob", 41))(using Some(session))
  _       <- MongoSession.commitTransaction[IO, S](session) // or abortTransaction to roll back
  _       <- IO.delay(session.close())
yield ()
```

Every `MongoClient`/`MongoDatabase`/`MongoCollection`/`Repository` method accepts the same `(using session:
Option[ClientSession] = None)`, so a `BaseMongoRepository` call can join an existing transaction the same way a raw
`collection` call can.

The manual dance above never aborts on failure either — if the body throws, the transaction is just left open until
it times out server-side. `client.withTransaction` does the whole thing automatically instead: starts a session,
commits on success or aborts (then re-raises) on failure, and closes the session either way — you don't manage any
part of the lifecycle yourself, and the session is given implicitly to everything inside the body, so no
`(using Some(session))` at each call site:

```scala
client.withTransaction {
  users.insertOne(User("2", "Bob", 41)) // Option[ClientSession] is already given here
}
```

`withTransaction` is built entirely from `Effect[F]`'s own minimal vocabulary (`flatMap`/`handleErrorWith`), so it
works identically on every runtime. That also means it only covers the "body raised an error" path — on cats-effect
specifically, a hard cancellation of the surrounding fiber won't trigger the abort/close, since `Effect[F]` has no
notion of cancellation the way `cats.effect.MonadCancel` does. Reach for `MongoClientResource` (see
[Runtime backends](#runtime-backends)) combined with your own `Resource`-based cleanup if that stronger guarantee
matters for a specific call site.

`MongoClient.withTransaction` is itself built on a lower-level `ClientSession.withTransaction`: same automatic
commit-on-success/abort-on-failure behavior, but manual again about the session's own lifecycle — it doesn't close
the session, since (like the fully manual path above) it doesn't own one you got from `startSession` yourself. Reach
for it directly if you're reusing one already-open session across more than one transaction:

```scala
for
  session <- client.startSession
  _       <- session.withTransaction(users.insertOne(User("2", "Bob", 41)))
  _       <- session.withTransaction(users.insertOne(User("3", "Carol", 29))) // same session, second transaction
  _       <- IO.delay(session.close())
yield ()
```

### Aggregation pipelines

`aggregate` takes a `Seq[Stage[A]]` — a typed pipeline-stage AST, mirroring `Filter`/`Update`/`Sort`, instead of raw
`BsonDocument`s — built with the same `Field.of` selectors:

```scala
import mongo4s.operations.{Sort, Stage}

val pipeline = Seq(
  Stage.matching(Field.of[User, Int](_.age).gte(18)),
  Stage.sortBy(Sort.asc(Field.of[User, String](_.name))),
  Stage.limit(10),
)
val adults: IO[List[User]] = users.aggregate[User](pipeline).all
```

`Stage` covers `$match`/`$project`/`$sort`/`$limit`/`$skip`/`$count`/`$unwind`/`$lookup`, plus `Stage.raw(document)`
as an escape hatch for anything else (`$group` included, for now — see the type's own doc comment). Field references
inside a `Stage` are always typed against the pipeline's *starting* document type `A`, the same way `aggregate[B]`'s
own output type `B` is specified manually rather than inferred stage-by-stage.

### Change streams

`watch` exists at all three levels, matching the driver's own scope hierarchy — `MongoClient.watch` (the whole
deployment, needs a replica set/sharded cluster), `MongoDatabase.watch` (one database), and `MongoCollection.watch`
(one collection):

```scala
val events: S[ChangeEvent[Person]] = people.watch()
```

Every event is a `ChangeEvent[A]`, not a bare `BsonDocument`:

```scala
final case class ChangeEvent[A](
  operationType: OperationType,                    // com.mongodb's own enum — INSERT/UPDATE/DELETE/...
  documentKey: Option[BsonDocument],
  fullDocument: Option[A],
  fullDocumentBeforeChange: Option[A],
  updateDescription: Option[UpdateDescription],     // updated/removed field paths, for UPDATE events
  resumeToken: BsonDocument,
  clusterTime: Option[BsonTimestamp],
)
```

`fullDocument` defaults to populated on **both** insert and update events (`FullDocument.UPDATE_LOOKUP`) — MongoDB's
own default only fills it in for inserts/replaces, leaving it `None` for the far more common "what does the document
look like now" update case unless you ask for the lookup explicitly. It's always `None` for deletes (there's no
document left to look up); override via `watch(fullDocument = FullDocument.DEFAULT)` if you want the driver's
original behavior instead.

`MongoCollection.watch` decodes through the collection's own codec, so `fullDocument`/`fullDocumentBeforeChange` come
back as `Option[A]`. `MongoClient.watch`/`MongoDatabase.watch` span more than one document shape, so they default to
`ChangeEvent[BsonDocument]` — use `watchAs[A]` instead if you know the events you'll see (e.g. because `pipeline`
already filters down to one namespace) all decode the same way:

```scala
val typed: S[ChangeEvent[Person]] = database.watchAs[Person](pipeline = onlyPeopleCollection)
```

The optional `pipeline: Seq[BsonDocument]` filters the change stream itself — and it matches against the **change
event's own shape** (`{operationType, fullDocument, ns, ...}`), not the collection's document shape, so it stays raw
`BsonDocument`, not a typed `Stage[A]`: a `Stage`-built `Field.of[Person, Int](_.age)` filter would render the path
`"age"`, but the change event needs `"fullDocument.age"` to reach the same field. A common case — filtering to one
operation type — needs no field-path awareness at all:

```scala
val insertsOnly = Seq(BsonDocument("$match", BsonDocument("operationType", BsonString("insert"))))
val events: S[ChangeEvent[Person]] = people.watch(pipeline = insertsOnly)
```

## BSON codecs

Every codec ultimately produces a `mongo4s.bson.BsonDocumentCodec[A]` (entity ⇄ `org.bson.BsonDocument`) or, for the
AST-free path below, a `WireCodec[A]`. Bring whichever backend fits — mongo4s never registers a global
`CodecProvider`, so backends never collide inside one process.

| Module | Backend                                                                        | Notes |
| --- |--------------------------------------------------------------------------------| --- |
| `mongo4s-bson-medeia` | [medeia](https://github.com/medeia/medeia)                                     | `derives BsonDocumentCodec` |
| `mongo4s-bson-zio` | [zio-schema-bson](https://github.com/zio/zio-bson)                             | via `zio.schema.Schema` |
| `mongo4s-bson-calypso` | [calypso](https://github.com/m2-oss/calypso)                                   | hand-written `forProductN` codecs |
| `mongo4s-bson-direct` | [mongo4s itself](https://github.com/mongo4s/mongo4s/tree/main/bson/direct/src) | `WireCodec[A]`, AST-free — see below |
| `mongo4s-bson-cats-data` | [cats-core](https://typelevel.org/cats/) | `BsonEncoder`/`BsonDecoder`/`WireCodec` for `NonEmptyList`/`Chain`/`NonEmptyVector`/`NonEmptySet`/`NonEmptyMap` |

### `bson-direct` — AST-free `WireCodec`

`medeia`/`zio-bson`/`calypso` all build an intermediate `org.bson.BsonValue` tree before it ever reaches the driver.
`WireCodec[A]` skips that: it's derived via `Mirror` and writes straight to the driver's own streaming
`BsonWriter`/`BsonReader`, the same low-level SPI jsoniter-scala uses for JSON — no `BsonDocument` is ever built, on
either side. No third-party codec dependency needed; `bson-direct` is transitively pulled in by `mongo4s-core`.

```scala
import mongo4s.bson.direct.WireCodec

final case class Address(city: String, zip: String) derives WireCodec
final case class Person(id: String, name: String, tags: List[String], address: Address) derives WireCodec

sealed trait Shape derives WireCodec
object Shape:
  final case class Circle(radius: Double)                   extends Shape derives WireCodec
  final case class Rectangle(width: Double, height: Double) extends Shape derives WireCodec
```

Products, `Option`, `List`, nested case classes, sealed traits/enums (via a `_type` discriminator field, written
first), and self-/mutually-recursive types all derive directly — recursive derivation is deferred behind a `lazy val`
internally so a type's own `given` never forces itself mid-construction. Anything with an existing `BsonEncoder`/
`BsonDecoder` (`ObjectId`, `Instant`, `UUID`, …) bridges automatically, at the cost of one `BsonValue` per field
instead of zero.

`getDirectCollection` registers the derived codec with the driver via `CodecRegistries.fromCodecs(...)`, so
insert/find/replace/update/delete/bulkWrite decode straight to `A` — genuinely zero `BsonDocument` construction on the
hot path, not just a thinner bridge:

```scala
import mongo4s.bson.direct.WireCodec
import mongo4s.repositories.BaseMongoRepository

final case class Person(id: String, name: String, age: Int) derives WireCodec
object Person:
  given PrimaryKey[Person, String] = PrimaryKey.single("id")(_.id)

for
  db         <- client.getDatabase("myapp")
  collection <- db.getDirectCollection[Person]("people")
  repo        = BaseMongoRepository(collection)
  _          <- repo.insertOne(Person("1", "bob", 30))
yield ()
```

`aggregate`/`distinct` still go through `BsonDocumentCodec`/`BsonDecoder` on a direct collection (they're not the hot
path); everything else — `Filter`/`Update`/`Field` construction — is identical regardless of which codec backs the
collection.

#### Field naming

By default `derives WireCodec` writes field and discriminator names exactly as they appear in the Scala source. To
match an existing collection's naming convention (`snake_case`, say), bring a `WireCodecConfig` into scope before
deriving — it reuses the same `mongo4s.bson.FieldNaming` the query layer (`Filter`/`Update`/`Sort`) already uses,
rather than a second, independent naming mechanism:

```scala
import mongo4s.bson.direct.WireCodecConfig

given WireCodecConfig = WireCodecConfig.SnakeCase

final case class Person(firstName: String, lastName: String) derives WireCodec // writes "first_name"/"last_name"
```

Field naming and discriminator naming (the `_type` value for sealed traits/enums) are independently configurable via
`WireCodecConfig(fieldNaming = ..., discriminatorNaming = ...)`. `WireCodecConfig` only affects how `WireCodec`
derives — pass the matching `FieldNaming` (e.g. `WireCodecConfig.SnakeCase.fieldNaming`) to `getDirectCollection`'s
own `naming` parameter too, so `Filter`/`Update`/`Field` queries render the same field names the codec wrote.

`encodeEmptyCasesAsString = true` writes a parameterless case (`case object`/empty `case class`) as a bare BSON
string instead of a `{"_type": "..."}` document — only safe when that case is nested inside another document, not
when it's the root type of a collection (BSON's root value must always be a document).

### `bson-cats-data` — `cats.data` support

`NonEmptyList`/`Chain`/`NonEmptyVector`/`NonEmptySet`/`NonEmptyMap` get `BsonEncoder`/`BsonDecoder` and `WireCodec`
instances from `mongo4s-bson-cats-data`, delegating to the already-existing `List`/`Vector`/`Set`/`Map` instances
rather than reimplementing BSON encoding:

```scala
import mongo4s.bson.catsdata.CatsDataBsonInstances.given // or CatsDataWireInstances.given for bson-direct
import cats.data.NonEmptyList

final case class Team(members: NonEmptyList[String]) derives BsonDocumentCodec // via medeia/zio-bson/calypso
```

`NonEmptySet`/`NonEmptyMap` additionally need a `cats.Order` for their element/key type — the same `Order` you'd
already need to construct one of these types directly.

## Repositories

`BaseMongoRepository[F, S, E, K]` implements `Repository` — count/find/insert/upsert/update/delete/bulkWrite, batched
by `batchSize` (default 500) — over any `MongoCollection[F, S, E]`, from either codec path:

```scala
BaseMongoRepository.create[F, S, E, K](db, "collection")     // Projection.excludeId default
BaseMongoRepository.identified[F, S, E, K](db, "collection") // no projection — for entities that store their own "_id"
BaseMongoRepository.objectId[F, S, E](db, "collection")      // WithId[ObjectId, E], auto _id round-trip
```

`WithId[Id, E]` wraps an entity with a separately-typed id (`type Oid[E] = WithId[ObjectId, E]`) and ships its own
`PrimaryKey`/`BsonDocumentCodec` instances, for entities that don't carry their own id field.

Like `MongoCollection`, every `Repository` method takes `(using session: Option[ClientSession] = None)` — a
repository call can join a [transaction](#sessions--transactions) the same way a raw `collection` call can.

For unit tests, `FakeMongoCollection` (in `mongo4s-repositories`' test sources, reusable via
`% "test->test;test->compile"`) implements `MongoCollection` in memory — the exact same `Filter`/`Update`/`Field` AST
the real driver interprets is interpreted against a `TrieMap` instead, so repository logic is testable without
`aggregate`/`distinct`/`watch` (they throw `UnsupportedOperationException`, naming the method) and without a running
MongoDB.

## Runtime backends

Each runtime module provides `given Effect[F]` (a minimal `pure`/`delay`/`map`/`flatMap`/`raiseError` capability) and
`given RsBridge[F, S]` (Reactive-Streams `Publisher` → `F`/`S`):

| Module | Effect | Stream | Notes |
| --- | --- | --- | --- |
| `mongo4s-cats` | `cats.effect.IO` | `fs2.Stream` | via `fs2.interop.reactivestreams` |
| `mongo4s-zio` | `zio.Task` | `zio.stream.ZStream` | via `zio-interop-reactivestreams` |
| `mongo4s-kyo` | `kyo.IO` | `kyo.Stream` | via `kyo-reactive-streams` |
| `mongo4s-rapid` | `rapid.Task` | `rapid.Stream` | |

`MongoClient.fromClient`/`fromSettings`/`fromConnectionString` return a bare `F[MongoClient[F, S]]` — you own calling
`.close`. On `mongo4s-cats`, `MongoClientResource` mirrors the same three constructor names, wrapped as a
`cats.effect.Resource` so `.close` runs on release automatically:

```scala
import mongo4s.cats.MongoClientResource

MongoClientResource.fromConnectionString[IO]("mongodb://localhost:27017").use: client =>
  ...
```

(zio/kyo/rapid equivalents, using each runtime's own resource-safety idiom instead of copying `Resource` as-is, are
on the roadmap.)

## Modules

Published for Scala 3 under `org.mongo4s`:

```scala
"org.mongo4s" %% "mongo4s-<module>" % "0.2.0"
```

| | Module | Notes |
| --- | --- | --- |
| core | `mongo4s-core` | `MongoClient`/`MongoDatabase`/`MongoCollection`, `Field`/`Filter`/`Update`, `PrimaryKey`; depends on `bson-core` + `bson-direct` only |
| bson | `mongo4s-bson-core` | `BsonEncoder`/`BsonDecoder`/`BsonDocumentCodec` — the scalar + document codec seam |
| | `mongo4s-bson-direct` | `WireCodec[A]` — AST-free product/sum derivation, no third-party dependency |
| | `mongo4s-bson-cats-data` | `cats.data` (`NonEmptyList`/`Chain`/`NonEmptyVector`/`NonEmptySet`/`NonEmptyMap`) instances |
| | `mongo4s-bson-medeia` | bridges `medeia`'s `derives BsonDocumentCodec` |
| | `mongo4s-bson-zio` | bridges `zio-schema-bson` |
| | `mongo4s-bson-calypso` | bridges `calypso`'s `forProductN` |
| runtime | `mongo4s-cats` | `cats-effect 3` + `fs2` |
| | `mongo4s-zio` | ZIO 2 + `zio-streams` |
| | `mongo4s-kyo` | kyo 1.0.0-RC6 |
| | `mongo4s-rapid` | rapid |
| repositories | `mongo4s-repositories` | `BaseMongoRepository`, `Repository`, `WithId` |

## Benchmarks

Three separate JMH harnesses in [`benchmarks/`](benchmarks), one developer machine — directional ballparks, not
hardware-independent authorities. Run them on your own hardware before making decisions on the numbers alone.

### Codec backends — to `org.bson.BsonDocument`

[`CodecBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/CodecBenchmark.scala) encodes/decodes a mid-size
entity (seven fields, one nested object) through each backend, straight to/from `org.bson.BsonDocument`, against
`mongo4cats`'s own codec modules (`mongo4cats-circe`, `mongo4cats-zio-json`):

```bash
sbt "benchmarks/Jmh/run mongo4s.benchmarks.CodecBenchmark"
```

| Codec | Encode ops/s | Decode ops/s |
| --- | ---: | ---: |
| `calypso` (`forProductN`) | **~3.36M** | ~3.24M |
| `medeia` (`derives`) | ~1.40M | **~3.34M** |
| `zio-bson` (`zio-schema` derived) | ~992k | ~2.98M |
| `mongo4cats-zio-json` | ~1.03M | ~920k |
| `mongo4cats-circe` | ~972k | ~665k |

All three of mongo4s's codec bridges beat both mongo4cats codecs by 3–5× on decode — the cost of
`case class ↔ circe/zio-json ↔ mongo4cats.Bson ↔ org.Bson` instead of straight to `org.bson`.

### AST-free wire codec — all the way to real bytes

[`WireFreeCodecBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/WireFreeCodecBenchmark.scala) goes one step
further than the table above: case class ↔ real BSON wire bytes (`BsonBinaryWriter`/`BsonBinaryReader`), not just
↔ `BsonDocument`. `medeiaFull*` is medeia's own `BsonDocument` plus the driver's own `BsonDocumentCodec` walking it to
bytes (two tree-walks); `direct*` is `WireCodec` writing straight to the wire (zero):

```bash
sbt "benchmarks/Jmh/run -prof gc WireFreeCodecBenchmark"
```

| Path | Throughput | Alloc |
| --- | ---: | ---: |
| `direct` encode | **~1.79M ops/s** | **1792 B/op** |
| `medeiaFull` encode | ~838k ops/s | 4712 B/op |
| `direct` decode | **~1.49M ops/s** | **1088 B/op** |
| `medeiaFull` decode | ~900k ops/s | 3200 B/op |

`WireCodec` is ~2.1× the throughput on encode, ~1.7× on decode, and allocates 2.6–2.9× less — avoiding medeia's own
intermediate AST *and* the driver's own `BsonDocument`, versus avoiding just the latter. In a typical CRUD workload
the network round trip dwarfs this difference (see below); it matters for bulk-decode-heavy paths — large cursor
streams, ETL, aggregation over big result sets.

### Runtime overhead — real MongoDB, every backend

[`RuntimeBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/RuntimeBenchmark.scala) runs the same
insert/find/update/delete/count workload against a real MongoDB through every mongo4s runtime, and `mongo4cats` as a
reference point:

```bash
docker run -d --name mongo4s-bench -p 27018:27017 mongo:7
sbt "benchmarks/Jmh/run -tu s .*RuntimeBenchmark.*"
```

**Throughput — operations per second, higher is better**

| Operation | mongo4s-cats | mongo4s-zio | mongo4s-rapid | mongo4s-kyo | mongo4cats-cats | mongo4cats-zio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `insertOne` | 2272 | **2562** | 2447 | 2505 | 2277 | 2504 |
| `find(filter).all` (~100 docs) | 1432 | 1585 | 1551 | **1595** | 1316 | 1325 |
| `find(filter).stream` (~100 docs) | 1404 | 1393 | **1557** | 1548 | 359 | 1147 |
| `updateOne` | 2002 | **2222** | 2198 | 2215 | 2138 | 2219 |
| `count(filter)` | 1804 | **2014** | 1996 | 1975 | 1905 | 1990 |

With a clean database on every trial, all six columns land within the same band for every operation — the TCP round
trip to MongoDB dominates at this scale, and none of the six `RsBridge`/collection wrappers stands out. The one real
outlier is `mongo4cats-cats`'s `find(filter).stream` (359 ops/s against its own `.all`'s 1316) — it bridges through a
hand-rolled `cats.effect.std.Queue`-backed `Subscriber` instead of `fs2.interop.reactivestreams`, which every mongo4s
`.stream()` uses. Full table and methodology notes are in the benchmark source.

### Codec choice under real MongoDB — mongo4s vs mongo4cats

The same `RuntimeBenchmark` also isolates the *codec* dimension: four configs, all on cats-effect, against the same
real MongoDB — `mongo4s` with `bson-medeia` vs `bson-direct`, and `mongo4cats` with `circe` vs `zio-json`. Two
separate runs, same four stacks, two different questions:

```bash
# how many operations per second
sbt "benchmarks/Jmh/run -tu s .*RuntimeBenchmark\.cats.* .*RuntimeBenchmark\.mongo4catsCats.*"

# how much garbage each operation generates
sbt "benchmarks/Jmh/run -prof gc .*RuntimeBenchmark\.cats.* .*RuntimeBenchmark\.mongo4catsCats.*"
```

**Throughput — operations per second, higher is better**

| Operation | mongo4s+medeia | mongo4s+bson-direct | mongo4cats+circe | mongo4cats+zio-json |
| --- | ---: | ---: | ---: | ---: |
| `insertOne` | 2321 | 2323 | 2381 | **2405** |
| `insertMany` (10 docs) | 2027 | 2012 | **2173** | 2115 |
| `findOneById` | 2047 | 2031 | **2184** | 2122 |
| `findOneByFilter` | 2117 | 2066 | **2270** | 2194 |
| `findAll` (~100 docs) | 1415 | **1477** | 1299 | 1310 |
| `findStream` (~100 docs) | 1426 | **1435** | 386 | 388 |
| `updateOne` | 2026 | 1988 | 2143 | **2159** |
| `deleteOne`\* | 1063 | 1062 | **1129** | 1108 |
| `count` | 1852 | 1847 | **1958** | 1916 |

Same finding as the [runtime table above](#runtime-overhead--real-mongodb-every-backend): all four land within the
same band on every operation (differences are within normal run-to-run noise, ~±5–10%) — the network round trip
dominates regardless of codec. The one outlier is `mongo4cats`' `find(filter).stream`, ~6× slower than everything
else *and independent of codec* (386 vs 388 ops/s for circe vs zio-json) — confirms it's the runtime's
`Queue`-backed `Subscriber` bridge, not the codec, exactly as found earlier.

**Memory allocated per single call, in KB — lower is less garbage, not less work done**

| Operation | mongo4s+medeia | mongo4s+bson-direct | mongo4cats+circe | mongo4cats+zio-json |
| --- | ---: | ---: | ---: | ---: |
| `insertOne` | 49.0 | 46.6 | 25.3 | **24.6** |
| `insertMany` (10 docs) | 89.9 | **66.5** | 80.9 | 74.6 |
| `findOneById` | 60.6 | 58.8 | 39.5 | **36.6** |
| `findOneByFilter` | 60.5 | 58.7 | 39.3 | **36.5** |
| `findAll` (~100 docs) | 419.4 | **241.9** | 951.7 | 674.8 |
| `findStream` (~100 docs) | 427.5 | **240.3** | 3027.2 | 2746.2 |
| `updateOne` | 47.0 | 46.9 | **22.4** | **22.4** |
| `deleteOne`\* | 94.2 | 92.3 | 45.3 | **44.8** |
| `count` | 56.5 | 56.5 | **31.5** | **31.5** |

Two things worth noting:

* **`bson-direct` allocates less than `bson-medeia` on every single operation** — the AST-free advantage measured in
  isolation ([above](#ast-free-wire-codec--all-the-way-to-real-bytes)) survives end-to-end through a real driver
  round trip, not just in a codec microbenchmark. The gap is modest on single-document ops (~4–6%) and much larger on
  bulk reads (`findAll`/`findStream`, ~42–44% less) — the more documents decoded per call, the more the saved
  `BsonDocument` tree-walks compound.
* **mongo4cats allocates *less* than mongo4s on single-document ops, but far *more* on bulk reads.** For
  `insertOne`/`updateOne`/`deleteOne`/`count`/`findOneById` mongo4cats's circe/zio-json path is lighter (its JSON
  codecs skip `org.bson.BsonDocument` for scalar-ish shapes where mongo4s's `BsonDocumentCodec` doesn't); for
  `findAll`/`findStream` it allocates 2–13× more than either mongo4s config — `mongo4cats.bson.BsonValue`, its own
  wrapper type, adds a full extra tree per document on top of `org.bson`'s, and that cost multiplies with document
  count. Neither library is uniformly lighter; which one wins depends on whether your workload is point-lookups or
  bulk scans.

## Design notes

* **Scala 3 only.** `given`-based throughout; derivation uses `Mirror` and inline, no runtime reflection.
* **Effect-agnostic by construction.** `core` depends on neither `cats-effect` nor `fs2` — `Effect[F]`/`RsBridge[F, S]`
  are minimal capability typeclasses; a runtime module is ~two `given` instances.
* **No global `CodecProvider`.** Every codec is resolved and (for `bson-direct`) registered per-collection, never
  process-wide — multiple codec backends coexist safely in one application.
* **Own filter/update AST, not the driver's opaque builder.** `Filter`/`Update` are real ADTs `mongo4s` interprets
  itself — both to a real `Bson` query and, for tests, against an in-memory map (`FakeMongoCollection`) — instead of
  each service hand-rolling its own in-memory fake.
* **Batching is empty-list-safe.** `insertMany`/`upsertMany`/`deleteMany`/`findMany` chunk by `batchSize`;
  `inFilter(Nil)` is `Filter.none`, not "match everything," so `deleteMany(Nil)` is a safe no-op.
* **`BaseMongoRepository` is a base class you extend**, not a generated one — declare extra domain queries directly
  against `collection`/`Filter`/`Field`, the same escape hatch the driver itself gives you.

## Contributing

Bug reports and PRs are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build the project,
run its tests, and where to make changes for common kinds of contributions. This project follows the
[Contributor Covenant](CODE_OF_CONDUCT.md); please report security issues per [SECURITY.md](SECURITY.md)
rather than in a public issue.

## License

Apache 2.0 — see [LICENSE](LICENSE).
