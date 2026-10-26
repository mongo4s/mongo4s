<p align="center">
  <img src="logos/mongo4s.png" alt="mongo4s logo" width="240">
</p>

# mongo4s

Effect-agnostic `MongoDB` client and repository layer for `Scala 3`. No hardcoded `cats-effect` or `fs2` — the runtime
(`cats-effect` / `ZIO` / `Kyo` / `rapid`) and the `BSON` codec (your own derivation, or `medeia` / `zio-bson` /
`calypso`) are
independent modules, each wired in through a `given` import. `core` depends on neither.

[![CI](https://github.com/mongo4s/mongo4s/actions/workflows/ci.yml/badge.svg)](https://github.com/mongo4s/mongo4s/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/org.mongo4s/mongo4s-core_3?color=blue)](https://central.sonatype.com/search?q=mongo4s)
[![Scala 3](https://img.shields.io/badge/Scala-3-blue)](https://www.scala-lang.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

`mongo4s` wraps the official `mongodb-driver-reactivestreams` directly. A type-safe
`Field`/`Filter`/`Update` builder replaces string-keyed queries, `PrimaryKey` turns an entity into single- or
compound-key lookups, and `BaseMongoRepository` gives you CRUD/batch operations over a collection for free. Every
piece is interpretable against a real `MongoDB` **and** an in-memory `FakeMongoCollection`, so repositories are
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
* [Adopters](#adopters)
* [Changelog](CHANGELOG.md)
* [Roadmap](ROADMAP.md)
* [Contributing](#contributing)

## Quick start

Pick a runtime. The default codec (`bson-direct`) derives straight from your case class — AST-free, no
third-party codec library, no extra dependency beyond `mongo4s-core` itself:

```scala
libraryDependencies ++= Seq(
  "org.mongo4s" %% "mongo4s-cats"           % "5.0.0", // mongo4s-core + cats-effect integration
  "org.mongo4s" %% "mongo4s-bson-direct"    % "5.0.0", // ast-free bson codecs
  "org.mongo4s" %% "mongo4s-bson-cats-data" % "5.0.0", // if you need NonEmptyList etc. codec instances
  "org.mongo4s" %% "mongo4s-repositories"   % "5.0.0", // if you need auto-generated CRUD repository ops for your model
)
```

```scala
import cats.effect.{IO, IOApp}

import mongo4s.cats.{CatsStream, MongoClientResource}
import mongo4s.bson.direct.WireCodec
import mongo4s.{Field, PrimaryKey}
import mongo4s.repositories.BaseMongoRepository

import mongo4s.cats.CatsInstances.given

final case class User(id: String, name: String, age: Int) derives WireCodec

object User:
  given PrimaryKey[User, String] = PrimaryKey.single("id")(_.id)

object Main extends IOApp.Simple:
  def run: IO[Unit] =
    MongoClientResource.fromConnectionString[IO]("mongodb://localhost:27017").use { client =>
      for
        db <- client.getDatabase("myapp")
        collection <- db.getDirectCollection[User]("users")

        users = BaseMongoRepository(collection)

        _ <- users.insertOne(User("1", "Alice", 30))
        alice <- users.findOne("1")
        adults <- users.findByFilter(Field.of[User, Int](_.age).gte(18))
      yield ()
    }
```

Swap `mongo4s-cats` for `mongo4s-zio` / `mongo4s-kyo` / `mongo4s-rapid` and the matching `*Instances.given` import to
change runtime — the body of the `for` is identical. What does change is how you take the client, since each runtime
keeps its own resource idiom: cats gives you a `Resource` to `.use`, ZIO a scoped `ZIO` and kyo a `Scope` effect (both
bind inside the `for` and are discharged by `ZIO.scoped`/`Scope.run`), and rapid takes the body as a function —
`MongoClientResource.fromConnectionString(uri) { client => … }`. See
[examples/](examples/src/main/scala/mongo4s/examples) for one app per runtime.
`BsonEncoder`/`BsonDecoder` for built-in types (`String`,
`Int`, `Option`, `List`, `Vector`, `Set`, `Seq`, …) resolve with no import at all — no
`mongo4s.bson.BsonInstances.given` needed unless you're summoning one directly.

Already have a model on `medeia`, `zio-schema`, or `calypso`? Swap `derives WireCodec` + `getDirectCollection` for
`derives MedeiaDocumentCodec`/etc. + `getCollection` (and `BaseMongoRepository.create[F, S, User, String](db, "users")`
instead of constructing it from a collection directly — `E` and `K` come from the type arguments, not from the
arguments, so they have to be written out) — see [BSON codecs](#bson-codecs) below for all four backends.

Already have a `MongoClientSettings` built elsewhere (connection pool tuning, read/write concerns, TLS, credentials,
…)? Use `MongoClientResource.fromSettings` instead of `fromConnectionString`. `MongoClient.fromClient`/`fromSettings`/
`fromConnectionString` give you the same thing unwrapped, if you'd rather own `.close` yourself.

`MongoClientSettings` sets read/write concerns for the whole client. To narrow them, `MongoDatabase` and
`MongoCollection` both carry `withReadConcern`/`withWriteConcern`/`withReadPreference`, each returning a **new**
handle rather than mutating the one you have:

```scala
val durable = collection.withWriteConcern(WriteConcern.MAJORITY)
durable.insertOne(user)          // majority-acknowledged
collection.insertOne(other)      // still the client's default
```

Because the derived handle is an ordinary `MongoCollection`, per-operation control is just chaining:
`collection.withWriteConcern(w).insertOne(x)`. Concerns inherit database → collection, as they do in the driver, and
a collection derived this way keeps the codec it was opened with — including the `WireCodec` that
`getDirectCollection` registers.

A driver `CodecRegistry` is not how you plug a codec into `mongo4s`, though — see
[Codecs and the driver's registry](#codecs-and-the-drivers-registry).

For more examples see [examples/src/main/scala/mongo4s/examples](examples/src/main/scala/mongo4s/examples) — a
shared domain model (opaque types, enums, nested case classes) run through every runtime/codec combination
(`cats + medeia`, `ZIO + zio-bson`, `kyo + medeia`, `rapid + calypso`), a repository example covering all four
`BaseMongoRepository` construction styles against bson-direct, and a sessions/transactions + typed aggregation
pipeline example on `cats + medeia`. Most of what this README shows is compiled there too —
[`ReadmeSnippets.scala`](examples/src/main/scala/mongo4s/examples/ReadmeSnippets.scala) walks the same ground section by
section, so if the API moves and these docs don't, CI fails.

## Core concepts

`MongoClient[F, S]` → `MongoDatabase[F, S]` → `MongoCollection[F, S, A]` mirror the driver's own hierarchy, wrapped in
your effect `F[_]` and stream type `S[_]`:

```scala
trait MongoCollection[F[*], S[*], A]:
  def insertOne(document: A)(using session: Option[ClientSession] = None): F[InsertOneResult]

  def find(filter: Filter[A] = Filter.all)(using session: Option[ClientSession] = None): FindQuery[F, S, A]

  def updateOne(filter: Filter[A], update: Update[A], options: UpdateOptions = UpdateOptions.default)(using session: Option[ClientSession] = None): F[UpdateResult]

  def deleteOne(filter: Filter[A], options: DeleteOptions = DeleteOptions.default)(using session: Option[ClientSession] = None): F[DeleteResult]

  def findOneAndUpdate(filter: Filter[A], update: Update[A], options: FindOneAndUpdateOptions[A] = FindOneAndUpdateOptions.default[A])(using session: Option[ClientSession] = None): F[Option[A]]

  def aggregate[B](pipeline: Seq[Stage[A]])(using session: Option[ClientSession] = None)(using BsonDocumentDecoder[B]): AggregateQuery[F, S, B]

  def distinct[B](field: Field[A, B], filter: Filter[A] = Filter.all)(using session: Option[ClientSession] = None)(using BsonDecoder[B]): DistinctQuery[F, S, B]

  def createIndex(index: Index[A])(using session: Option[ClientSession] = None): F[String]

  def watch(options: WatchOptions[A] = WatchOptions.default[A])(using session: Option[ClientSession] = None)(using Streamable[S, ChangeEvent[A]]): S[ChangeEvent[A]]
// count, estimatedCount, insertMany, updateMany, deleteMany, bulkWrite, createIndexes, listIndexes, dropIndex,
// dropIndexes, renameCollection, drop, ...
```

Every method that reaches the server takes an optional `ClientSession` and defaults to `None`, so none of that is
visible until you opt into [a transaction](#sessions--transactions).

### Field selectors

`Field.of[E, A](_.someField)` is a macro that reads a field selector at compile time — no strings, no reflection —
and gives you a typed path to build filters, updates, and sorts:

```scala
val adults = Field.of[User, Int](_.age).gte(18)
val named  = Field.of[User, String](_.name).equalTo("Jenna") && adults
val setAge = Field.of[User, Int](_.age).set(27)
val city   = Field.of[Order, String](_.address.city).equalTo("Barcelona") // dotted paths from nested selectors
```

Each segment is checked against the case class it is selected from, so `_.name.length` and `_.items.head.sku` are
compile errors rather than paths that render fine and match nothing.

Selector-derived names are spelled through the collection's `FieldNaming`. Names that are already what the document
stores — a map key, an array position, `_id`, a shape with no case class — go through `at`, `/` or `Field.stored`,
and are used verbatim:

```scala
val totals = Field.of[Order, Map[String, Int]](_.totals)
val eur    = totals.at("EUR")                         // "totals" is renamed, "EUR" is not
val first: Field[Order, Item] = itemsField / "0"      // any other stored segment
val id     = Field.stored[Order, ObjectId]("_id")
```

### Filters

```scala
ageField.gte(13) && ageField.lte(19)     // $and
ageField.notIn(List(40, 41, 42))         // $nin
tagsField.contains("urgent")             // array membership
tagsField.containsAll(List("a", "b"))    // $all
tagsField.hasSize(3)                     // $size
nameField.regex("^A")                    // $regex
scoreField.exists                        // $exists
scoreField.hasType(BsonTypeName.Int)     // $type
scoreField.hasAnyType(BsonTypeName.Int, BsonTypeName.Long) // $type over several types
totalField.mod(4, 1)                     // $mod
flagsField.bitsAllSet(4)                 // $bitsAllSet — also $bitsAnySet, $bitsAllClear, $bitsAnyClear
Filter.text[User]("scala")               // $text
Filter.expr(totalField.expr > limitField.expr) // $expr, typed — see Aggregation expressions
Filter.expr[User](someBsonDocument)      // $expr, raw
Filter.jsonSchema[User](schema)          // $jsonSchema
locationField.near(berlin, maxDistance = Some(1500)) // $near — also $nearSphere
locationField.within(GeoShape.Within(area))          // $geoWithin
areaField.intersects(point)                          // $geoIntersects
```

The geospatial operators take GeoJSON `Geometry` values — `Point`, `LineString`, `Polygon` and the `Multi*` forms,
with longitude first as MongoDB expects. A polygon's rings have to close, so a ring that does not repeat its first
point is refused rather than sent for the server to reject. `$geoWithin` also takes the legacy `2d` shapes through
`GeoShape.Centre`, `CenterSphere` and `Box`.

`$near` sorts by distance and needs a geospatial index; distances are in metres for GeoJSON. The server refuses it
inside `$or` and inside an aggregation `$match` — `Stage.geoNear`, as a pipeline's first stage, is the tool there
(see [Nearest first](#nearest-first)). `$geoWithin` needs neither an index nor a sort, so it is the cheaper choice
when you only want containment.

The comparisons have symbolic aliases where they read better — `===`, `=!=`, `>`, `>=`, `<`, `<=` — spelling the
same `Filter` as `equalTo`/`notEqualTo`/`gt`/`gte`/`lt`/`lte`.

An operator that only means something for one kind of field compiles only on that kind: `regex` on text — a
`String`, an `Option[String]` or a collection of strings — `mod` on a number, `hasSize` on a collection, the `$bits`
operators on an integer (`Int`, `Long` or either in an `Option`). On any other field it is a compile error rather than
a filter the server accepts and that matches nothing.

`hasType` takes a `BsonTypeName`, not a string. The values are MongoDB's own `$type` aliases — `"object"` for a
document, `"binData"`, `"javascriptWithScope"` — which are easy to misspell into a filter that silently matches
nothing, so the enum spells them for you. `BsonTypeName.of(bsonValue)` names the type of a value you already have.

`itemsField.elemMatch(...)` is the one worth calling out: conditions combined on the array path alone can be
satisfied by *different* elements, and `$elemMatch` is how you require one element to satisfy all of them.

```scala
val bigOrder = itemsField.elemMatch(
  Field.of[Item, String](_.sku).equalTo("abc") && Field.of[Item, Int](_.quantity).gt(2)
)
```

An array of scalars has no fields to name, so `Field.self[A]` stands for the element itself. A condition on it renders
as the bare operator, and conditions joined with `&&` merge into one document — which is how `$elemMatch` takes a
range that a single comparison could not:

```scala
val inRange = scoresField.elemMatch(Field.self[Int].gte(80) && Field.self[Int].lt(85))
// {"scores": {"$elemMatch": {"$gte": 80, "$lt": 85}}}
```

`Filter.and`/`or` fold `Filter.all` and `Filter.none` away rather than emitting a one-element `$and`. That is what
makes an empty list safe: `field.in(Nil)` is `Filter.none`, not "match everything". A chain — `a && b && c` — renders
as one flat `$and` rather than each one nested inside the next.

The empty lists answer what the server answers: `in(Nil)` matches nothing, `notIn(Nil)` matches everything, and
`containsAll(Nil)` matches nothing, the way `$all: []` does — no document is ever counted as containing all of an
empty list.

### Updates

```scala
ageField.set(31)                  // $set
ageField.inc(1)                   // $inc — also $mul, $min, $max
nameField.unset                   // $unset
tagsField.push("vip")             // $push — also $pull, $addToSet, and the $each variants
itemsField.pullWhere(qtyField.lt(1))         // $pull by a condition on the elements
scoresField.pullWhere(Field.self[Int].gte(6)) // ...or on a scalar element itself
Update.setOnInsert(nameField, "") // $setOnInsert, for merge-style upserts
Update.rename(oldField, newField) // $rename — also currentDate, popFirst/popLast
Update.currentTimestamp(syncedField) // $currentDate as a BSON timestamp, on a Field[E, BsonTimestamp]
flagsField.bitOr(4)               // $bit — also bitAnd, bitXor
```

`$bit` works on integers only, so it compiles on an `Int` or `Long` field — or either in an `Option` — and takes a
mask of the field's own width. A missing field counts as zero, the way the server counts it.

Numeric operators only apply to numeric fields, so `Update.inc(nameField, 1)` does not compile. An `Option`-typed
field takes the unwrapped value — `scoreField.inc(5L)` on a `Field[User, Option[Long]]` — because `None` has no
numeric encoding, and the obvious stand-in, `$inc` by zero, is a write that quietly does nothing.

A numeric operator takes the field's own type, so on a `Field[User, Int]` only an `Int` compiles — `inc(1L)` and
`mul(1.5)` do not. That keeps *mongo4s* from being the one that changes the stored width. The **server** can still
change it: an `$inc` that overflows an `Int32` is stored as an `Int64`, silently, and a value that then no longer
fits the modelled `Int` is a decode error naming the value — `2147483711 is out of range for Int`. Model a counter
that can grow as `Long`, and read a field whose stored width you do not control as the widest type it can reach.

`$min` and `$max` take any type MongoDB orders — a date, a string, an `ObjectId` — not only a number, because that
is what the server compares. `$inc` and `$mul` stay numeric, since arithmetic is what they do.

`Update.combine`/`and` merge operators of the same name into one sub-document, so setting two fields produces one
`$set`. `Update.Raw` carries operators the `AST` does not model and merges the same way:

```scala
Update.set(nameField, "bob").and(Update.Raw[User](BsonDocument("$unset", BsonDocument("legacyNickname", BsonString("")))))
// {"$set": {"name": "bob"}, "$unset": {"legacyNickname": ""}}
```

Rendering never mutates the document you handed to `Raw`, so the same value can be reused across updates and
rendered under more than one `FieldNaming`.

One operator document holds one change per path, so merging cannot keep two: `Update.inc(n, 1).and(Update.inc(n, 2))`
would send only the second. An update that changes a path twice is refused when it renders, rather than losing the
first change. So is one whose paths overlap across operators — `set(name)` with `unset(name)`, or `unset(notes)` with
a `set` on `notes.0.rank` — which the server would refuse anyway, one round trip later.

#### Updating from the document's own values

An update that computes from the document itself — a field built from two others, a value run through an
aggregation expression — is a pipeline, not operators. `Update.pipeline` takes the stages the server accepts in one
(`$addFields`, `$project`, `$unset`, `$replaceRoot`, `$replaceWith`, or a `Stage.raw`) and refuses any other. The
values are [aggregation expressions](#aggregation-expressions):

```scala
val exclaimed = Update.pipeline(
  Stage.addFields[User]("name" := Field.of[User, String](_.name).expr ++ Expr.lit("!"))
)

collection.updateMany(Filter.all, exclaimed)
```

`updateOne`, `updateMany`, `findOneAndUpdate` and a bulk `UpdateOne`/`UpdateMany` all take one. Two pipelines `and`
into one; a pipeline and an operator update do not combine, because the server takes one or the other.

#### Pushing onto arrays

`pushAll` takes a `PushOptions[A]` carrying MongoDB's `$each` modifiers — `$slice`, `$sort` and `$position`. The
classic use is a capped, ordered log kept by the server:

```scala
tagsField.pushAll(List("vip"), PushOptions.default[String].sortedAscending.withSlice(10))
// {"$push": {"tags": {"$each": ["vip"], "$slice": 10, "$sort": 1}}}
```

`$sort` has two shapes and they are mutually exclusive, so setting one clears the other: `sortedAscending`/
`sortedDescending` for arrays of scalars, which the server sorts as bare values, and `sortedBy(Sort[A])` for arrays of
documents — `PushOptions.default[Note].sortedBy(Sort.desc(rankField))` renders `"$sort": {"rank": -1}`.

#### Updating array elements

The positional operators are methods on an array field, each giving a field of the element type, and `/` with another
`Field` reaches into the element — spelled through the collection's `FieldNaming` like every other selector. `each` is
`$[]`, every element:

```scala
val orderId  = Field.of[Order, String](_.id)
val items    = Field.of[Order, List[Item]](_.items)
val quantity = Field.of[Item, Int](_.quantity)

collection.updateOne(orderId.equalTo("1"), Update.set(items.each / quantity, 0))
```

`matched` is `$`, the first element the query matched — so the query has to match one:

```scala
collection.updateOne(items.elemMatch(Field.of[Item, String](_.sku).equalTo("abc")), Update.inc(items.matched / quantity, 1))
```

`where` is `$[identifier]`, the elements an *array filter* selects. The identifier is a `Field.element`, and the same
value builds both the update path and the filter, so the two cannot drift apart:

```scala
val low = Field.element[Item]("low")

collection.updateOne(
  orderId.equalTo("1"),
  Update.set(items.where(low) / quantity, 100),
  UpdateOptions.default.withArrayFilters(Seq((low / quantity).lt(3))),
)
// {"$set": {"items.$[low].quantity": 100}} with arrayFilters [{"low.quantity": {"$lt": 3}}]
```

On an array of scalars the element is the value itself: `scores.where(high)` with `high.gte(90)` as its filter. An
identifier the server would refuse — anything but a lowercase letter followed by letters and digits — is refused
when it is built.

#### Write options

`updateOne`/`updateMany` take an `UpdateOptions`, `replaceOne` a `ReplaceOptions`. Both are immutable, built by
chaining from `default`, and exist so that new options stay additive — a write method's signature never has to grow
another parameter again:

```scala
UpdateOptions.default                                     // nothing set
UpdateOptions.upsert                                      // shorthand for default.withUpsert
UpdateOptions.upsert.withArrayFilters(Seq(elementQty.lt(3)))
UpdateOptions.default.withCollation(caseInsensitive).withComment("nightly reconcile")
```

`deleteOne`/`deleteMany` take a `DeleteOptions` and `count` a `CountOptions`, on the same pattern:

```scala
collection.deleteMany(filter, DeleteOptions.default.withCollation(caseInsensitive))
collection.count(filter, CountOptions.default.withSkip(20).withLimit(10))
```

What each type carries today: `collation`, `hint` and `comment` everywhere; `let` variables on every write, which an
`$expr` reads as `Expr.variable`; `bypassDocumentValidation` on the two that write whole documents (`UpdateOptions`,
`ReplaceOptions`); `limit`, `skip` and `maxTime` on `CountOptions`.

`UpdateOptions` and `ReplaceOptions` also take a `sort`, which picks the one document `updateOne`/`replaceOne` changes
when the filter matches several (MongoDB 8.0+). It means nothing to `updateMany`, which changes every match, so
`updateMany` refuses one rather than letting the driver drop it without a word:

```scala
collection.updateOne(Filter.all, Update.set(nameField, "oldest"), UpdateOptions.default.withSort(Sort.asc(ageField)))
```

Inserts take options on the same pattern — `InsertOneOptions` and `InsertManyOptions`, with `comment` and
`bypassDocumentValidation`. An `insertMany` is ordered by default and stops at the first document that fails;
`InsertManyOptions.unordered` has the server carry on past it, and the `BulkWriteFailed` it raises lists every
failure, with the count of what landed:

```scala
collection.insertMany(users, InsertManyOptions.unordered)
```

Each operation family has its own options type on purpose rather than one shared bag: `upsert` means nothing on a
delete, and a type that offered it there would let you write down a request the server cannot answer.

The `findOneAnd*` trio works the same way, except its options carry the entity type because `sort` and `projection`
do — `FindOneAndUpdateOptions[A]`, `FindOneAndReplaceOptions[A]`, `FindOneAndDeleteOptions[A]`. `returnUpdated`
defaults to true, and `returningPrevious` is how you ask for the document as it was:

```scala
collection.findOneAndUpdate(filter, update)                                          // the updated document
collection.findOneAndUpdate(filter, update, FindOneAndUpdateOptions.default[User].returningPrevious)
collection.findOneAndUpdate(filter, update, FindOneAndUpdateOptions.upsert[User].withSort(Sort.asc(ageField)))
```

They carry `collation`, `hint`, `maxTime` and `comment` as well, and the update and replace ones
`bypassDocumentValidation`, the same as the plain writes.

An update that would produce no operators throws instead of sending `{}` — MongoDB rejects it, and failing at the
call site beats a write that silently does nothing.

`bulkWrite` takes a `Seq[WriteCommand[E]]` — `InsertOne`/`ReplaceOne`/`UpdateOne`/`UpdateMany`/`DeleteOne`/`DeleteMany`,
carrying the same `Filter` and `Update` values the single-document calls take:

```scala
collection.bulkWrite(
  Seq(
    WriteCommand.InsertOne(User("2", "Bob", 41)),
    WriteCommand.updateOne(named, setAge), // lowercase helpers default upsert = false
    WriteCommand.DeleteMany(ageField.lt(0)),
  )
)
```

Commands run in the order given; `BulkWriteOptions.unordered` lets the server carry on past one that fails instead of
stopping there, and `BulkWriteOptions` carries `comment`, `bypassDocumentValidation` and `let` for the whole bulk. The
delete commands take a `DeleteOptions` — `WriteCommand.deleteOne(filter, DeleteOptions.default.withHint(byAge))`.
`BulkWriteResult.upsertedIds` is keyed by each command's position in the sequence, and so is a failure: a
`MongoError.BulkWriteFailed` names each command that failed by its index, and its `result` is what the others wrote.

#### Writing across collections at once

MongoDB 8.0 writes to several collections — in several databases — in one command, the client-level `bulkWrite`. Each
collection turns its own `WriteCommand` into a write addressed to it, with its own codec and naming, and the client
sends them together:

```scala
client.bulkWrite(
  List(
    users.write(WriteCommand.InsertOne(User("3", "Cleo", 28))),
    orders.write(WriteCommand.updateOne(orderId.equalTo("1"), Update.inc(totalField, 5))),
  )
)
```

It takes the same `BulkWriteOptions` and returns a `ClientBulkWriteResult` of counts. A failure is a
`MongoError.ClientBulkWriteFailed` naming each failed write by its position and carrying the partial result. A
`comment`, `let` or `bypassDocumentValidation` belongs to the whole bulk here, so setting one on a single command's
options is refused rather than quietly dropped. It is not a transaction — the writes before a failure stay — but it
runs inside one, through `withTransaction`, when they must land together.

### Queries

`find` returns a builder; nothing is sent until `first`, `all`, `stream` or `attempting`:

```scala
collection
  .find(adults)
  .sort(Sort.asc(nameField))
  .projection(Projection.empty[User].include(ageField).withoutId)
  .skip(20)
  .limit(10)
  .hint(indexKeys)
  .collation(Collation.builder().locale("en").build())
  .maxTime(5.seconds)
  .batchSize(100)
  .comment("adults page 3")
  .all
```

`filter` narrows what is already there — it ands with the filter the query was built from — while `sort`,
`projection`, `skip` and `limit` replace. `first` reads one document rather than the query's `limit`. `find` also
takes `let` variables for its `$expr`, `allowDiskUse`, `noCursorTimeout`, and the cursor's `cursorType` and
`maxAwaitTime`. The same `hint`/`collation`/`maxTime`/`batchSize`/`comment` options are on `aggregate`, which adds
`allowDiskUse`, `let`, `maxAwaitTime` and `bypassDocumentValidation` — the last for a `$out` or `$merge` into a
validated collection — and `distinct` takes `collation`, `hint`, `comment`, `maxTime` and `batchSize`.
`estimatedCount` takes an `EstimatedCountOptions` with `maxTime` and `comment`.

A tailable cursor on a capped collection stays open after the last document and delivers each one written after it,
the way a log is tailed:

```scala
collection.find().cursorType(CursorType.TailableAwait).maxAwaitTime(1.second).stream
```

Its stream is bridged as a live stream, emitting each document as it arrives rather than once a batch fills — the
same bridge a change stream takes. `all` on it never finishes, since the cursor never ends; take from the stream
instead.

The numbers are checked where you write them, because MongoDB reads the edge cases as something else: a `limit` or
`batchSize` below one — 0 is no limit at all, and a negative one a single batch — a negative `skip`, and a `maxTime`
under a millisecond, which truncates to 0 and so to no time limit. Each throws at the builder, naming the value.

`distinct` over a field typed as an array answers with the elements' type, because that is what the server answers
with — `distinct(Field.of[User, List[String]](_.tags))` is a `DistinctQuery` of `String`, one value per distinct tag.

`Projection` keeps inclusion and exclusion apart, because MongoDB rejects a projection mixing them. They are separate
types — `Projection.empty` starts neutral and the first `include` or `exclude` commits it — so chaining `exclude` onto
an inclusion projection does not compile at all, rather than silently returning more fields than you asked for. `_id`
is the exception: `withoutId` drops it from an inclusion projection, giving `{"field": 1, "_id": 0}`.

`slice` cuts an array field down server-side, so a document with ten thousand comments does not travel to fetch the
last five. It is neither an inclusion nor an exclusion, so it chains onto any of the three without committing the
projection either way:

```scala
Projection.empty[Post].slice(commentsField, -5)                    // the last five
Projection.empty[Post].sliceFrom(commentsField, skip = 20, count = 10)
Projection.empty[Post].include(titleField).slice(commentsField, 5) // and still an inclusion
```

The field has to be an array — `slice` on a scalar does not compile. The document still decodes through the entity's
own codec, because the field is present and merely shorter.

Slicing a field the projection also includes keeps it once, sliced. A `$slice` alone would put the server back into
returning every field, so when the sliced fields are all an inclusion holds, `_id` is kept explicitly to hold it in
inclusion mode — and the one combination the server cannot express, only sliced fields and no `_id`, is refused
rather than answered with the whole document.

An inclusion projection can also compute fields from [aggregation expressions](#aggregation-expressions), in `find`
as well as in `$project` — `Projection.empty[User].include(nameField).compute("initial" := nameField.expr.substring(0, 1))`.

Sorting by `$text` relevance is `Sort.byTextScore()`, which renders `{"score": {"$meta": "textScore"}}` and takes
the output field's name if you want a different one. It is what makes `Filter.text` useful — an unranked text search
returns matches in whatever order the server finds them:

```scala
collection.find(Filter.text("mongodb")).sort(Sort.byTextScore()).all
```

`explain` answers the question a typed filter otherwise leaves open — did it use an index?

```scala
val plan: IO[BsonDocument] = collection.find(adults).sort(Sort.asc(nameField)).explain()

val measured: IO[BsonDocument] =
  collection.find(adults).explain(ExplainVerbosity.EXECUTION_STATS)
```

It is on `find` and on `aggregate`, runs the query the builder had already produced — options, hint and collation
included — and returns the server's plan as a `BsonDocument`. `explain` on `aggregate` describes the whole pipeline,
not the `$limit`-ed form `first` sends.

The document stays a document because an explain result is not one shape. It is a union that depends on how the
server chose to optimise: the same `aggregate(...).explain()` call returns a top-level `queryPlanner` when MongoDB
collapses the pipeline into a plain query, and a top-level `stages` array when it does not. Inside, the plan is a
recursive tree over an open set of stage names, each carrying its own fields, with a layer per shard on a sharded
cluster — and the server stamps `explainVersion` on it precisely because it reserves the right to change.

`ExplainSummary` types the part that *is* stable — the questions you actually ask of a plan:

```scala
import mongo4s.results.ExplainSummary

val summary: IO[ExplainSummary] =
  collection.find(adults).explain(ExplainVerbosity.EXECUTION_STATS).map(ExplainSummary.of)

// summary.indexes            List("age_1")
// summary.usedIndex          true
// summary.scannedCollection  false
// summary.sortedInMemory     false — a SORT stage means the order was not index-provided
// summary.execution          Some(ExecutionSummary(returned, keysExamined, docsExamined, durationMillis))
```

It reads by walking for field names rather than by following a fixed path, so it answers the same way for a find, a
pipeline the server optimised into a find, and a pipeline it kept as stages. `execution` is present only when the
plan was actually run — `ExplainVerbosity.EXECUTION_STATS` or higher. It is a summary, not a model of the output:
when it cannot recognise something it says nothing rather than guessing, and the full document is still there.

### Partial reads

`find(...).all` decodes whole entities, which means every modelled field has to be there. When you want a few
fields rather than the document, `selectAs` names the shape you want and gives you exactly that:

```scala
val summaries = collection.find(adults).sort(Sort.asc(nameField)).selectAs[(name: String, age: Int)].all
// F[List[(name: String, age: Int)]]

summaries.map(_.map(_.name)) // fields are read by label, not by position
```

The named tuple is the whole specification. The projection sent to the server is built from its labels, and so is
the decoder, so the two cannot drift apart — there is no second place to keep in step, and no case class and codec
to declare for a shape you only wanted once. A label that is not a field of the entity, or a field asked for at the
wrong type, is a compile error naming it.

`selectAs` closes the chain: `filter`, `sort`, `skip` and `limit` are set before it, and `first`, `all`, `stream`
and `attempting` after it. It works the same on a `getDirectCollection`, which is where it matters most — the
entity's `WireCodec` demands every modelled field, so before this a partial read there was not possible at all.

Labels are *derived* names, spelled through the collection's `FieldNaming` exactly as `Field.of` is, so a
`(fullName: String)` on a `snakeCase` collection projects and reads `full_name`.

An `Option` label reads a field the document does not carry as `None`, which is the answer reading the whole entity
gives for the same document — a projected shape and a full read never disagree about the same absence. A field the
entity declares as required but the document is missing is still a decode error naming it, reported per document by
`attempting`.

One field per label: `selectAs[(city: String)]` cannot reach `address.city`, because a nested projection comes back
as a nested document that a flat result cannot represent. Project the outer field, or read the shape through
`getCollection` with a codec of your own.

### Results

Writes report what actually happened. `UpdateResult` carries `matchedCount`, `modifiedCount` and `upsertedId`, which
is the only way to tell "matched but unchanged" from "nothing matched", or to recover the id an upsert generated:

```scala
val result = collection.updateOne(filter, update, UpdateOptions.upsert)
result.map(r => if r.wasUpserted then r.upsertedId else None)
```

`DeleteResult` carries `deletedCount`; `BulkWriteResult` carries all four counts plus `upsertedIds`, keyed by the
position of the command that produced it. `InsertOneResult`/`InsertManyResult` carry the stored `_id`s.

**`upsertedId` is the document's `_id`, not your primary key**, and it is reported only when the upsert actually
inserted — an upsert that matched reports `matchedCount = 1` and `None`. Whether that `_id` is anything you
recognise depends on where your key lives:

| `PrimaryKey` | what ends up in `_id` | `upsertedId` | same as your `K`? |
| --- | --- | --- | --- |
| `single("id")(_.id)` — key on a plain field | generated by the server | `BsonObjectId` | **no** — different value *and* different type |
| `storedId(_._id)` — key **is** `_id` | your key | `BsonString("k1")` | yes |
| `WithId[ObjectId, E]` | the `ObjectId` you constructed it with | that same `ObjectId` | yes |

Read that table as three common arrangements, not as a rule — `_id` and `K` are independent. What lands in `_id`
is decided by the **entity's codec**; `PrimaryKey` only decides what the repository filters on. Let them disagree
and `upsertedId` is neither:

```scala
final case class Doc(_id: String, tenant: String, name: String) derives MedeiaDocumentCodec
object Doc:
  given PrimaryKey[Doc, String] = PrimaryKey.single("tenant")(_.tenant)

repo.upsert(Doc("d1", "tenant-A", "x"))  // K is "tenant-A"; upsertedId is BsonString("d1")
```

`_id` can also be an `Int`, or a whole subdocument, or pinned by the filter (`_id` equality in an upsert's filter
is copied into the document the server creates). That is why the type is `BsonValue` and not something narrower: at
the collection level there is no `K` to narrow it to, and at the repository level the key is not what the field
holds. `Repository.insertOne` returns `K` for exactly this reason — the key comes from the entity, which is right
in every arrangement above, whereas the id is only ever right about `_id`.

This all holds identically for `collection.replaceOne`/`updateOne` with `upsert` and for `Repository.upsert`/
`upsertMany` — the repository builds the same command.

Under an unacknowledged write concern (`w=0`) the server sends nothing back, so every one of these comes back empty —
zero counts, `None` for the ids. That is indistinguishable from a write that matched nothing, which is the trade
`w=0` makes: the driver has no answer to report. Nothing throws, so the write path stays usable; if you need to tell
the two apart, do not use `w=0`. `Repository.insertOne`/`insertMany` are the one exception: their key
comes from the entity you passed, never from the server, so they still return it under `w=0` — a write that failed
silently there returns a key just the same, exactly as `w=0` asks for.

### Reads that survive bad documents

`find(...).all` fails the whole query if any document does not decode. A collection is rarely written by one version
of one service, though, and when it isn't, a document that doesn't fit is a fact about the data rather than a reason
to lose the rest of the page. `attempting` reports each document separately:

```scala
val readable: IO[List[User]] =
  collection.find().attempting.all.map(_.collect { case Right(user) => user })

val everything: S[DecodeResult[User]] = collection.find().attempting.stream
```

`DecodeResult[A]` is `Either[BsonError, A]`. It's available on `find`, `aggregate` and `distinct`, and as
`watchAttempting` on a collection's `watch` — `watchAsAttempting` at the client and database level, where the
element type is named at the call site. Transport errors still fail the effect — only decoding is made per-document.

A change stream is the one place the decode result sits *inside* the element: `watchAttempting` gives a
`ChangeEvent[DecodeResult[A]]`, so an event whose document does not decode still arrives whole — its operation, its
namespace, its key and, above all, its resume token. A consumer can set the bad document aside and still checkpoint
past it, instead of reading it again on every restart.

### Errors the server raises

A failed write does not arrive as a driver exception. `mongo4s` translates every failure the driver reports into
`MongoError`, so branching on one is a pattern match rather than a code comparison:

```scala
import mongo4s.MongoError

collection.insertOne(user).recoverWith {
  case MongoError.DuplicateKey(_)  => collection.replaceOne(byEmail, user).void
  case MongoError.WriteConflict(_) => retry
}
```

| Case | Raised for |
| --- | --- |
| `DuplicateKey` | a unique index violation |
| `WriteConflict` | two transactions touching the same document |
| `ExecutionTimeout` | `maxTime` expired, or the server's own limit |
| `Unauthorized` | the credentials do not allow the operation |
| `Unavailable` | the socket failed, or no server could be selected |
| `BulkWriteFailed` | one bulk command, several failures — `failures` keeps them all, `duplicateKeys` filters |
| `Failed` | anything else the server reported, with its `code` intact |

Every case carries `cause`, the driver's own exception, so nothing is lost — along with `code`, `labels` and
`hasLabel`, which is how `withTransaction` decides what to retry. Errors `mongo4s` raises itself are untouched:
`BsonError.DecodingFailure` for a document that does not fit, `RsBridgeError` for the stream bridge.

Translation happens on the `Publisher` before any runtime sees it, so `IO`, `Task`, `KIO`, `rapid.Task` and every
stream get the same type for the same failure. What `MongoError.DuplicateKey` deliberately does *not* carry is the
name of the index that was violated: the server sends it, but the driver's `WriteError.getDetails` comes back empty,
so the only remaining source is the human-readable message — and scraping that would be a guess that silently
changes with a server upgrade. Read `getMessage` when you need it.

### Primary keys

`PrimaryKey[E, K]` turns an entity into a key-based filter — a single field, a native `_id` (`ObjectId` or your own
encoder), or a compound key of any width:

```scala
given PrimaryKey[User, String]   = PrimaryKey.single("id")(_.id)
given PrimaryKey[Note, ObjectId] = PrimaryKey.storedId(_.id) // keys on "_id" — see WithId

given PrimaryKey[Order, (userId: String, seq: Int)] =
  PrimaryKey.compound(o => (userId = o.userId, seq = o.seq), FieldNaming.snakeCase)
```

`PrimaryKey.id(_.id)` is `single("id")` spelled short, for the common case.

A compound key is a **named tuple**, so its labels are the field names — there is no second list of strings to keep
in step with the extractors, and no arity ceiling. The labels are Scala identifiers; the optional `FieldNaming`
spells them the way the collection stores them, so `userId` above is written as `user_id` while the key still reads
as ordinary Scala at every call site:

```scala
users.findOne((userId = "u1", seq = 3))
```

The order of the labels is part of the key's type, so writing them in a different order at a call site is a compile
error rather than a key silently assembled wrong.

Field names are known without a key value — that is what lets
`repository.ensureKeyIndex` build the unique index that makes the key a key. Without one, two concurrent `upsert`s
on the same key can both miss and both insert.

`storedId` keys on `_id` whatever the key's type; the entity's codec has to actually write it there, so an entity
whose codec writes `id` will never match. Note that these are **stored** names, used verbatim: the collection's
`FieldNaming` is not applied to them, so under `snakeCase` write `"user_id"`, not `"userId"`.

For entities that don't carry their own id field, `WithId[ObjectId, E]` ships a ready-made `PrimaryKey` — no `given`
needed.

`inFilter` on a compound key produces an `$or` of `$and`s; on a single field it's a plain `$in` — and an empty key
list always produces `Filter.none`, so `findMany(Nil)`/`deleteMany(Nil)` are safe no-ops instead of matching every
document.

### Sessions & transactions

`client.withTransaction` opens a session, runs the body in a transaction on it, and closes it — on every path:

```scala
client.withTransaction {
  users.insertOne(User("2", "Bob", 41)) // Option[ClientSession] is already given here
}
```

The session is given implicitly to everything inside the body, so a collection or repository call joins the
transaction without being told to — no `(using Some(session))` at each call site.

**A helper defined elsewhere has to ask for it.** The session reaches calls that are written inside the body;
a method compiled somewhere else was compiled against the default — no session — and its writes commit on their own,
outside the transaction, which a rollback then does not undo. Give any helper the parameter and it joins:

```scala
def register(users: MongoCollection[F, S, User], user: User)(using Option[ClientSession]): F[Unit] =
  users.insertOne(user).void          // joins the caller's transaction

def register(users: MongoCollection[F, S, User], user: User): F[Unit] =
  users.insertOne(user).void          // commits outside it, silently
```

The two shapes differ by one `using` clause and by whether a rollback takes the write with it; there is no warning,
because a missing implicit is exactly what the default is there to supply. If you prefer passing a value, open the
session yourself with `client.startSession` and use `session.withTransaction`.

The same holds for a call **built** before the transaction and only run inside it. A call takes its session when it
is written, not when it runs, so `val write = users.insertOne(user)` outside the block commits on its own even when
`write` runs inside it. Write the call inside the body, or build it in a function that takes the session.

It commits on success and rolls back on failure **and on cancellation**: `Effect[F]` carries a `guaranteeCase` that
sees how the action ended, so an interrupted transaction does not linger on the server until it is reaped. A
rollback that itself fails is attached as a suppressed exception rather than replacing the error that caused it.

It also **retries**, the way the driver's own `withTransaction` does. A `TransientTransactionError` means nothing
was committed, so the whole transaction — body included — is started again; an `UnknownTransactionCommitResult`
means the commit may already have landed, so only the commit is asked again rather than the work being redone. Both
are bounded by one deadline taken before the first attempt, 120 seconds by default:

```scala
import mongo4s.operations.TransactionOptions

client.withTransaction(users.insertOne(User("2", "Bob", 41)), TransactionOptions.default.withRetryTimeout(10.seconds))

client.withTransaction(body, TransactionOptions.withoutRetries) // report the first failure, as 2.x did
```

`TransactionOptions` also carries the transaction's own `readConcern`, `writeConcern`, `readPreference` and
`maxCommitTime`. Errors the server did not label are never retried — a failure in your own code fails the
transaction immediately, exactly as before.

Two rules follow the driver specification for this API. Between one attempt of the whole transaction and the next
there is a short random pause — up to 5 ms, growing by half each attempt to a ceiling of 500 ms — so two clients
that conflicted do not retry in lockstep and conflict again. And a commit that ran out of `maxCommitTime` is not
asked again, even though the server labels it as possibly committed: a limit you set is not one to keep retrying past.

The deadline is measured with `Effect.monotonic`, which has a default implementation reading `System.nanoTime`; the
`cats` and `zio` backends override it with their runtime's own clock, so `TestControl`/`TestClock` can drive the
retry window in a test without waiting on a real one.

If you're reusing one already-open session across more than one transaction, the same behaviour applies to the session
itself — it commits and rolls back the same way, but leaves the session's own lifetime to you:

```scala
import mongo4s.withTransaction // the extension on ClientSession

for
  session <- client.startSession
  _       <- session.withTransaction(users.insertOne(User("2", "Bob", 41)))
  _       <- session.withTransaction(users.insertOne(User("3", "Carol", 29))) // same session, second transaction
  _       <- IO.delay(session.close())
yield ()
```

`MongoSession.startTransaction`/`commitTransaction`/`abortTransaction` are there for the fully manual path, where
you pass the session explicitly with `(using Some(session))` at each call site. Nothing is automatic on that path —
including the rollback.

A session is useful without a transaction too — to read your own writes causally, or to read one point in time from
a snapshot. `client.withSession` opens one, gives it to every call in the body the same way, and closes it however
the body ends:

```scala
import mongo4s.operations.SessionOptions

client.withSession(
  for
    before <- users.count()
    rows   <- users.find().all // the same point in time as the count, whatever is written meanwhile
  yield (before, rows),
  SessionOptions.snapshot,
)
```

`SessionOptions` carries `causallyConsistent`, `snapshot` and the `TransactionOptions` a session's transactions
start with; a session cannot be both a snapshot and causally consistent, so asking for both throws. `startSession`
takes the same options when you want the session's lifetime in your own hands.

Transactions need a replica set or sharded cluster.

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

// aggregate lives on the collection, and decodes into any type with a BsonDocumentCodec
val adults: IO[List[User]] = collection.aggregate[User](pipeline).all
```

Grouping uses typed accumulators:

```scala
import mongo4s.operations.Accumulator

val byAge = Seq(
  Stage.groupBy(Field.of[User, Int](_.age))(
    "count" -> Accumulator.count[User],
    "names" -> Accumulator.push(Field.of[User, String](_.name)),
  )
)
```

Beyond `sum`/`avg`/`min`/`max`/`first`/`last`/`push`/`addToSet`/`count`, the accumulators MongoDB added in 5.x
and 7.0 are typed too:

```scala
Stage.groupBy(cityField)(
  "best"   -> Accumulator.topN(Sort.desc(scoreField), nameField, 3), // also top, bottom, bottomN
  "recent" -> Accumulator.lastN(nameField, 2),                      // also firstN, maxN, minN
  "median" -> Accumulator.median(ageField),
  "p90"    -> Accumulator.percentile(ageField, 0.9),
  "spread" -> Accumulator.stdDevPop(ageField),                      // also stdDevSamp
  "traits" -> Accumulator.mergeObjects(traitsField),
)
```

`top`/`bottom` and their `N` forms rank by the `Sort` they are given and output one field; `firstN`/`lastN` follow
the order the documents arrive in, so sort before the group if that order matters. `firstN`, `lastN` and the ranking
forms keep a missing value as `null`, while `maxN`/`minN` skip it. `median`, `percentile` and the standard
deviations compile only on a numeric field, and the first two always ask for the server's only method,
`approximate`. An `n` below one, an empty sort and a percentile outside `0..1` are refused where they are written.

`Stage` covers `$match`/`$project`/`$sort`/`$limit`/`$skip`/`$count`/`$unwind`/`$lookup`/`$graphLookup`/`$group`/
`$addFields`/`$unset`/`$replaceRoot`/`$replaceWith`/`$facet`/`$sample`/`$sortByCount`/`$unionWith`/`$bucket`/
`$bucketAuto`/`$densify`/`$fill`/`$setWindowFields`/`$geoNear`/`$redact`/`$out`/`$merge`, and Atlas Search's
`$search`/`$searchMeta`/`$vectorSearch` (see [Atlas Search](#atlas-search)), with `Stage.raw(document)` as the
escape hatch for anything else. `$unwind` takes `includeArrayIndex` to number each element it unwinds, and
`$unionWith` a pipeline over the other collection — `Stage.unionWith[Order, Order]("archive", List(Stage.matching(recent)))`.

`$addFields`, a `$group` key, an accumulator's input, `$replaceWith` and `$redact` take
[aggregation expressions](#aggregation-expressions) — computed values, typed:

```scala
Stage.addFields[Order]("total" := priceField.expr * qtyField.expr)
Stage.groupByExpr(createdAtField.expr.year)("revenue" -> Accumulator.sum(priceField.expr * qtyField.expr))
Stage.groupByKeys[Order]("city" := cityField.expr, "month" := createdAtField.expr.month)("orders" -> Accumulator.count[Order])
Stage.replaceWith(Expr.mergeObjects(Expr.root[Order], Expr.document("total" := priceField.expr * qtyField.expr)))
```

`$unset` drops fields by name — the pipeline's spelling of an excluding `$project`. It takes one field, and more with
`and`, because several fields of unrelated types have no single `Field` type to share:

```scala
Stage.unset(ageField).and(tagsField)
```

`$redact` keeps, prunes or descends into each level of a document by an aggregation expression, and `Expr.keep`,
`Expr.prune` and `Expr.descend` are the three answers it can give:

```scala
Stage.redact(Expr.cond(levelField.expr === Expr.lit(1), Expr.prune, Expr.descend))
```

A raw `BsonValue` still works, with `Stage.Descend`, `Stage.Prune` and `Stage.Keep` as the answers spelled in BSON.

`$lookup` has three forms. `Stage.lookup` is the equality join on `localField`/`foreignField`; `Stage.lookupWith`
takes a sub-pipeline that runs against the foreign collection, with `let` binding values from the outer document; and
`Stage.lookupWithKeys` does both — joins on the keys, then narrows what joined with the sub-pipeline, the concise
form MongoDB 5.0 added. Its keys are typed on both sides, so joining fields of different types does not compile:

```scala
Stage.lookupWith[Order, User](
  from = "users",
  pipeline = List(Stage.matching(Field.of[User, Int](_.age).gte(18))),
  as = "adults",
  let = Some(BsonDocument("owner", BsonString("$user_id"))),
)

Stage.lookupWithKeys[Order, User, String](
  from = "users",
  localField = Field.of[Order, String](_.userId),
  foreignField = Field.of[User, String](_.id),
  pipeline = List(Stage.matching(Field.of[User, Int](_.age).gte(18))),
  as = "adultOwner",
)

Stage.graphLookup[Order, User, String](
  from = "users",
  startWith = BsonString("$user_id"),
  connectFrom = Field.of[User, String](_.managerId),
  connectTo = Field.of[User, String](_.id),
  as = "chain",
  options = GraphLookupOptions.default[User].withMaxDepth(3).withDepthField("depth"),
)
```

Note the second type parameter: the sub-pipeline and the `connectFrom`/`connectTo` fields belong to the **foreign**
collection, so they are typed against it rather than against `A`. They are rendered with the outer collection's
`FieldNaming` — fine when a project uses one convention throughout, which is the usual case; if the two collections
disagree, spell the foreign names with `Field.stored`.

`$out` and `$merge` write the pipeline's result into a collection, and both take options that stay out of the way
until you need them — with none set they render as the bare collection name the server also accepts:

```scala
Stage.out[Order]("archive")                                        // {"$out": "archive"}
Stage.out[Order]("archive", OutOptions.default.inDatabase("cold")) // {"$out": {"db": "cold", "coll": "archive"}}

Stage.merge[Order](
  "archive",
  MergeOptions.default
    .onFields(List("user_id", "seq"))
    .whenMatched(MergeOptions.WhenMatched.KeepExisting)
    .whenNotMatched(MergeOptions.WhenNotMatched.Insert),
)
```

#### Aggregation expressions

An `Expr[E, A]` is an aggregation expression that reads a document of `E` and computes an `A`. `field.expr` turns a
`Field` into one — spelled through the collection's `FieldNaming`, and reading an `Option[A]` field as the `A` it
holds — `Expr.lit` a value, and `"name" := expr` or `field := expr` a computed field:

```scala
val total: Expr[Order, Double] = priceField.expr * qtyField.expr        // a Double times an Int is a Double
val net                         = total - discountField.expr.orElse(Expr.lit(0.0)) // $ifNull when the field is null
val size                        = Expr.cond(qtyField.expr > Expr.lit(10), Expr.lit("bulk"), Expr.lit("single"))

Stage.addFields[Order]("total" := total, "size" := size)
```

The operators are typed by the capability the server needs, the same way filters are. Arithmetic compiles on numbers
and combines widths the way the server does — an `Int` with a `Long` is a `Long`, anything with a `Double` a `Double`,
anything with a `BigDecimal` a `BigDecimal` — and `/` is always a `Double`. A comparison takes two values of one kind,
or two numbers. The string operators compile on text, the array ones on a collection, the date ones on an `Instant`:

| | |
| --- | --- |
| numbers | `+` `-` `*` `/` `%` `abs` `ceil` `floor` `sqrt` `round` |
| comparison | `equalTo` `notEqualTo` `lt` `lte` `gt` `gte`, or `===` `=!=` `<` `<=` `>` `>=` |
| logic | `&&` `\|\|` `!`, `Expr.cond`, `Expr.switch`, `orElse` |
| text | `++` `toLower` `toUpper` `trimmed` `length` `substring` `split` `matches` |
| arrays | `size` `at` `contains` `concatArrays` `filter` `map` |
| dates | `year` `month` `dayOfMonth` `dayOfWeek` `hour` `minute` `second` `millisecond` `truncated` `formatAs` `plus` `minus` `until` |
| conversion | `asString` `toInt` `toLong` `toDouble` `toDecimal` `toBoolean` `toDate` `typeName` |
| documents | `Expr.document`, `Expr.mergeObjects`, `Expr.root` |
| variables | `Expr.variable` for `let`, `Expr.now`, `Expr.searchScore` / `vectorSearchScore` / `textScore` |

`filter` and `map` hand you the element as an expression, and `get` reads a field of it:

```scala
itemsField.expr.filter(item => item.get(itemQty) > Expr.lit(1))
itemsField.expr.map(item => item.get(itemPrice) * item.get(itemQty))
```

The element is MongoDB's `$$this`; nesting one `filter` or `map` inside another needs `filterAs`/`mapAs` with a name of
its own, so the inner one does not hide the outer.

Three things the Scala syntax decides rather than the library. `->` binds tighter than `>`, so a `switch` branch whose
condition is a comparison needs parentheses — `(qtyField.expr > Expr.lit(10)) -> Expr.lit("big")`. ScalaTest's
`Matchers` bring a `===` of their own that wins inside a spec, so write `equalTo` there. And a `let` variable is read
with `Expr.variable[Int]("limit")`, which has no field to check the name against.

`Expr.raw` carries any expression the AST does not model. The fake does not evaluate expressions, so it refuses every
stage, projection and filter that holds one, by name.

#### Nearest first

`$geoNear` is `$near` as a stage: it sorts by distance, writes each document's distance into a field you name, and —
unlike `$near` — lets any other stage follow. The server takes it only as a pipeline's first stage, and only over a
geospatial index:

```scala
Stage.geoNear(locationField, Geometry.Point(13.405, 52.52), "distance")

Stage.geoNear(
  locationField,
  Geometry.Point(13.405, 52.52),
  "km",
  GeoNearOptions.default[Place].where(openField.equalTo(true)).withMaxDistance(5000).withDistanceMultiplier(0.001),
)
```

The point is GeoJSON, so distances are in metres: `withDistanceMultiplier` rescales what is written, not what
`withMinDistance`/`withMaxDistance` read, and a negative distance is refused before the server sees it. The field is
always sent as `key`, which the server needs as soon as a collection has more than one geospatial index, and
`withIncludeLocs` names a field to receive the location that matched. Legacy `2d` coordinate pairs are not modelled —
`Stage.raw` carries them.

#### Bucketing, filling gaps and windows

`$bucket` takes its boundaries as values of the field being grouped, so they are typed rather than raw BSON, and
`default` names the bucket for everything outside them — without it the server rejects such a document:

```scala
Stage.bucketBy(ageField, Seq(0, 20, 40), default = Some(BsonString("other")))("count" -> Accumulator.count[Person])
```

`$bucketAuto` picks the boundaries itself, aiming for the number of buckets asked for with about as many documents in
each. `bucketAutoRounded` also rounds them to a preferred-number series — `R5` to `R80`, `1-2-5`, `E6` to `E192`, or
powers of two — which the server only does for numbers, so it compiles only on a numeric field:

```scala
Stage.bucketAuto(nameField, 4)("count" -> Accumulator.count[Person])
Stage.bucketAutoRounded(ageField, 5, BucketGranularity.R10)()
```

`$sortByCount` is the `$group`-then-`$sort` written by hand everywhere: one `{_id: value, count: n}` per value, most
frequent first, ties in no particular order:

```scala
Stage.sortByCount(cityField)
```

`$densify` fills the gaps in a series, so every step is present whether or not a document was written for it —
useful before a window function that would otherwise skip the missing rows:

```scala
Stage.densify(ageField, DensifyRange.by(10).within(DensifyBounds.Between(BsonInt32(0), BsonInt32(40))))
Stage.densify(recordedAt, DensifyRange.every(1, DateUnit.Hour).within(DensifyBounds.Partition), Seq(sensorField.path))
```

`$setWindowFields` computes an accumulator over a span around each document rather than over the whole group. The
sort is what gives a window its direction, so it is required rather than optional; an output with no `window` covers
the whole partition:

```scala
Stage.setWindowFields(Sort.asc(ageField), partitionBy = Some(sensorField.path))(
  "runningTotal" -> WindowOutput(Accumulator.sum(ageField)).over(Window.documents(WindowBound.Unbounded, WindowBound.Current))
)
```

`Window.documents` counts in rows, `Window.range` in the sort field's own values — rows sharing a value fall in the
same window — and `Window.rangeOver` does the same for a date field measured in a `DateUnit`.

The window functions — what only makes sense in a window, never in a `$group` — sit beside the accumulators in the
same output list, as `WindowFunction`s:

```scala
Stage.setWindowFields(Sort.asc(dayField), partitionBy = Some(shopField.path))(
  "total" -> WindowOutput(Accumulator.sum(amountField)),
  "place" -> WindowFunction.rank[Sale],                                  // also denseRank, documentNumber
  "prev"  -> WindowFunction.shift(amountField, by = -1, default = Some(0)),
  "kept"  -> WindowFunction.locf(amountField),                           // also linearFill
  "slope" -> WindowFunction.derivative(amountField, Window.range(WindowBound.At(-1), WindowBound.Current), Some(DateUnit.Hour)),
  "area"  -> WindowFunction.integral(amountField, Window.documents(WindowBound.Unbounded, WindowBound.Current)),
  "trend" -> WindowFunction.expMovingAvg(amountField, 7),                 // or expMovingAvgAlpha(amountField, 0.3)
)
```

`$derivative` and `$integral` need a window, so it is a parameter rather than an option; the others take none.
`linearFill`, the calculus and the moving average compile only on a numeric field, and a moving average over fewer
than one document, or with an `alpha` outside `(0, 1)`, is refused where it is written.

`$fill` fills the nulls and the missing values a `$densify` leaves behind — with a value, with the last value seen, or
with the line between the neighbours. The last two follow the sort order, so they need a `sortBy`:

```scala
Stage.fill(Sort.asc(recordedAt), partitionBy = Seq(sensorField.path))(
  FillOutput.linear(temperatureField), // numeric fields only
  FillOutput.locf(statusField),
  FillOutput.value(noteField, "none"),
)
```

Where there is nothing to fill from — no value before a gap for `locf`, no neighbour on one side for `linear` — the
server writes an explicit `null` rather than leaving the field missing.

These stages take their partition fields as `FieldPath` rather than `Field`, which is why the examples above write
`sensorField.path`: a partition is a list of fields of unrelated types, and `Field[E, ?]` loses the type the path
extension needs. Everything else about them is the usual typed selector.

`on` names fields of the **target** collection, so it is a list of stored names rather than `Field` values — the
target's shape is not `A`. A single field renders as a string and several as an array, which is what the server
expects. Note that `aggregate(...).first` does not append its usual `$limit` after a terminal stage, since the server
rejects anything following `$out`/`$merge`.

Two things about the typing. `A` is the pipeline's *starting* document type and never changes down the pipeline — a
`$group` or `$project` invents a new shape, but the stages after it are still typed against `A`, and the pipeline's
real output type is stated once, at `aggregate[B]`. And a field belonging to a stage's own output — `_id` after a
`$group`, a facet's counter — has no `Field[A, _]` to name it, so it goes through `Stage.raw`:

```scala
val buckets = Seq(
  Stage.groupBy(ageField)("count" -> Accumulator.count[User]),
  Stage.raw[User](BsonDocument("$sort", BsonDocument("_id", BsonInt32(1)))), // "_id" is the group's, not User's
)
```

`aggregate` asks for a `BsonDocumentDecoder[B]`, not a full codec: a pipeline's output is read and never written,
so a read model needs no encoder you would have to invent. A `BsonDocumentCodec` satisfies it, and
`aggregate[BsonDocument]` works out of the box — useful for `$facet` and ad-hoc `$project`s.

`aggregateDirect[B]` is the AST-free counterpart: it asks for a `WireCodec[B]` rather than a `BsonDocumentDecoder[B]`
and, on a collection opened with `getDirectCollection`, decodes the pipeline's output straight off the wire with no
`BsonDocument` in between — the same trip `find` already makes there.

```scala
final case class ByAge(_id: Int, total: Int) derives WireCodec

collection.aggregateDirect[ByAge](Seq(Stage.groupBy(ageField)("total" -> Accumulator.count[User]))).all
```

It carries the direct path's strictness with it — every field the output type declares has to be present, exactly
as for an entity read through `getDirectCollection`, so a model with a field the pipeline does not produce is a
decode error rather than a default. `attempting` reports it per document.

On a collection opened with `getCollection` the same call still works — the `WireCodec` is bridged to a document
codec — so the method is about the codec you have, not about which constructor you used.

It is worth reaching for when the pipeline returns *a lot*. Measured against a real MongoDB it is **1.40×** the
bridged path over ten thousand documents and indistinguishable from it over ten, where the round trip is all there is
— see [BENCHMARKS.md](BENCHMARKS.md#aggregation-through-a-cursor--real-mongodb).

### Indexes

`Index[E]` is built from the same field selectors, and carries the options MongoDB attaches to an index:

```scala
import mongo4s.operations.Index

collection.createIndex(Index.ascending(nameField).descending(ageField).named("name_age"))
collection.createIndex(Index.unique(idField))
collection.createIndex(Index.ascending(createdAtField).expiringAfter(30.days)) // TTL
collection.createIndex(Index.ascending(ageField).where(ageField.gte(18))) // partial
collection.createIndex(Index.text(bioField).withSparse)
collection.createIndex(Index.text(titleField).text(bioField).withWeight(titleField, 10).withDefaultLanguage("spanish"))
collection.createIndex(Index.hashed(idField)) // sharding
collection.createIndex(Index.geo2DSphere(Field.stored[User, Any]("location"))) // also geo2D
collection.createIndex(Index.ascending(ageField).withHidden) // built, but ignored by the planner
collection.createIndex(Index.ascending(Field.stored[User, Any]("$**"))) // wildcard: just a stored path

collection.listIndexes // F[List[BsonDocument]], as the server reports them
collection.dropIndex("name_age")

collection.createIndexes(List(Index.ascending(nameField), Index.descending(ageField))) // one command
collection.dropIndexes // every index but the one on _id
```

Keys are ordered — a compound index is only usable by queries that respect that order. `createIndex` returns the
name the server gave it, and is idempotent, so it's safe on every start. `createIndexes` builds several in one
command and returns their names in the order given; the server builds them together, so if one cannot be built, none
is.

A wildcard index needs no API of its own: `$**` is an ordinary stored path, so `Field.stored` builds it and the
collection's `FieldNaming` leaves it alone. `withWildcardProjection` narrows which fields such an index covers, and
`withCollation` attaches one to any index.

`expireAfterSeconds` is a whole number on the server, so a sub-second `TTL` is rejected rather than silently truncated
to "expire immediately". When "immediately" is what you mean — each document carries its own expiry time and should
go when that time comes — say so with `expiringAtFieldTime`, which is a `TTL` of zero asked for on purpose:

```scala
collection.createIndex(Index.ascending(expiresAtField).expiringAtFieldTime)
```

A text index weighs its fields with `withWeight` — a match in a field of weight 10 scores ten times one of weight 1 — and
`withDefaultLanguage` and `withLanguageOverride` choose the stemming language, the second from a field of each
document.

A partial index takes the filter operators the server accepts there: equality, the comparisons, `$in`, `exists`,
`hasType`, `$and` and `$or`. Negations are not among them — and `!f` renders as `$nor`, so `Index.where(!f)` is one
the server refuses. Write the positive form of the condition instead.

From a repository, `ensureKeyIndex` builds the unique index the `PrimaryKey` describes, without you restating its
fields. A key stored as `_id` needs none — the server keeps `_id` unique on its own and refuses a `unique` flag on it
— so for such a key `ensureKeyIndex` builds nothing and answers `_id_`, the index that is already there.

### Atlas Search

Full-text and vector search run on Atlas Search indexes, which live beside the collection rather than in it. A
collection creates, lists, redefines and drops them:

```scala
collection.createSearchIndexes(
  List(
    SearchIndex.dynamic[Movie](),                                                       // named "default", every field mapped
    SearchIndex.vector("plots", embeddingField, 1536, VectorSimilarity.Cosine).filteringOn(yearField),
  )
)
collection.listSearchIndexes // F[List[BsonDocument]], each with its status and whether it is queryable
collection.dropSearchIndex("plots")
```

An index builds in the background, and a search against it finds nothing until `listSearchIndexes` reports it
`queryable`. `SearchIndex.of(name, definition)` takes a mapping you write yourself, and the vector index compiles only
on a numeric array field.

`$search` takes a `SearchOperator`. The common ones are typed — the text ones on text fields, `between`/`atLeast`/
`below` on fields the server orders — and `compound` combines them; `SearchOperator.raw` carries the rest:

```scala
val matrix = SearchOperator.compound[Movie](
  must = List(SearchOperator.text(plotField, "hacker").orIn(titleField).fuzzy(1)),
  filter = List(SearchOperator.atLeast(yearField, 1999)),
)

collection.aggregate[Movie](
  Seq(
    Stage.search(matrix, SearchOptions.default.withCount(SearchCount.total)),
    Stage.project(Projection.empty[Movie].include(titleField).compute("score" := Expr.searchScore)),
  )
)
```

`SearchOperator` also has `phrase`, `autocomplete`, `wildcard`, `regex`, `equalTo`, `below` and `exists`.
`$searchMeta` takes the same operator and answers with counts instead of documents. `$vectorSearch` finds the nearest
documents to a vector, among `numCandidates` approximate ones or exactly with `exactVectorSearch`, optionally narrowed
by a `Filter` on the fields the index declared with `filteringOn`:

```scala
Stage.vectorSearch("plots", embeddingField, queryVector, limit = 10, numCandidates = 200, filter = Some(yearField.gte(2000)))
```

All of this runs against a local `mongodb/mongodb-atlas-local` container as well as against Atlas — Testcontainers
ships `MongoDBAtlasLocalContainer` for it, which is how this library's own tests exercise it. The fake has no search
index, so it refuses both the index calls and the stages by name.

### Creating a collection

MongoDB creates a collection on first write, so `createCollection` is for the cases where the shape has to be
declared up front — a capped log, a schema the server enforces, a time series, a clustered collection:

```scala
import mongo4s.operations.{ClusteredIndex, CreateCollectionOptions, TimeSeries}

database.createCollection("events", CreateCollectionOptions.capped(8 * 1024 * 1024).withMaxDocuments(10000))

database.createCollection(
  "readings",
  CreateCollectionOptions.default
    .withTimeSeries(TimeSeries.on("recordedAt").withMetaField("sensor").withGranularity(TimeSeriesGranularity.MINUTES))
    .expiringAfter(30.days),
)

database.createCollection("people", CreateCollectionOptions.default.withValidator(jsonSchema))
database.createCollection("orders", CreateCollectionOptions.default.withClusteredIndex(ClusteredIndex.onId))
```

`withCapped` takes the size because the server rejects a capped collection without one — there is no way to spell a
broken request here. Two combinations the server accepts and then quietly ignores are rejected instead: a
`maxDocuments` on a collection that is not capped, and a validation level or action with no validator to apply to.
Sub-second `expiringAfter` is refused for the same reason it is on an index — the server stores whole seconds, and
truncating to zero means "expire immediately".

`ClusteredIndex.onId` is the only clustered key offered because `_id` is the only one the server clusters on today.
Anything else this API does not model is still reachable through `database.runCommand`.

`renameCollection` moves a collection to a new name in the same database. It refuses to replace an existing
collection unless `dropTarget` says to, and the handle it was called on keeps pointing at the old name — now empty —
so open the collection again under the new one:

```scala
collection.renameCollection("people_archive")
collection.renameCollection("people_archive", dropTarget = true)
```

A move across databases is an admin command, so it goes through `runCommand` on the `admin` database.

### Change streams

`watch` exists at all three levels, matching the driver's own scope hierarchy — `MongoClient.watch` (the whole
deployment), `MongoDatabase.watch` (one database), and `MongoCollection.watch` (one collection). All of them need a
replica set or sharded cluster:

```scala
val events: S[ChangeEvent[User]] = collection.watch()
```

Every event is a `ChangeEvent[A]`, not a bare `BsonDocument`:

```scala
final case class ChangeEvent[A](
  operationType: OperationType, // com.mongodb's own enum — INSERT/UPDATE/DELETE/...
  namespace: Option[Namespace],   // the database and collection it happened in
  destination: Option[Namespace], // where a rename moved the collection to
  documentKey: Option[BsonDocument],
  fullDocument: Option[A],
  fullDocumentBeforeChange: Option[A],
  updateDescription: Option[UpdateDescription], // updated/removed field paths, for UPDATE events
  resumeToken: BsonDocument,
  clusterTime: Option[BsonTimestamp],
  wallTime: Option[BsonDateTime],  // when the change was applied, in server wall-clock time
  splitEvent: Option[SplitEvent],  // set only on a fragment of an event too large for one message
)
```

`MongoCollection.watch` decodes through the collection's own codec, so `fullDocument`/`fullDocumentBeforeChange`
come back as `Option[A]`. `MongoClient.watch`/`MongoDatabase.watch` span more than one document shape, so they
default to `ChangeEvent[BsonDocument]` — use `watchAs[A]` when you know the events all decode the same way.

At those two levels `namespace` is how you tell which collection an event came from — `Namespace(database,
collection)`, with no collection for an event about the whole database, such as its drop.

Everything else is `WatchOptions[A]`:

```scala
import mongo4s.changestream.WatchOptions

collection.watch(
  WatchOptions
    .resumeAfter[User](token) // shorthand for default[User].resumingAfter(token)
    .withFullDocument(FullDocument.DEFAULT)
    .withMaxAwaitTime(2.seconds)
    .withBatchSize(64)
)

WatchOptions.default[User].startingAfter(token) // the other two starting points, same builder
WatchOptions.default[User].startingAt(timestamp)
```

`fullDocument` defaults to `UPDATE_LOOKUP`, not the server's own default — MongoDB fills the document in only for
inserts and replaces, which leaves the most common question ("what does this document look like now?") unanswered on
updates. It is always `None` for deletes; there is no document left to look up. `fullDocumentBeforeChange` needs
pre-images enabled on the collection.

`resumeToken` is on every event so a consumer that dies mid-stream can restart from just after the last event it
actually handled, rather than from now:

```scala
collection.watch().evalTap(handle).evalTap(e => saveToken(e.resumeToken))
// later, on restart:
collection.watch(WatchOptions.resumeAfter[User](savedToken))
```

`resumingAfter` and `startingAfter` are alternatives — each clears the other, since the server rejects a stream
carrying both `resumeAfter` and `startAfter`.

`withExpandedEvents` is the driver's `showExpandedEvents`: DDL events — `createIndexes`, `drop`, `rename` and the
rest — are reported alongside the document ones. It needs MongoDB 6.0, so the flag is sent only when you ask for it
rather than as a default an older server would reject.

`clusterTime` and `wallTime` answer different questions: the first is the logical timestamp the change is ordered
by, the second is the server's wall clock when it was applied — useful for lag, useless for ordering. `splitEvent`
is set only on the fragments of an event too large for one message, and says which fragment of how many; without
`$changeStreamSplitLargeEvent` in the pipeline you will never see it.

`WatchOptions.pipeline` filters the change stream itself, and matches against the **change event's own shape**
(`{operationType, fullDocument, ns, ...}`), not the collection's document shape. A `Field.of` path is therefore the
wrong tool: it would render `"age"` where the event needs `"fullDocument.age"`. Use `Stage.raw`, or `Field.stored`
for a path under `fullDocument`:

```scala
val insertsOnly = WatchOptions
  .default[User]
  .withPipeline(Seq(Stage.raw(BsonDocument("$match", BsonDocument("operationType", BsonString("insert"))))))

collection.watch(insertsOnly)
```

A change stream never completes on its own — take from it, or interrupt it. A document that does not decode ends it
with an error; `watchAttempting` — `watchAsAttempting` at the client and database level — reports the failure and
carries on instead, which is what you want for a long-lived subscription. Its elements are
`ChangeEvent[DecodeResult[A]]`: the failure is on the document, and the event around it — resume token included —
still arrives, so a consumer can checkpoint past a document it could not read.

`fullDocument = UPDATE_LOOKUP` is a second read per update event, done by the server while it builds the event, and
the whole document on the wire. A consumer that only needs to know what changed can ask for
`FullDocument.DEFAULT` and read `updateDescription` instead.

## BSON codecs

Every codec ultimately produces a `mongo4s.bson.BsonDocumentCodec[A]` (entity ⇄ `org.bson.BsonDocument`) or, for the
*AST-free* path below, a `WireCodec[A]`. Bring whichever backend fits — `mongo4s` never registers a global
`CodecProvider`, so backends never collide inside one process.

| Module | Backend                                                                        | Notes |
| --- |--------------------------------------------------------------------------------| --- |
| `mongo4s-bson-medeia` | [medeia](https://github.com/medeia/medeia)                                     | `derives BsonDocumentCodec` |
| `mongo4s-bson-zio` | [zio-bson](https://github.com/zio/zio-bson)                                    | `zio.bson.BsonCodec` (add `zio-schema-bson` yourself to derive one from a `Schema`) |
| `mongo4s-bson-calypso` | [calypso](https://github.com/m2-oss/calypso)                                   | hand-written `forProductN` codecs |
| `mongo4s-bson-direct` | [mongo4s itself](https://github.com/mongo4s/mongo4s/tree/main/bson/direct/src) | `WireCodec[A]`, AST-free — see below |
| `mongo4s-bson-cats-data` | [cats-core](https://typelevel.org/cats/) | `BsonEncoder`/`BsonDecoder`/`WireCodec` for `NonEmptyList`/`Chain`/`NonEmptyVector`/`NonEmptySet`/`NonEmptyMap`, `WireCodec` for `Ior` |

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

Products, `Option`, `Either`, nested case classes, sealed traits/enums (via a `_type` discriminator field, written
first), and self-/mutually-recursive types all derive directly — recursive derivation is deferred behind a `lazy val`
internally so a type's own `given` never forces itself mid-construction.

**Nothing derives without being asked.** A type gets a `WireCodec` from `derives WireCodec`, from a `given` you
wrote, or from an existing `BsonEncoder`/`BsonDecoder` pair — never from merely being a case class. So a field whose
type you forgot to model is a compile error naming that type, not a codec invented for it, and an `opaque type` keeps
whatever representation you give it instead of borrowing the one underneath. The exception is the cases of an `enum`
or the leaves of a sealed trait: they cannot carry `derives` themselves, so the sum derives them — the same line
`circe` draws.

```scala
final case class Address(city: String) derives WireCodec
final case class Person(name: String, address: Address) derives WireCodec  // Address must have one

opaque type UserId = String
object UserId:
  given WireCodec[UserId] = ScalarWireCodec[String].imap(apply)(_.value)   // yours, not String's

enum Shape derives WireCodec:   // Circle and Square derive with it
  case Circle(radius: Double)
  case Square(side: Double)
```

`String`, `Int`, `Long`, `Double`, `Boolean`, `BigDecimal`, `Instant`, `UUID` and `ObjectId` are read and written
natively, in the same BSON representation the `BsonEncoder`/`BsonDecoder` path uses — `Decimal128` for `BigDecimal`,
`Date` for `Instant`, `ObjectId` for `ObjectId` — so the server indexes and range-compares them exactly as it would
on a collection opened with `getCollection`. Any *other* type with an existing `BsonEncoder`/`BsonDecoder` bridges
automatically, at the cost of one `BsonValue` per field instead of zero.

`List`/`Vector`/`Seq`/`Set`/`Array` write a real BSON array, and `Map[String, A]` a real `BSON` document keyed by its
own keys — `String` being the key type isn't a limitation of the general mechanism, it's just what BSON's own field
names are. Every *other* `Iterable` collection with a `scala.collection.Factory` (`Queue`, `ArraySeq`, `ListSet`,
`LazyList`, `SortedSet`/`TreeSet` given an `Ordering`, …) gets an array-shaped `WireCodec` too, generically — no
dedicated `given` needed per type.

`getDirectCollection` registers the derived codec with the driver via `CodecRegistries.fromCodecs(...)`, so
`insert/find/replace/update/delete/bulkWrite` decode straight to `A` — genuinely zero `BsonDocument` construction on the
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

The trade for that speed is strictness: derivation requires every modelled field to be present, unless its decoder
supplies a default — which `Option` does. So a **projection that drops a field the entity declares cannot be read
back** through the entity's own codec. That is what `selectAs` is for — see [Partial reads](#partial-reads) — and
`getCollection` with a `BsonDocumentCodec` of your own remains the escape hatch below that.

Strictness stops at the field list, though — **not** at BSON numeric widths. A document that stores `42` as an
Int32 where the model says `Long` (written by `mongosh`, by a `$inc`, or by another service) reads the same through
`getDirectCollection` as through `getCollection`: any whole number in range is accepted, and one that cannot be a
whole number of that type — `7.5` into a `Long`, a value past `2^63` — is refused on both. A `Double` target is the
exception on both paths: every BSON number converts to one, so a very large `Int64` or a long `Decimal128` rounds
rather than being refused, exactly as `BsonDecoder[Double]` has always done. The rule is written once, in
`BsonDecoder`, and the wire codec defers to it, so the two cannot drift apart.

What that costs: a field stored in the width the model declares is read straight off the wire, allocating nothing;
a field stored in some other numeric width costs one `BsonValue` for the conversion. Matching data pays nothing for
the leniency.

`aggregate` decodes through `BsonDocumentCodec` because its output shape isn't `A`; `aggregateDirect` is the
AST-free counterpart for an output that has a `WireCodec`. `distinct` still goes through `BsonDecoder`, which reads
one `BsonValue` per result rather than a document, so there is no tree to skip. Everything else —
`Filter`/`Update`/`Field` construction — is identical regardless of which codec backs the collection.

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

Field naming and discriminator naming (the `_type` value for sealed traits/enums) are independently configurable.
`WireCodecConfig` is built by starting from `Default` (or `SnakeCase`) and chaining `withFieldNaming`,
`withDiscriminatorNaming`, `withEncodeEmptyCasesAsString`, `withOmitNoneFields`, `withDecodeMissingAsDefault` — the same shape `WatchOptions` uses,
and the reason new derivation options can be added in a minor release without breaking binary compatibility:

```scala
given WireCodecConfig = WireCodecConfig.SnakeCase.withDiscriminatorNaming(FieldNaming.snakeCase)
```

`getDirectCollection` takes no `naming` of its own: the codec is what writes the field names, so it is what decides
how a query spells them. A derived `WireCodec` — a product, an enum or a sealed trait — reports the
`WireCodecConfig` it was built with, and so does one mapped from it with `imap` or `iemap`; a hand-written one says
so by overriding `fieldNaming`. `getCollection` still takes the parameter, because a `BsonDocumentCodec` from
`medeia`, `calypso` or `zio-bson` carries its own naming configuration that mongo4s cannot read; there the parameter
still has to match the codec.

A collection has one naming, so the entity and everything nested in it should be derived under the same
`WireCodecConfig`. A nested type derived under a different one writes its own spelling, but `Field.of(_.nested.field)`
renders every segment through the root's; reach such a field with `Field.stored` and its stored path.

`FieldNaming` applies per *derived* segment only. Stored names — `Field.stored`, a `PrimaryKey`'s field names, a
`$lookup`'s foreign field, map keys — are used verbatim. `FieldNaming.snakeCase` splits on every capital, so
`userID` becomes `user_i_d`; `FieldNaming.overrides(Map("userID" -> "userId"), fallback = snakeCase)` is the way out
of that.

`encodeEmptyCasesAsString = true` writes a parameterless case (`case object`/empty `case class`) as a bare BSON
string instead of a `{"_type": "..."}` document — only safe when that case is nested inside another document, not
when it's the root type of a collection (BSON's root value must always be a document).

#### Absent `Option` fields

A field holding `None` is **left out of the document entirely** rather than stored as an explicit `null` — the key
costs nothing on disk, and a collection of mostly-empty optional fields gets materially smaller. Reads are
unaffected either way: a missing field decodes to `None` through the same `defaultOnMissing` that has always
covered it, and a document already carrying an explicit `null` still decodes to `None`, so a collection written
before this keeps working unchanged.

```scala
final case class Contact(name: String, email: Option[String]) derives WireCodec

Contact("bob", None) // {"name": "bob"} — no "email" key at all
```

Two things do change with the field gone. `{field: null}` as a filter still matches, since MongoDB reads that
predicate as "null **or** missing", but `$exists: true` no longer matches a `None`, and a **sparse index** on that
field no longer includes those documents. Where either matters, `withOmitNoneFields(false)` restores the explicit
`null`:

```scala
given WireCodecConfig = WireCodecConfig.Default.withOmitNoneFields(false)
```

A nested `Option` does not survive a round-trip, with or without the flag: `Some(None)` is written as `null` and reads
back as `None`, because BSON has one null and both layers map onto it. This is true of the AST path
(`BsonEncoder`/`BsonDecoder`) as well. If you need to tell "absent" from "present but empty", model it as an `enum`
or a wrapper case class rather than `Option[Option[A]]`.

The flag governs *fields of a derived product* only. A `None` inside an array is still written as `null` regardless,
because dropping an element would shift every position after it. `Update.set(field, None)` also still writes `null` —
that is `BsonEncoder`, not `WireCodec`, and `$set: null` and `$unset` are different intents; use `field.unset` for
the latter. The AST bridges (`medeia`, `zio-bson`, `calypso`) follow their own library's rule here, not this flag.

#### Missing fields and case-class defaults

A missing field fails the decode unless it is an `Option`, even when the case class gives it a default. That is
deliberate: a default is the constructor's, and silently standing in for what the database holds is how a document
written without the field reads back as something nobody stored. `withDecodeMissingAsDefault(true)` is the opt-in for
the case it exists for — a field added to a case class that older documents do not have:

```scala
given WireCodecConfig = WireCodecConfig.Default.withDecodeMissingAsDefault(true)

final case class Account(name: String, status: Status = Status.Active) derives WireCodec
// {"name": "ann"} reads back as Account("ann", Status.Active)
```

A field that is there always wins, and a field with no default still fails when it is missing. The default is
evaluated per decode, as the constructor would — a `UUID.randomUUID()` default gives each document its own. One case
to know before turning it on: an `Option` field *with* a default takes that default when it is missing, not `None` —
`since: Option[Int] = Some(2020)` reads a document without `since` as `Some(2020)`. An explicit `null` is a value,
not a missing field, and still reads as `None`. Encoding does not change: a default is written like any other value.

#### Hand-writing a codec: `contramap`/`map`/`emap`/`imap`

A type that is not a case class, enum or sealed trait needs a codec written for it — `WireCodec[A]` is just
`WireEncoder[A] with WireDecoder[A]`, and both halves compose the same way `BsonEncoder`/`BsonDecoder` already do
elsewhere in mongo4s:

```scala
import mongo4s.bson.direct.WireCodec

enum Provider(val value: String):
  case Stripe extends Provider("stripe")
  case Adyen  extends Provider("adyen")

object Provider:
  def from(value: String): Option[Provider] = Provider.values.find(_.value == value)

  given WireCodec[Provider] =
    WireCodec[String].iemap(raw => from(raw).toRight(s"Unsupported provider: $raw"))(_.value)
```

`imap` (total in both directions) and `iemap` (decode can fail — reported as `BsonError.InvalidValue`) replace a
hand-rolled `WireCodec.instance(...)` for the common case of one type wrapping another: no `BsonWriter`/`BsonReader`
calls to write by hand, and the wrap/unwrap logic lives in exactly one place instead of being duplicated across the
encode and decode sides.

Both keep what the codec underneath knows about an absent field: a wrapper over an `Option` leaves the field out of
the document when it is empty, and reads the missing field back as that same empty value — through `iemap` too, as
long as the validation accepts it. They keep the codec's naming as well, so a derived entity wrapped in `iemap` to
validate what it reads still has its queries spelled the way it writes.

Resolution order matters when a type qualifies for more than one instance. An existing `BsonEncoder`/`BsonDecoder`
pair — hand-written, or coming from `mongo4s-bson-medeia`/`-zio`/`-calypso` — is bridged into a `WireCodec` and takes
precedence over automatic `Mirror` derivation, so a case class carrying those instances resolves without needing
`derives WireCodec`. Writing `derives WireCodec` on the type still wins over both: it puts a concrete instance in the
companion, which is more specific than either generic given.

#### `ScalarWireCodec` — reusing a `WireCodec` as a `BsonEncoder`

An opaque type over a primitive (`opaque type UserId = String`) typically needs a `WireCodec[UserId]` for
`getDirectCollection` *and* a `BsonEncoder[UserId]` for `Field.of[...].equalTo`/`PrimaryKey.single` — two
typeclasses, normally two independently hand-written codecs. `ScalarWireCodec[A]` — the type every primitive in
`WirePrimitiveInstances` actually has — closes that gap:

```scala
import mongo4s.bson.direct.ScalarWireCodec
import mongo4s.bson.BsonEncoder

opaque type UserId = String

object UserId:
  def apply(value: String): UserId = value
  extension (id: UserId) def value: String = id

  given ScalarWireCodec[UserId] = ScalarWireCodec[String].imap(UserId.apply)(_.value)
  given BsonEncoder[UserId]     = summon[ScalarWireCodec[UserId]].toBsonEncoder
```

`toBsonEncoder` builds one `BsonValue` per call — cheap on the query-construction path `BsonEncoder` actually runs
on, unlike materializing one for every field of every document, which is exactly what `WireCodec` exists to avoid
in the first place. `ScalarWireCodec` is deliberately narrower than `WireCodec`: a derived case class or sum type
(`derives WireCodec`) is genuinely document-shaped, so it's never typed as `ScalarWireCodec` — asking for one
(`ScalarWireCodec[SomeCaseClass]`) fails to compile instead of misbehaving at runtime.

#### `Either[A, B]`

`WireCodec[Either[A, B]]` derives given `WireCodec[A]` and `WireCodec[B]`, flat like a derived sealed trait
(`{"_type": "Circle", "radius": 2.0}`) rather than wrapped in a `Left`/`Right` envelope — the discriminator is each
branch's own runtime type name, and a document-shaped branch (a case class) has its fields inlined directly instead
of nested under a `"value"` key:

```scala
final case class ValidationError(message: String)
final case class Approved(reference: String)

// Right(Approved("abc")) -> {"_type": "Approved", "reference": "abc"}
// Left(ValidationError("bad input")) -> {"_type": "ValidationError", "message": "bad input"}
val result: Either[ValidationError, Approved] = ...
```

A scalar branch (no fields to inline, e.g. a bare `String`) keeps a `"value"` field, since a bare BSON scalar can't
also carry the discriminator in the same slot. The two branches must resolve to distinguishable type names —
`Either[Foo, Foo]`, or two differently-named types that happen to share a runtime class name once generics are
erased, throws when the codec is summoned rather than risking a silent wrong-branch decode later. So does a branch
with a field of its own named `_type`, which inlined next to the discriminator would overwrite it.

The discriminator is `ClassTag[A].runtimeClass.getSimpleName` — for `Int`/`Long`/`Double`/`Boolean`/etc, that's the *JVM
primitive's* name (`"int"`, not `"Integer"`), since `ClassTag[Int]`'s `runtimeClass` is the primitive class,
not the boxed one. Not a bug, just worth knowing if one branch is a bare numeric/boolean type.

### `bson-cats-data` — `cats.data` support

`NonEmptyList`/`Chain`/`NonEmptyVector`/`NonEmptySet`/`NonEmptyMap` get `BsonEncoder`/`BsonDecoder` and `WireCodec`
instances from `mongo4s-bson-cats-data`, delegating to the already-existing `List`/`Vector`/`Set`/`Map` instances
rather than reimplementing `BSON` encoding:

```scala
import mongo4s.bson.catsdata.CatsDataBsonInstances.given // or CatsDataWireInstances.given for bson-direct
import cats.data.NonEmptyList

final case class Team(members: NonEmptyList[String]) derives MedeiaDocumentCodec // or your codec backend's own derives
```

`NonEmptySet`/`NonEmptyMap` additionally need a `cats.Order` for their element/key type — the same `Order` you'd
already need to construct one of these types directly.

`Ior[A, B]` gets a `WireCodec` too, discriminated by each branch's own type name rather than
`"Left"`/`"Right"`/`"Both"`. Unlike `Either`, which inlines a case class branch's fields, `Ior`'s single-sided
branches always nest their payload under `"value"`. `Both` doesn't have a
single "own" type — it holds an `A` and a `B` at once — so its discriminator is the two names joined (`"String+Foo"`),
with each side nested under its own `"left"`/`"right"` key rather than inlined, to avoid a silent
field-name collision if `A` and `B` happen to share a field.

### Codecs and the driver's registry

The driver resolves codecs from a process- or client-wide `CodecRegistry`. `mongo4s` does not: a codec is resolved per
collection, from the `BsonDocumentCodec[A]` or `WireCodec[A]` in implicit scope at the `getCollection` /
`getDirectCollection` call. That is what lets medeia, zio-bson, calypso and `bson-direct` coexist in one process
without colliding.

So a `CodecRegistry` you set on `MongoClientSettings` is **not** where an entity codec belongs — mongo4s never asks it
for one, on either path:

* On the `getCollection` path, `mongo4s` asks the driver for `BsonDocument` and does its own encode/decode. So do
  `listIndexes`, `listCollections`, `runCommand`, `aggregate`, `distinct` and `watch` — all of them read
  `BsonDocument`/`BsonValue`. Your registry is never asked about `A`.
* On the `getDirectCollection` path, `mongo4s` registers the derived `WireCodec[A]` **ahead of** the client's registry,
  so a `Codec[A]` registered there does not silently shadow the codec the collection was opened with.

That does not make it inert, though. It is still load-bearing in two ways:

* **It has to keep providing the driver's own `BsonDocument` codec.** Since `mongo4s` reads and writes `BsonDocument`
  everywhere, a registry that *replaces* the defaults instead of extending them fails every single operation with
  `CodecConfigurationException: Can't find a codec for … BsonDocument`. Always build yours as
  `CodecRegistries.fromRegistries(yours, MongoClientSettings.getDefaultCodecRegistry)`.
* **It governs everything past `underlying`.** `MongoClient`/`MongoDatabase`/`MongoCollection` each expose the driver
  object they wrap, and there the driver's rules apply in full — `collection.underlying.withDocumentClass(classOf[Foo])`
  resolves `Foo` from your registry, `mongo4s` not involved.

So: set a registry for driver-level defaults, or for a type you handle through the escape hatch — not to route the
entities `mongo4s` already has codecs for.

## Repositories

`BaseMongoRepository[F, S, E, K]` implements `Repository` — `count/find/insert/upsert/update/delete/bulkWrite`, batched
by `batchSize` (default 500) — over any `MongoCollection[F, S, E]`, from either codec path.

Reads and writes that take a *condition* rather than a key come in two spellings, and the name says which: `…ByField`
takes a field and a value, `…ByFilter` takes a whole `Filter`. `findByField`/`findByFilter` return a `List`,
`getByField`/`getByFilter` the same rows as a stream, and `updateByField`/`updateByFilter` and
`deleteByField`/`deleteByFilter` write to every row that matches:

```scala
users.findByField(nameField, "alice")             // List[User]
users.getByField(nameField, "alice")              // the same, as a stream
users.updateByField(ageField, 30, birthday)       // every 30-year-old
users.deleteByField(ageField, 30)                 // likewise
```

A `…ByField` call is exactly its `…ByFilter` counterpart over `field.equalTo(value)` — there for the common case
that would otherwise spell out a one-field filter.

`bulkWrite` takes the same `BulkWriteOptions` the collection does, and `insertMany` the same `InsertManyOptions`. An
ordered write is sent in batches, which keeps its meaning — it stops at the first failure and what came before it
stays applied. An **unordered** one is sent as one command, because the point of `unordered` is that the server
applies everything it can, and batching would stop at the first batch that failed instead.

A failure in a later batch is reported as the whole write, the way the driver reports its own internal batches:
`BulkWriteFailed` names the failing command by its position in the sequence you passed, not within its batch, and
its `result` counts what the earlier batches wrote. `insertMany` and `upsertMany` batch the same way and report the
same way.

`findMany(keys)` is one `$in` per batch, so it answers the way a query does: in no particular order, without the
keys that matched nothing, and once per document however often its key was asked for. Key the result by
`pk.key` when you need it lined up with the input.

A `findOneAndUpdate` projection you pass wins over the repository's `defaultProjection`; the default applies when you
pass none.

```scala
BaseMongoRepository(collection)                                // over a collection you already have
BaseMongoRepository.create[F, S, E, K](db, "collection")       // reads return the document as stored, _id included
BaseMongoRepository.createDirect[F, S, E, K](db, "collection") // the same over getDirectCollection and a WireCodec
BaseMongoRepository.withoutId[F, S, E, K](db, "collection")    // strips _id from reads — for entities that do not model it
BaseMongoRepository.objectId[F, S, E](db, "collection")        // WithId[ObjectId, E], auto _id round-trip
```

`create` is the default, and it is the right one far more often than it looks: none of the four codec backends
rejects a document carrying an `_id` the entity does not model — `medeia`, `zio-bson`, `calypso` and a bridged
`WireCodec` all decode it and ignore the field. So `withoutId` is not needed to make such an entity readable.

Reach for it when a codec **you** wrote is strict about unknown fields, or when you would rather not carry `_id`
over the wire at all. Never reach for it for an entity keyed on `_id`: the projection strips the key, and it comes
back missing.

`insertOne` returns the entity's `K`, and `insertMany` the `K` of every entity, in insertion order and across
batches. A repository carries a `PrimaryKey[E, K]`, so it can name what it just inserted rather than hand back a
raw `BsonValue` for you to decode. `MongoCollection` has no key type and keeps returning the driver's
`InsertOneResult`/`InsertManyResult` — that is where the server's own `_id` still lives.

`insertIfAbsent` inserts an entity only when nothing holds its key yet, and answers whether it did. `upsert`
overwrites what is there; `insertOne` fails with `MongoError.DuplicateKey`, and inside a transaction that failure
ends the transaction. `insertIfAbsent` is an upsert that only writes on insert — every field of the entity under
`$setOnInsert`, matched by key — so a key that is already there leaves the stored entity untouched and the
transaction intact:

```scala
users.insertIfAbsent(User("1", "alice", 30)) // F[Boolean]: true when it inserted, false when the key was taken
```

`replace` swaps the entity stored under the same key, and never inserts. It takes an optional condition the stored
entity must still meet, which is what optimistic concurrency needs. Keep a version on the entity, replace only while the
version is the one you read, and treat a `matchedCount` of zero as "someone else wrote first":

```scala
users.replace(user.copy(name = "alicia", version = user.version + 1), versionField.equalTo(user.version))
// matchedCount == 0 → it changed (or went) under you: read it again and retry
```

An operator update under the same guard is `updateByFilter` with the key and the version in the filter. Adding an
array element only when no element with the same id is there yet is the same shape — the condition goes in the filter,
and a miss means it was already there:

```scala
orders.updateByFilter(orderId.equalTo("1") && !items.elemMatch(sku.equalTo("abc")), Update.push(items, item))
```

Paging goes through `Page`:

```scala
import mongo4s.repositories.Page

users.findByFilter(adults, Page.sortedBy(Sort.asc(nameField)).skipping(20).taking(10))
users.getByFilter(adults, Page.first(100)) // same, as a stream
```

Skip-based paging re-scans what it skips, so it degrades on deep pages — and a row deleted earlier in the
collection shifts everything after it, so a walk can miss rows it never saw. `findPage` avoids both by asking for
what comes *after* a key rather than for an offset:

```scala
val firstPage = users.findPage(100)                    // KeysetPage(items, next)
val nextPage  = users.findPage(100, after = firstPage.next)

users.findPage(100, after = Some(lastKey), filter = adults) // narrowed, still paged by key
users.findPage(20, descending = true)                       // newest key first
```

A page is a `KeysetPage[E, K]`: its `items`, and `next` — the key to pass as `after` for the page that follows, or
`None` when there is none. The repository asks for one row more than the page holds to know which, so the last page
is reported as last rather than discovered by fetching an empty one.

The repository already knows the key's fields, so it builds both halves itself: the sort that gives the page an
order, and the comparison that steps past the cursor. For a compound key that comparison is lexicographic — an
`$or` over "first field greater, or first equal and second greater, or …" — which is the part that is easy to get
subtly wrong by hand. `ensureKeyIndex` builds the index that makes the whole walk a range scan.

The cursor is the key of the last row you handled, so a crash resumes from there rather than from an offset that no
longer means the same thing. The walk is only as sound as the key: it has to be unique, it has to compare the way
the server orders it — keep one BSON type per key field, since a mixed-type field sorts by type before value — and it
should be indexed, which is what `ensureKeyIndex` is for.

`ensureKeyIndex` builds the unique index the `PrimaryKey` describes. Nothing builds it for you: indexes are often
owned by migration scripts rather than by the application, so the repository leaves the decision where it belongs.
A key on `_id` needs nothing, since the server indexes `_id` uniquely on its own; any other key without that index
is not unique — two inserts with the same key both land, and so do two `insertIfAbsent` calls that race.
`WithId[Id, E]` wraps an entity with a
separately-typed id (`type Oid[E] = WithId[ObjectId, E]`) and ships its own `PrimaryKey`/`BsonDocumentCodec`
instances, for entities that don't carry their own id field — it lives in `mongo4s-core`, so it's available whether
or not you use the repository layer.

Like `MongoCollection`, every `Repository` method takes `(using session: Option[ClientSession] = None)` — a
repository call can join a [transaction](#sessions--transactions) the same way a raw `collection` call can.

`BaseMongoRepository` is `open`: adding a domain query means declaring it against `collection`/`Filter`/`Field`
directly, not reimplementing what's already there.

### Counters

MongoDB has no sequences. The usual stand-in is a collection of counters, one document per name, moved by a single
`findOneAndUpdate` that increments and upserts — one round trip, atomic on the server, and the first call creates the
counter. `MongoCounters` is that, given a database and a collection name:

```scala
import mongo4s.repositories.MongoCounters

for
  counters <- MongoCounters.create[IO, S](database)            // the "counters" collection, or name your own
  number   <- counters.next("orders")                           // 1, then 2, 3, …
  block    <- counters.next("ids", step = 100L)                 // reserve a block of 100: this is its last value
  current  <- counters.current("orders")                        // Option[Long], without moving it
  _        <- counters.reset("orders", 1000L)                   // the next one is 1001
yield number
```

Each counter is a document `{_id: name, value: n}`, so the key is the counter's name and the `_id` index is all it
needs. `next` hands every value out exactly once however many callers race for it, and like everything else it takes
a session, so a number can be drawn inside a transaction. `Counters[F]` is the trait, for a double of your own;
`FakeCounters` in the testkit is the real implementation over a `FakeMongoCollection`.

For unit tests, `FakeMongoCollection` (in `mongo4s-testkit`, a published module — add it as
`"org.mongo4s" %% "mongo4s-testkit" % "5.0.0" % Test`) implements `MongoCollection` in memory. The exact same
`Filter`/`Update`/`Field` AST the real driver interprets is interpreted against an in-memory buffer instead, so
repository logic is testable without a running MongoDB.

Filters, updates, sorting, paging and projections are simulated, and so is a subset of `aggregate`:
`$match`, `$sort`, `$skip`, `$limit`, `$project`, `$unset`, `$count`, `$sortByCount`, `$fill` and `$group` with
`$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$firstN`, `$lastN`, `$maxN`, `$minN`, `$top`, `$bottom`,
`$topN`, `$bottomN`, `$stdDevPop`, `$stdDevSamp` and `$mergeObjects`. That covers the pipelines most service code writes, so aggregating no longer forces a
test to reach for Docker.

`distinct` is simulated too, array fields flattened into their elements the way the server does, and so is `$slice`
in a projection. Filters follow the server's array semantics — a filter on an array field matches its elements, and a
dotted path descends through an array of documents — `$regex` searches rather than anchors and honours its options,
`$eq null` matches a missing field, `$all: []` matches nothing, `$elemMatch` and `pullWhere` take a condition on a scalar
element as well as on a document one, `$inc` keeps the width the server keeps, and an insert stamps an `_id` and
refuses a duplicate one — through `insertOne`, `insertMany` (ordered or not), a `bulkWrite` insert, and an upsert that
inserts, so `wasUpserted` and `upsertedIds` say what the server would say. A `sort` on `updateOne`/`replaceOne` picks
the document the server would pick.

Numbers compare the way the server compares them: an `Int64` of 5 equals an `Int32` of 5 and a `Double` of 5.0,
`Long`s past `Double`'s precision sort exactly, and `NaN` sorts below every number. A duplicate in `insertMany` or a
`bulkWrite` is a `BulkWriteFailed` naming the command, after the writes before it — and, unordered, the writes after
it too. A unique index is enforced, sparse and partial ones included, and is named as the server names it
(`email_1`), so `dropIndex` finds it. `listIndexes` describes each index as the server does — `v`, `key`, `name` and
its options, after the `_id_` index — creating one that already exists changes nothing, one that clashes with an
existing index's name or keys fails with the server's code, `createIndexes` builds all or none, and `dropIndex`
refuses an index that is not there, and `_id_`. A `unique` index on `_id` alone is refused with the server's code too, and
so is a partial index whose filter holds an operator the server does not accept there. A replacement keeps the `_id` of the document it replaces, an update that
changes nothing counts as matched but not modified, and a `findOneAnd*` whose projection leaves a document the codec
cannot read fails, as the real collection does, rather than answering `None`.

The fake streams through the same `RsBridge` the real collection does, and reads the collection when the stream
runs, not when it is built — a stream set up before an insert sees the insert. `aggregate`, `distinct` and
`selectAs` stream as well as `find`.

One fake can be shared between fibers. Each operation runs start to finish under one lock, so concurrent inserts
all land, a duplicate `_id` or unique key lets exactly one of them through, `$inc`s from many fibers add up, and
`findOneAndUpdate` hands each document to one caller — what a test of a counter or a work queue relies on. That is
stricter than the server, which promises atomicity per document: every answer the fake gives is one MongoDB could
give, but a race across documents that the server could expose will not show here.

Every update operator is simulated — `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`, `$currentDate` (a date or a
timestamp), `$push` with its `$each`/`$position`/`$slice`/`$sort` modifiers, `$pull` by value or by condition, `$pullAll`,
`$pop`, `$addToSet` and `$bit` — and `$setOnInsert` correctly does nothing on an update that matches. The positional
operators are not: `each`, `matched` and `where` paths are refused by name, rather than written to a field called
`$[]`.

Everything else throws `UnsupportedOperationException` naming what was asked for, rather than quietly answering
wrong — a fake that lies is worse than no fake. That includes `watch`, `explain`, `renameCollection` — the fake
holds a single namespace, with nowhere to move it — a write for the client-level `bulkWrite`, for the same reason,
`$text` and ranking by `$meta textScore`, every [aggregation expression](#aggregation-expressions) and so `$expr`,
Atlas Search's indexes and stages, a tailable cursor, `$jsonSchema`, the geospatial operators, `Filter.Raw`, `Stage.Raw`, `Update.Raw`, a pipeline update, a
collation anywhere it is accepted, an update carrying `arrayFilters`, a unique index over an array field, every
aggregation stage outside the list above, `$median` and `$percentile`, whose `approximate` method only the server
knows, and the `$addToSet` *accumulator*, which is refused on purpose: MongoDB leaves the order of its result
undefined, so no fake can be faithful to it.

Upserts insert on a miss the way the server does, whether they replace or update — `updateOne`, `updateMany`,
`findOneAndUpdate`, `findOneAndReplace`, `replaceOne` and their `bulkWrite` commands. An update builds the new
document from the filter's equality conditions — `$eq`, a one-value `$in` or `$or`, every value of an `$all`, through
`$and` — then applies its operators, `$setOnInsert` included, and stamps an `_id` first if none came from the filter.
Two equalities on one path are refused with the server's code 54, `$all` with several values among them. A
replacement keeps only the `_id` from the filter.

One caveat carries over from `find`: where MongoDB does not define an order, neither does the fake, and the two need
not agree. `$group` emits its buckets in the order their keys were first seen, and `distinct` the order it first met
each value. End the pipeline with `$sort`, or sort the values yourself, if the order matters — as you would have to
against a real server anyway.

`FakeRepository` is the same idea one layer up: a real `BaseMongoRepository` over a `FakeMongoCollection`, so the
repository logic under test is the one that ships. `repository.fake` reaches the collection underneath, for seeding
and for `snapshot`. Both take the runtime's `Effect` and `RsBridge` from scope — the same givens a real client uses —
so `FakeRepository[IO, S, User, String]()` and `FakeMongoCollection[IO, S, User](codec)` are all there is to it.

## Runtime backends

Each runtime module provides `given Effect[F]` (sequencing, failure, and a finalizer that sees how the action ended)
and `given RsBridge[F, S]` (Reactive-Streams `Publisher` → `F`/`S`):

| Module | Effect | Stream | Notes |
| --- | --- | --- | --- |
| `mongo4s-cats` | any `F` with `cats.effect.kernel.Async` | `fs2.Stream` | via `fs2.interop.reactivestreams` |
| `mongo4s-zio` | `zio.Task` | `zio.stream.ZStream` | via `zio-interop-reactivestreams` |
| `mongo4s-kyo` | `A < (Async & Abort[Throwable])` | `kyo.Stream` | via `kyo-reactive-streams` |
| `mongo4s-rapid` | `rapid.Task` | `rapid.Stream` | |

Beyond the members a runtime has to supply, `Effect` carries defaults it may override with its own: `monotonic` and
`sleep` — the pause between transaction retries — read the runtime's clock and scheduler on all four backends
instead of `System.nanoTime` and a blocked thread.

`MongoClient.fromClient`/`fromSettings`/`fromConnectionString` return a bare `F[MongoClient[F, S]]` — you own calling
`.close`. Every runtime module also ships `MongoClientResource` with the same three constructor names, wrapped in
that runtime's own resource-safety idiom rather than one type copy-pasted across all four:

```scala
// cats — cats.effect.Resource
import mongo4s.cats.MongoClientResource

MongoClientResource.fromConnectionString[IO]("mongodb://localhost:27017").use { client =>
  // logic
}
```

```scala
// zio — ZIO.acquireRelease, released when the enclosing ZIO.scoped block exits
import mongo4s.zio.MongoClientResource

ZIO.scoped:
  for
    client <- MongoClientResource.fromConnectionString("mongodb://localhost:27017")
    ...
  yield ()
```

```scala
// kyo — Scope.acquireRelease, released by Scope.run
import mongo4s.kyo.MongoClientResource

Scope.run:
  for
    client <- MongoClientResource.fromConnectionString("mongodb://localhost:27017")
    ...
  yield ()
```

```scala
// rapid has no Resource/Scope type — Task.guarantee is its only finalizer primitive, attached to an
// already-known computation — so this is bracket-shaped (a `use` callback) instead of a composable value
import mongo4s.rapid.MongoClientResource

MongoClientResource.fromConnectionString("mongodb://localhost:27017") { client =>
  // logic
}
```

### Bridge configuration

`RsBridgeConfig` controls how a driver `Publisher` becomes your `F` and `S`. A `given` of your own overrides the
default wherever an `RsBridge` is summoned:

```scala
import mongo4s.RsBridgeConfig

given RsBridgeConfig = RsBridgeConfig.default
  .withBufferSize(512)        // outstanding demand for streaming reads
  .withTimeout(5.seconds)     // per non-streaming operation; unset by default
  .withStrictSingleResult     // fail on a second result instead of taking the first
```

`bufferSize` bounds memory against a fast cursor. On `cats` and `kyo` it does not apply to `all`, which asks for
everything by definition; `zio` and `rapid` read `all` through the same buffered source as a stream, so it applies
there. `timeout` is a backstop for a cursor that stops signalling entirely — the driver has its own timeouts,
and streams are deliberately excluded, since a change stream sitting idle is working rather than stuck.
`strictSingleResult` is off by default, matching the driver.

Operations that expect at most one document read two, not the whole cursor — enough to notice a second result under
`strictSingleResult`, and no more. `find(...).first` and `AggregateQuery.first` also push a `limit`/`$limit` to the
server, so on those the server sends one document and `strictSingleResult` has nothing to catch; it is `distinct` and
the other single-result publishers that it guards.

## Modules

Published for `Scala 3.9 LTS` under `org.mongo4s`:

```scala
"org.mongo4s" %% "mongo4s-<module>" % "5.0.0"
```

| | Module | Notes |
| --- | --- | --- |
| core | `mongo4s-core` | `MongoClient`/`MongoDatabase`/`MongoCollection`, `Field`/`Filter`/`Update`, `PrimaryKey`, `WithId`; depends on `bson-core` + `bson-direct` only |
| bson | `mongo4s-bson-core` | `BsonEncoder`/`BsonDecoder`/`BsonDocumentCodec` — the scalar + document codec seam |
| | `mongo4s-bson-direct` | `WireCodec[A]` — AST-free product/sum derivation, no third-party dependency |
| | `mongo4s-bson-cats-data` | `cats.data` (`NonEmptyList`/`Chain`/`NonEmptyVector`/`NonEmptySet`/`NonEmptyMap`/`Ior`) instances |
| | `mongo4s-bson-medeia` | bridges `medeia`'s `derives BsonDocumentCodec` |
| | `mongo4s-bson-zio` | bridges `zio-bson` (add `zio-schema-bson` yourself for the `Schema`-derived route) |
| | `mongo4s-bson-calypso` | bridges `calypso`'s `forProductN` |
| runtime | `mongo4s-cats` | `cats-effect 3` + `fs2` |
| | `mongo4s-zio` | ZIO 2 + `zio-streams` |
| | `mongo4s-kyo` | kyo 1.0.0-RC7 |
| | `mongo4s-rapid` | rapid |
| repositories | `mongo4s-repositories` | `BaseMongoRepository`, `Repository`, `Page` |
| testkit | `mongo4s-testkit` | `FakeMongoCollection`, `FakeRepository`, `FakeCounters` — in-memory doubles for unit tests |

Every module is built with the same Scala version, so there is no longer a subset that a project on an older
compiler has to do without.

**Java.** The published class files target Java 17, so every module runs on JDK 17 or newer — except `mongo4s-kyo`,
whose kyo dependency ships Java 25 class files and so needs JDK 25 at runtime as well as to build. Building the
project itself needs JDK 25 throughout.

**MongoDB.** The integration suite runs against MongoDB 8.2, and Atlas Search against the `mongodb-atlas-local` 8.2
image. A few operations need a newer server than the driver's minimum: `$median`/`$percentile` and the ranking
accumulators need 7.0, and the client-level `bulkWrite` and a `sort` on `updateOne`/`replaceOne` need 8.0.

### Compatibility

Binary compatibility within a major version is checked by [MiMa](https://github.com/lightbend/mima) on every build,
new `Effect`/`RsBridge` methods carry default implementations, and deprecations get at least one minor release before
removal. `mongo4s-kyo` sits outside the promise while kyo is on a release candidate.

`5.0.0` is a breaking release, almost entirely in binary shape rather than in what a call says: most existing calls
compile unchanged against it. `insertOne`/`insertMany` take options, `bulkWrite` takes `BulkWriteOptions` instead of
an `ordered` flag, `$addFields` takes computed fields rather than raw `BSON` pairs, and several AST cases changed
shape — `Group`'s key, `Filter.Expr`, `Filter.Type`, `Update.CurrentDate`, `Projection.Include`,
`Stage.Unwind`/`UnionWith`/`Redact`, and the bulk delete commands. `FindQuery`, `AggregateQuery`, `DistinctQuery`,
`MongoCollection` and `MongoClient` gain members an implementation of your own has to provide. Two behaviours change
without a compile error: `containsAll(Nil)` matches nothing, as `$all: []` does on the server, and a `sort` on
`updateMany` is refused rather than dropped. Each has a migration note in [COMPATIBILITY.md](COMPATIBILITY.md).

`4.0.0` changes shapes MiMa would flag, and behaviour a recompile alone would not reveal: an `Update` that changes one
path twice is refused rather than keeping only the last change, `findPage` returns a `KeysetPage` with its next
cursor, `watchAttempting` keeps the event whose document failed, `distinct` over an array answers with its elements,
the operators that only apply to one kind of field stop compiling on the others, and the testkit's fake streams
through the `RsBridge` instead of taking an `emit` function. `MongoCollection` gains members an implementation of your
own has to provide, `Stage` gains cases an exhaustive match has to cover, and the fake's `listIndexes` answers the
way the server does. Each has a migration note in [COMPATIBILITY.md](COMPATIBILITY.md).

`3.0.0` moved the whole build onto `Scala 3.9 LTS` and dropped `3.3 LTS`. `TASTy` is not forward compatible, and that
is a break MiMa cannot see, which is why it took a major release rather than a minor. What it buys is one Scala
version across every module: the three that used to be pinned to a fast-release `3.8` are on the LTS line with the
rest. It carried API changes of its own — compound `PrimaryKey`s became named tuples, `WireCodec` no longer
derives itself unasked, and driver exceptions arrive as `MongoError` — each with a migration note in
[COMPATIBILITY.md](COMPATIBILITY.md).

Full policy, the migration guides and the Scala-version rules: **[COMPATIBILITY.md](COMPATIBILITY.md)**.
What changed in each release: **[CHANGELOG.md](CHANGELOG.md)**. What is not covered yet and how it will land:
**[ROADMAP.md](ROADMAP.md)**.

## Benchmarks

Six JMH harnesses in [`benchmarks/`](benchmarks), against `mongo4cats` `0.8.0` as a reference point. The short
version: `bson-direct`'s `WireCodec` writes and reads real BSON bytes **2.4–3.2×** faster than either `mongo4cats`
codec and allocates up to **6.4×** less; against a real `MongoDB` the single-document operations land level for all
six stacks, while bulk reads pull ahead — `mongo4cats` allocates **2.1–6.0×** more on `findAll`/`findStream`, reads
`findAll` **1.13–1.35×** slower, and its `find(...).stream` runs at **1,626 ops/s** against `mongo4s`'s **3,174** on
the same effect system (`mongo4cats` `0.7.18` ran it at 392; `0.8.0` closed most of that gap, not all).

The server-backed rates were measured against `MongoDB` in Docker on macOS, where a bare `ping` inside the container
costs 0.135 ms and the round trip is still most of a single-document call. They compare stacks honestly; read them as
ratios rather than as throughput a server can deliver.

Every table, the methodology and the commands to reproduce them: **[BENCHMARKS.md](BENCHMARKS.md)**.

## Design notes

Four decisions the rest of the library follows from: **Scala 3 only**, so field selectors can be macros and codecs
can be `derives`; **no global state**, so codecs are per-collection and four backends coexist in one process;
**`Effect[F]`'s `guaranteeCase` sees cancellation**, so an interrupted transaction rolls back instead of lingering;
and **`Filter`/`Update` are a real AST**, not the driver's opaque builders, so the same query runs against MongoDB
or against an in-memory buffer.

Each of those, the failure that motivated it, the sharp edges accepted along the way, and what was rejected:
**[DESIGN.md](DESIGN.md)**.

## Adopters

<a href="https://betby.com">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="logos/betby.svg"/>
    <img src="logos/betby-dark.svg" alt="Betby" height="56"/>
  </picture>
</a>

Using mongo4s? Open a PR to add your logo.

## Contributing

Bug reports and PRs are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build the project,
run its tests, and where to make changes for common kinds of contributions. This project follows the
[Contributor Covenant](CODE_OF_CONDUCT.md); please report security issues per [SECURITY.md](SECURITY.md)
rather than in a public issue.

## License

Apache 2.0 — see [LICENSE](LICENSE).
