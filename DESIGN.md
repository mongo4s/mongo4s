# Design notes

[← back to the README](README.md)

Why `mongo4s` is shaped the way it is. The README says what the API does; this says what was chosen, what was
rejected, and what it cost. Several of these decisions look arbitrary until you know the failure they were a
response to, so the failures are here too.

* [Principles](#principles)
* [The shape: mirror the driver](#the-shape-mirror-the-driver)
* [Effect-agnostic by construction](#effect-agnostic-by-construction)
* [Codecs are per-collection, never global](#codecs-are-per-collection-never-global)
* [The AST-free path](#the-ast-free-path)
* [The query AST](#the-query-ast)
* [Reads: strict by default, lenient on request](#reads-strict-by-default-lenient-on-request)
* [Streaming](#streaming)
* [Transactions](#transactions)
* [Change streams](#change-streams)
* [Repositories, and one AST with two interpreters](#repositories-and-one-ast-with-two-interpreters)
* [Evolving the API](#evolving-the-api)
* [Deliberately not done](#deliberately-not-done)

## Principles

1. **Scala 3 only.** `given`-based throughout, derivation via `Mirror` and `inline`, no runtime reflection. Not
   cross-building to 2.13 is what buys macro field selectors, `derives`, opaque types and typed contextual
   parameters — the whole reason the API can be type-safe where a cross-built one cannot.
2. **The compiler is the test.** A wrong field name, a missing codec, a value of the wrong type for a field: all of
   these should be compile errors, not runtime exceptions or silently empty result sets.
3. **No global state.** No process-wide codec registry, no implicit ambient effect. Everything is resolved at the
   call site and scoped to the collection it belongs to.
4. **The driver is not hidden.** `underlying` is public on client, database and collection. Anything `mongo4s` does
   not model is still reachable, and reaching for it is not a defeat.

## The shape: mirror the driver

`MongoClient[F, S]` → `MongoDatabase[F, S]` → `MongoCollection[F, S, A]` follow the official driver's own hierarchy
one-to-one. Someone who knows `mongodb-driver-reactivestreams` can guess where things are; someone who does not can
read the driver's documentation and have it apply.

Two type parameters, not one: `F[*]` is the effect and `S[*]` is the stream. They are separate because no effect
system bundles them — `cats-effect` pairs with `fs2`, ZIO ships its own `ZStream`, and the pairing is a user
decision, not ours.

Every method that reaches the server takes `(using session: Option[ClientSession] = None)`. Sessions are therefore
invisible until you opt in, and *joining* a transaction requires no change at the call site — the session is given
implicitly by `withTransaction` and picked up by every call inside the block, including repository calls that know
nothing about transactions. The alternative — a second overload of every method taking an explicit session, which
is what `mongo4cats` does — doubles the API surface and makes a transactional call textually different from a
non-transactional one.

## Effect-agnostic by construction

`mongo4s-core` depends on the Mongo driver and nothing else. Not `cats-effect`, not `fs2`. A runtime module is two
`given` instances:

```scala
trait Effect[F[*]]:
  def pure[A](a: A): F[A]
  def delay[A](a: => A): F[A]
  def map[A, B](fa: F[A])(f: A => B): F[B]
  def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]
  def raiseError[A](error: Throwable): F[A]
  def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]): F[A]
  def guaranteeCase[A](fa: F[A])(finalizer: ExitCase => F[Unit]): F[A]
  // suspend, unit, void, attempt, guarantee, onError, bracket, bracketCase, monotonic — derived, all overridable
```

Seven abstract members. That number is a deliberate ceiling: everything derivable is derived with a default so that
supporting a new effect system is an afternoon, not a project, and so that adding capability later does not break
implementors.

**`guaranteeCase` is the load-bearing one.** It is the only member that could not be dropped without losing
something real. It sees `ExitCase.Succeeded | Errored | Canceled`, and the `Canceled` case is why an interrupted
transaction rolls back immediately instead of lingering on the server until it is reaped, and why a canceled read
releases its cursor. An effect abstraction built on `Monad` + `MonadError` alone cannot express that, which is the
reason `Effect` exists at all rather than the library taking a `cats.effect.Async` constraint.

Error handling in the finalizer is defined rather than incidental: a finalizer that itself fails does not replace
the error that caused it. It is attached with `addSuppressed`, so the original failure survives — the rollback
error is what you want to *see*, but the error that triggered the rollback is what you need to *debug*.

`RsBridge[F, S]` turns a Reactive-Streams `Publisher` into `F` or `S`:

```scala
trait RsBridge[F[*], S[*]]:
  def one[A](publisher: => Publisher[A]): F[A]
  def option[A](publisher: => Publisher[A]): F[Option[A]]
  def list[A](publisher: => Publisher[A]): F[List[A]]
  def unit[A](publisher: => Publisher[A]): F[Unit]
  def stream[A](publisher: => Publisher[A])(using Streamable[S, A]): S[A]
  def liveStream[A](publisher: => Publisher[A])(using Streamable[S, A]): S[A] = stream(publisher)
```

**Every `Publisher` is taken by name.** This is not a style choice. The driver opens a cursor when a publisher is
subscribed to, and building an `F[A]` value is not running it — a by-value parameter would open a cursor for a
computation that may never run, or run twice for one that is retried. By-name means the subscription happens when
the effect does.

`Streamable[S, A]` is a marker typeclass with no methods. It exists so that every streaming call names its element
type at the call site, which is what lets the kyo backend supply the per-element `Tag` its stream type requires.
Without it, kyo could not be supported at all without changing every signature.

## Codecs are per-collection, never global

The driver's model is a process-wide `CodecRegistry`: register `Codec[A]` once, and every collection typed `A` uses
it. `mongo4s` does not use it for entity codecs at all. A codec is resolved as a `given` at the call site that
creates the collection, and is attached to that collection alone.

The cost is one more implicit parameter. What it buys: `medeia`, `zio-bson`, `calypso` and `bson-direct` can all be
in use in the same process, in the same service, on different collections — and a missing codec is a compile error
naming the type, rather than a `CodecConfigurationException` under load.

The driver's registry still matters twice, and both are load-bearing:

* It must keep supplying the driver's own `BsonDocument` codec. A registry that *replaces* the defaults instead of
  extending them fails every operation, because `mongo4s` reads and writes `BsonDocument` underneath. Pinned by
  `CodecRegistryItSpec`.
* It governs everything past `underlying`. The escape hatch is the user's, and their registry is what applies there.

On the direct path the derived `WireCodec[A]` is registered **ahead of** the client's registry:

```scala
CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(derived), underlying.getCodecRegistry)
```

The other order was tried and was wrong: a `Codec[A]` in the user's registry silently shadowed the derived codec, so
the collection wrote a shape nobody had asked for and no error was raised. Also pinned by a spec.

## The AST-free path

Every other Scala Mongo library encodes through an intermediate tree: case class → some JSON or BSON AST →
`org.bson.BsonDocument` → wire bytes. `bson-direct` derives a `WireCodec[A]` that writes into the driver's
`BsonWriter` directly and reads from its `BsonReader`, with no intermediate representation at all. It is handed to
the driver through a two-method adapter, and the driver calls it with its own `BsonBinaryWriter`.

That is where the performance comes from — see [BENCHMARKS.md](BENCHMARKS.md) — but the reason it is the *default*
codec is dependency footprint: it lives in `mongo4s` itself, so the default path pulls in no third-party codec
library.

**Which scalars are native is a correctness question, not only a speed one.** A type with no `ScalarWireCodec` falls
back to the `BsonEncoder`/`BsonDecoder` bridge, which builds exactly the one `BsonValue` per field this path exists
to avoid — and `Instant`, `ObjectId`, `UUID` and `BigDecimal` sit in almost every real entity, so the fallback was
the common case rather than the exception. Each native instance writes and accepts exactly what the `BsonValue` path
writes and accepts, leniency included: `Instant` also reads a `Timestamp`, `BigDecimal` also reads
`Int32`/`Int64`/`Double`. That symmetry is what lets the same collection be opened either way.

It also makes losing one invisible to behavioural tests: the fallback produces byte-identical documents, so dropping
a native instance would cost allocations and nothing else — every round-trip and format test would still pass. The
spec therefore asserts the *structure*, that the summoned `WireCodec` really is a `ScalarWireCodec`, because nothing
observable distinguishes the two.

**It is strict by construction, and that is a trade, not an oversight.** Derivation requires every modelled field to
be present unless its decoder supplies a default (`Option` does). So a projection that drops a modelled field cannot
be read back through the entity's own codec. Strictness catches the far more common bug, which is a document that
quietly lost a field, so the codec keeps it — and `selectAs` answers the partial read instead.

**`selectAs` names the shape rather than deriving it from the query.** `find(...).selectAs[(name: String, age: Int)]`
builds both the projection and the decoder from one named tuple, so they cannot drift apart. Reading the shape off
the selectors instead — `select(_.name, _.age)`, with the type inferred — was tried first and does not work:
`NamedTuple[N, V]` is opaque with `V` as its lower bound, so a `transparent inline` result widens to the bare tuple
and the labels are gone by the time the caller sees it. The bare tuple still *conforms* to the named type through
that same lower bound, which makes the failure look like success until a field is read by name. Naming the shape in
the signature keeps the type where the compiler cannot approximate it away.

`aggregate` and `distinct` take their output type from the call site rather than from the collection, so neither
could use the collection's own `WireCodec`. For `aggregate` that mattered: a pipeline reading a large collection is
exactly the hot path `bson-direct` exists for, and stopping at a `BsonDocument` there gave the AST-free claim an
asterisk. `aggregateDirect[B]` takes a `WireCodec[B]` instead and registers it the same way `getDirectCollection`
registers the entity's, so the output is read straight off the wire. It carries the strictness with it — the same
numeric-width rules apply to the pipeline's output as to a stored entity.

The size of that asterisk is now measured rather than assumed: over ten thousand documents `aggregateDirect` is 1.6×
the bridged path against a real server, and over ten it is a wash, because the round trip is the whole cost at that
scale. Both halves are in [BENCHMARKS.md](BENCHMARKS.md); the second half is the reason `aggregate` was never
deprecated in favour of it.

`distinct` is left alone deliberately: it reads one `BsonValue` per result rather than a document, so there is no
intermediate tree to skip and nothing to win.

## `$slice` and `$meta` are shapes the AST was missing

`Sort` used to be a list of `(FieldPath, Boolean)`, which can say ascending or descending and nothing else. `$text`
had been supported as a filter since 2.0 while the thing you almost always do with it — rank by relevance — had no
representation at all, so the feature was half a feature. `SortOrder` replaces the boolean; `TextScore` renders
`{"$meta": "textScore"}` and takes an output field name rather than a model field, because that name is invented by
the query and must not go through `FieldNaming`.

`$slice` is the other shape that did not fit: it is neither an inclusion nor an exclusion, which is exactly why the
two-type split could not express it. Rather than add a fourth projection type, all three carry a list of slices, so
`slice` chains onto any of them without committing the projection either way — which matches what the server
accepts. The fake simulates it, because taking the first or last N of an array has one right answer; the parity spec
checks that answer against a real server for all three forms.

Sorting by text score is the opposite case and the fake refuses it: a score only exists where a text index does.
That check runs when the sort is applied rather than inside the comparator, because a comparator is never called for
a single-document result and the refusal would have depended on how much data a test happened to insert.

## `explain` returns a document, not a type

The point of a `Filter` AST is that the query is a value you can build and pass around; the question that follows is
whether the server can serve it from an index, and until now nothing in `mongo4s` could answer it. `explain` runs
the publisher the builder had already configured, so what is explained is the query that would have run, hint and
collation included — not a reconstruction of it.

Its result stays a `BsonDocument` on purpose. The explain output is one of the least stable shapes MongoDB
publishes: it differs by server version, by verbosity, and between a standalone and a sharded cluster, where the
winning plan gains a layer per shard. A case class over that would be a guess that compiles and then quietly stops
matching what the server sends. The `it` spec asserts on `IXSCAN`/`COLLSCAN` for the same reason.

## Errors the server raises

`mongo4s` translates every driver failure into `MongoError` before any runtime sees it. The reason is the same one
behind `Filter` being an AST rather than driver builders: a library that exists so you never write
`com.mongodb.*` in service code cannot then require you to catch `com.mongodb.MongoWriteException` and compare
`getError.getCategory` to decide whether a unique index fired.

**Where it happens is the interesting part.** There is no single point in the bridge: `cats` and `kyo` collect
through `PublisherCollector`, `zio` goes through `zio-interop-reactivestreams`, and `rapid` runs everything —
including `list` and `unit` — through `PublisherIterator`. Translating in any one of those would have covered some
runtimes and some methods. So the translation wraps the `Publisher` itself, upstream of all of it, which is the only
place all four runtimes and all five bridge methods provably meet. `RsBridgeBackendSpec` asserts it for `one`,
`option`, `list`, `unit` *and* `stream` on every runtime, because "wrap the publisher here too" is exactly the kind
of thing a fifth runtime would forget.

**Classification comes from the driver, not from a table of numbers.** `ErrorCategory.fromErrorCode` already knows
which codes mean a duplicate key or an execution timeout, so `MongoError` asks it instead of hard-coding 11000 and
11001. Only `WriteConflict` and the two authorisation codes are named here, because the driver has no category for
them.

**A bulk write keeps all of its failures.** One `bulkWrite` or `insertMany` can fail several documents at once, and
collapsing that to a single `DuplicateKey` would silently drop the rest, so `BulkWriteFailed` carries every failure
with the index of the operation that produced it, and `duplicateKeys` is a filter over them rather than a different
shape.

**`DuplicateKey` does not name the index, on purpose.** The server does report `keyPattern` and `keyValue`, but the
driver's `WriteError.getDetails` comes back empty for a duplicate key — verified against MongoDB 7 — so the only
remaining source is the human-readable message. Parsing it would produce a field that works until a server upgrade
rewords the string, then silently returns nothing. The message stays reachable through `getMessage`.

Translating changed one thing that had nothing to do with errors: `withTransaction` decides what to retry by asking
whether the failure carries `TransientTransactionError`, and it used to do that by matching `MongoException`. After
translation it no longer sees one. `MongoError` therefore exposes `labels`/`hasLabel` from the wrapped exception,
and `hasErrorLabel` accepts both — the existing retry specs caught this on all four runtimes, which is the argument
for having had them.

## The query AST

`Filter` and `Update` are real `enum` ADTs that `mongo4s` interprets itself, not thin wrappers over the driver's
opaque `Bson` builders. That is what makes a second interpreter possible at all (see
[repositories](#repositories-and-one-ast-with-two-interpreters)), and it is what lets the AST be *normalized* rather
than passed through.

**Field paths track derived-vs-stored per segment, not per path.** `Field.of[E, A](_.some.field)` is a macro that
reads the selector at compile time; each segment it produces is a *derived* name, spelled through the collection's
`FieldNaming`. Names that are already what the document stores — a map key, an array index, `_id`, a `PrimaryKey`'s
field names, a `$lookup`'s foreign field — are *stored* names, used verbatim. Per-segment is the only granularity
that works: `totals.at("EUR")` has to rename `totals` under `snake_case` while leaving the map key alone.

`Field[E, A]` is opaque over `FieldPath`, which is opaque over `List[Segment]`. Both aliases being transparent
*inside the same file* is a live hazard: `Field./` calling `field.path / segment` resolved to its own `/` and became
an infinite loop. `Field./` and `Field.at` must go through the named `FieldPath.stored`, never the symbolic operator.

**`Filter.and`/`or` fold `all` and `none` away** instead of emitting a one-element `$and`. This is not cosmetic:
it is what makes `field.in(Nil)` produce `Filter.none` rather than an empty predicate, so `deleteMany(Nil)` is a safe
no-op instead of deleting the collection. An empty list is the most dangerous input a query builder takes, and it
gets a defined answer here rather than an emergent one.

**`Update` merges operators of the same name into one sub-document.** `{$set: {a: 1}}` combined with `{$set: {b: 2}}`
is `{$set: {a: 1, b: 2}}`, not two `$set` keys the server would reject. `Update.Raw` merges the same way — but it
must **clone** the caller's document first. Not cloning meant a rendered update wrote into the caller's own
`BsonDocument`: a shared `Raw` came back permanently carrying fields it never declared, and rendering it twice under
different `FieldNaming`s emitted both spellings. An update that would render empty throws rather than sending `{}`.

Numeric operators go through `NumericOf[C, A]`, mirroring `ElementOf[C, A]`, so an `Option[Long]` field takes a plain
`Long`. There is deliberately **no** `NumericValue[Option[A]]`: `None` has no numeric encoding, and the obvious
stand-in — `$inc` by zero — is a write that silently does nothing.

Symbolic aliases (`===`, `=!=`, `>`, `>=`) exist where they read better, each with a `@targetName`.

## Reads: strict by default, lenient on request

`all` fails the whole query on the first undecodable document. That is the right default: a decode failure usually
means the model and the collection disagree, and finding out immediately beats processing 99 documents and silently
dropping one.

But a collection written by more than one version of more than one service will contain documents this version
cannot read, and failing the whole page because of one of them is useless. So `attempting` — on `find`, `aggregate`
and `distinct` — reports each document as `DecodeResult[A] = Either[BsonError, A]`, and `watchAttempting` does the
same for change streams. Transport errors still fail the effect; only decoding is made recoverable, because only
decoding is a per-document property.

## Streaming

Operations expecting at most one document read **two**, not the whole cursor (`SingleResultProbe = 2`) — exactly
enough to notice a second result under `RsBridgeConfig.strictSingleResult`, and no more. `AggregateQuery.first`
pushes a `$limit` into the pipeline for the same reason: bounding work at the server, not in the client.

**`liveStream` exists because fs2 chunks.** `fs2.interop.reactivestreams.fromPublisher(p, bufferSize)` will not emit
an element until the chunk is full or the publisher finishes — its own scaladoc says so. With the default
`bufferSize = 256`, a change stream delivered **nothing** until 256 events had piled up, and a change stream never
finishes. `find(...).stream` only ever worked by accident: its cursor completes, which flushes the partial chunk.

So the bridge has two stream methods. Finite reads keep `stream` and its buffering, which is what makes them fast.
Every `watch` goes through `liveStream`, which the cats backend overrides to `fromPublisher(p, 1)`. `liveStream` has
a default implementation delegating to `stream`, both because of the compatibility promise and because zio
(queue-based), rapid (an enqueue-in-`onNext` iterator) and kyo (`EmitStrategy.Eager`) have no chunk-fill behaviour to
work around.

This was found only because change streams had never actually been exercised — the integration spec was calling
`cancel` on itself and reporting green. Two lessons kept from that: never let an integration spec cancel itself into
looking green, and never time a change-stream test with `sleep` (take the server's `operationTime` from `hello` and
pass `startingAt(now)`, which is race-free no matter when the stream actually subscribes).

`RsBridgeConfig` exposes `bufferSize`, a per-operation `timeout` and `strictSingleResult`. The timeout deliberately
does **not** apply to streams: a change stream sitting idle is working, not stuck.

## Transactions

```scala
client.withTransaction {
  users.insertOne(User("2", "Bob", 41)) // the session is already given here
}
```

It commits on success and rolls back on failure **and on cancellation**. The cancellation half is the whole reason
`Effect.guaranteeCase` has the shape it does. A rollback that itself fails is attached as a suppressed exception
rather than replacing the original error.

**Retries follow the driver, including which failure means what.** A `TransientTransactionError` says nothing was
committed, so the whole transaction is restarted — body and all. An `UnknownTransactionCommitResult` says the commit
may already have landed, so repeating the body could double the write; only the commit is asked again. Treating the
two the same in either direction is a data-loss or double-write bug, which is why they are separate paths rather
than one retry loop.

Both are bounded by a single deadline taken **before the first attempt**, so the retries are bounded in total rather
than per round — an unbounded chain of individually-quick attempts is the failure mode a per-attempt limit misses.
That deadline needs a clock, and `Effect` had none: `monotonic` was added with a default reading `System.nanoTime`,
which is what every runtime's own monotonic clock does anyway, so no implementor had to change. `cats` and `zio`
override it with their runtime's clock, which is what makes the retry window drivable by `TestControl`/`TestClock`
instead of by waiting.

Retrying is the default because it is the driver's default. `TransactionOptions.withoutRetries` restores the `2.x`
behaviour of reporting the first failure.

The manual path (`startTransaction`/`commitTransaction`/`abortTransaction` with `(using Some(session))` at each call
site) is still there, and nothing is automatic on it, including the rollback. Both exist because the safe version
should be the easy one, not the only one.

## Change streams

Every event is a `ChangeEvent[A]` — a real case class with `operationType`, `documentKey`, `fullDocument`,
`fullDocumentBeforeChange`, `updateDescription`, `resumeToken` and `clusterTime` — not a raw `BsonDocument` to dig
through. `watch` exists at all three driver scopes: client (whole deployment), database, and collection.

`fullDocument` defaults to `UPDATE_LOOKUP`, **not** the server's default. MongoDB fills the document in only for
inserts and replaces, which leaves the most common question — "what does this document look like now?" — unanswered
on exactly the events you are usually watching for. The server's default is the surprising one; this is the useful
one.

`resumeAfter` and `startAfter` clear each other, because the server rejects a stream carrying both.

**One known footgun, accepted deliberately:** `WatchOptions[E].pipeline` is typed `Seq[Stage[E]]`, but a change
stream pipeline matches against the *change event envelope* (`{operationType, fullDocument, ns, …}`), not the
document. So a `Field.of` path renders `"age"` where the event needs `"fullDocument.age"`. `Stage.raw` is the right
tool there. The alternative was a second, parallel `Stage` type for events; that was judged worse than one type with
a documented sharp edge.

## Repositories, and one AST with two interpreters

`BaseMongoRepository` gives CRUD, batching and paging over a collection. It is **`open`, not generated** — extra
domain queries are declared directly against `collection`/`Filter`/`Field`, the same escape hatch the driver gives
you. A generated repository would have to either predict every query or make you drop out of the abstraction to
write one.

Batching is empty-list-safe throughout, which follows from `Filter`'s folding: `inFilter(Nil)` is `Filter.none`, so
`deleteMany(Nil)` deletes nothing.

**`BulkWriteResult.upsertedIds` is keyed by command position**, so `combine` deliberately does *not* rebase indices;
a caller splitting one logical write into batches calls `shiftUpsertedIds(offset)` per batch first, since only the
caller knows how many commands preceded it. Getting this wrong collapsed every batch onto index 0 — a bug that only
appears once a write exceeds one batch, which is to say in production and not in tests.

Because `Filter`/`Update` are an AST rather than driver builders, they can be interpreted twice: once into real
`Bson` for the server, and once against an in-memory buffer. `FakeMongoCollection` is that second interpreter —
filters, updates, sorting, paging, projections and a subset of `aggregate` are simulated, while `distinct`, `watch`,
`$text`, `$expr`, the geospatial operators, `Filter.Raw`, `Stage.Raw` and an update carrying `arrayFilters` throw
`UnsupportedOperationException` naming what was asked for rather than quietly answering wrong. A fake that lies is
worse than no fake.

**The aggregation subset is drawn along one line: whether MongoDB's answer is unambiguous.** `$match`, `$sort`,
`$skip`, `$limit`, `$project`, `$count` and `$group` over `$sum`/`$avg`/`$min`/`$max`/`$first`/`$last`/`$push` all
have exactly one right answer, down to the BSON type — `$count` and a `$sum` of `Int32`s yield an `Int32`, `$avg`
always yields a `Double`. `$addToSet` does not: MongoDB leaves the order of its result undefined, so any array a fake
returned would be a guess a test could then depend on. It is refused for that reason rather than for effort, as are
`$sum`/`$avg` over `Decimal128`, whose rounding is the server's to define.

The subset is not maintained by reading the manual. `FakeAggregateParityItSpec` runs the same pipelines through the
fake and through a real MongoDB and compares the raw `BsonDocument`s, so a divergence in value *or* in numeric width
fails the build. That spec is the reason the paragraph above can state the types as fact.

Order is the one thing the fake deliberately does not promise: where MongoDB leaves it undefined, so does the fake,
and the two need not agree — `$group` emits buckets in first-seen order, exactly as `find` without a `Sort` returns
insertion order. A pipeline whose output order matters has to end in `$sort` against a real server too.

It ships as its own published module, `mongo4s-testkit`, so it is usable from a consumer's own tests rather than only
inside this build. Alongside it, `FakeRepository` is a `BaseMongoRepository` over a `FakeMongoCollection` — the real
repository logic, only the collection faked. Nothing about repository behaviour is reimplemented for tests, so the
fake cannot drift from what production does; the only thing standing in for MongoDB is the storage underneath.

## Evolving the API

Three commitments, and the mechanics that make each one keepable:

**New `Effect`/`RsBridge` members carry default implementations.** Implementing either typeclass yourself keeps
compiling across minor releases. `liveStream` and `Effect.monotonic` were both added this way.

**Binary compatibility is checked, not asserted.** MiMa runs in CI against the previous release. Every deliberate
break is a MiMa exclusion in `build.sbt` and an entry in [COMPATIBILITY.md](COMPATIBILITY.md) — waivers are visible, not
silent. `3.0.0` needs no filters at all: a major release is allowed to break, so the list there is a migration guide
rather than a set of waivers.

**MiMa has a blind spot, and the compiler covers it.** An `inline` method that reaches a non-public member makes the
compiler synthesize a public accessor for it. That accessor's name is not part of the stable ABI: recompiling can
rename it, and code that inlined the old one fails at link time. MiMa never sees this, because the accessor is
synthesized rather than declared. `-WunstableInlineAccessors` reports each one and `@publicInBinary` pins it. Three
existed the first time the flag was switched on — both derivations' `make`, and `FieldMacro` reached from
`Field.of`. For a library this inline-heavy the check is not optional, so it is on in the build rather than left to
whoever remembers.

**Configuration types are builders, not case classes.** This one was learned the expensive way. Adding a field with
a default to a `case class` is source-compatible but *never* binary-compatible: default arguments are resolved at the
call site, so the JVM only ever sees the old arity, and `apply`, `copy` and the constructor all change. Since
derivation options will keep arriving, `WireCodecConfig` became a `final class` with a private constructor and
`withX` methods — one break taken once, and every option after it is additive:

```scala
given WireCodecConfig = WireCodecConfig.SnakeCase.withOmitNoneFields(false)
```

`WatchOptions` was the counter-example here for a while — a case class with `withX` on top, carrying the same
hazard, with a note that it should get the same treatment the next time it changed. Adding `showExpandedEvents` was
that time, so it is now a `final class` with a private constructor too, and the note has been paid off rather than
restated.

## Deliberately not done

**A discriminator-free `Either`/`Ior` codec.** Dropping `_type` and trying branch A then branch B via
`BsonReader.getMark()`/`.reset()` is technically feasible — the driver supports mark/reset. It was rejected because
it is silently unsafe whenever the two branches are structurally similar enough that both decode "successfully": the
wrong branch wins with no error. That is precisely the bug class the codec audit existed to eliminate.

**A global `CodecProvider` convenience.** It would remove one implicit parameter and reintroduce process-wide
coupling between unrelated collections. Not worth it.

**Bounding a live stream with rapid's `Stream.take(n)`.** Not a `mongo4s` decision, but worth knowing: rapid decides
`Step.Stop` *inside* `transform`, so it needs element n+1 to arrive before it stops. On an infinite change stream
carrying exactly n events it blocks forever. Not fixable from here.

**A circe bridge.** There is no `mongo4s-bson-circe` module, and there is not going to be one. circe is a *JSON*
codec: routing an entity through it means `case class → circe → some BSON tree → org.bson`, which is the same
`case class ↔ circe ↔ mongo4cats.Bson ↔ org.Bson` trip [BENCHMARKS.md](BENCHMARKS.md) measures at 2.5–5× slower on
decode. That cost is the reason `mongo4s` exists; shipping a first-class path back to it would be arguing against
the library's own premise. A model already on circe should get a `BsonDocumentCodec[A]` written against BSON
directly — `bson-direct` derives one with no third-party dependency at all.
