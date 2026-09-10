# Benchmarks

[← back to the README](README.md)

Six JMH harnesses in [`benchmarks/`](benchmarks). Every number below was measured on one machine, in one
sitting:

| | |
| --- | --- |
| Machine | Apple M4 Max (`Mac16,5`), 14 cores, 36 GB |
| OS | macOS 26.6.2 (`25G83`) |
| JVM | OpenJDK 25.0.4.1 (Homebrew), 64-Bit Server VM, mixed mode, sharing |
| Scala | 3.9.0, sbt 2.0.8, JMH 1.37 |
| Server | `mongo:7` in Docker, `localhost:27018`, single node |

These are directional ballparks from one laptop, not hardware-independent authorities — an M4 Max has no
hyper-threading and a memory system unlike a typical server's, and the MongoDB numbers cross a loopback socket
rather than a network. Run them on your own hardware before making decisions on the numbers alone.

## Codec backends — to `org.bson.BsonDocument`

[`CodecBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/CodecBenchmark.scala) encodes/decodes a mid-size
entity (seven fields, one nested object) through each backend, straight to/from `org.bson.BsonDocument`, against
`mongo4cats`'s own codec modules (`mongo4cats-circe`, `mongo4cats-zio-json`):

```bash
sbt "benchmarks/Jmh/run -f 6 mongo4s.benchmarks.CodecBenchmark"
```

| Codec | Encode ops/s | Decode ops/s |
| --- | ---: | ---: |
| `mongo4s-bson-direct` (via `DocumentCodecBridge`) | ~2.74M | ~2.07M |
| `calypso` (`forProductN`) | **~4.22M** | **~3.67M** |
| `medeia` (`derives`) | ~2.42M | ~2.72M |
| `zio-bson` (`zio-schema` derived) | ~1.31M | ~2.71M |
| `mongo4cats-zio-json` | ~1.25M | ~1.12M |
| `mongo4cats-circe` | ~1.21M | ~0.79M |

Every `mongo4s` codec bridge beats both `mongo4cats` codecs on decode, by **1.8×** at the narrowest
(`bson-direct` against `zio-json`) and **4.6×** at the widest (`calypso` against `circe`) — the cost of
`case class ↔ circe/zio-json ↔ mongo4cats.Bson ↔ org.Bson` instead of straight to `org.bson`. On encode `calypso`'s
handwritten `forProductN` is **1.5×** the next backend, which is what skipping derivation buys; it leads on decode
too. Run-to-run error is ±3.4–8.1% across the table, wide enough that neighbouring rows are not separated by it —
`medeia` and `zio-bson` decode at the same rate within error, and so do the two `mongo4cats` encoders.

`bson-direct` is in this table through `DocumentCodecBridge.toDocumentCodec` — the path `aggregate` takes on a
direct collection when the caller hands it that bridge, `WireCodec` forced to materialize the `BsonDocument` it
normally skips. Handicapped that way it is a middling backend rather than a fast one: **1.13×** `medeia` on encode,
and behind both `medeia` and `zio-bson` on decode, which read the `BsonDocument` natively instead of through a
`BsonDocumentReader`. Its AST-free numbers are in the next table — but note the two tables stop in different places,
this one at a `BsonDocument` and the next at real bytes, so rows are not comparable across them. What the
intermediate tree costs shows up *inside* the next table, where every backend is measured to the same endpoint.

## AST-free wire codec — all the way to real bytes

[`WireFreeCodecBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/WireFreeCodecBenchmark.scala) goes one step
further than the table above: the same entity, but all the way to **real BSON wire bytes**
(`BsonBinaryWriter`/`BsonBinaryReader`) rather than stopping at a `BsonDocument`. That is the trip the driver actually
makes, and it is the only axis on which an AST-free codec can be compared to an AST-based one at all.

Every backend is measured on that one axis, doing whatever it has to do to get there:

* `WireCodec` writes into the `BsonWriter` directly — no intermediate representation at all.
* `medeia`/`calypso`/`zio-bson` build their `BsonDocument` first, then the driver's own `BsonDocumentCodec` walks
  that document out to bytes — two tree-walks.
* `mongo4cats-circe`/`mongo4cats-zio-json` build a `mongo4cats.bson.BsonValue`, which its own codec then walks out.

Every row below is a backend you can actually pick. `WireCodec` is what a `getDirectCollection` call runs: the derived
codec is handed to the driver through a two-method adapter, and the driver calls it with its own
`BsonBinaryWriter`/`BsonBinaryReader` — nothing else sits in between.

```bash
sbt "benchmarks/Jmh/run -f 6 -prof gc WireFreeCodecBenchmark"
```

**Encode — case class → BSON bytes**

| Backend | Throughput | Alloc |
| --- | ---: | ---: |
| hand-written, straight to the wire | ~2.48M ops/s | 1824 B/op |
| **`WireCodec`** | **~2.08M ops/s** | **1880 B/op** |
| `calypso` | ~1.42M ops/s | 3211 B/op |
| `medeia` | ~1.23M ops/s | 4592 B/op |
| `zio-bson` | ~866k ops/s | 4785 B/op |
| `mongo4cats-circe` | ~717k ops/s | 6411 B/op |
| `mongo4cats-zio-json` | ~651k ops/s | 5528 B/op |

**Decode — BSON bytes → case class**

| Backend | Throughput | Alloc |
| --- | ---: | ---: |
| **`WireCodec`** | **~1.60M ops/s** | **1248 B/op** |
| hand-written, straight to the wire | ~1.55M ops/s | 1088 B/op |
| `calypso` | ~1.03M ops/s | 3183 B/op |
| `medeia` | ~954k ops/s | 2861 B/op |
| `zio-bson` | ~864k ops/s | 2840 B/op |
| `mongo4cats-zio-json` | ~599k ops/s | 6313 B/op |
| `mongo4cats-circe` | ~502k ops/s | 8552 B/op |

Three things fall out of this.

`WireCodec` **wins both directions against every AST backend** — ahead of the best of them by
**1.47×** on encode (`calypso`) and **1.68×** on decode (`medeia`), and ahead of both
`mongo4cats` codecs by **2.7–3.2×**. It allocates **2.4×** less than `medeia` on encode,
**2.3×** less on decode, and **6.9×** less than `mongo4cats-circe` on decode.

**Derivation is not free, and the hand-written row is the ceiling it is measured against.** A codec written by hand
straight into the `BsonWriter` encodes **1.19×** faster than the derived one and allocates
**3%** less; on decode the two are level within error. That gap is what `derives WireCodec` costs, and it is
the honest upper bound for the AST-free path — not a reason to hand-write one, but worth knowing it exists.

The `AST` backends do not rank the same in both directions — `calypso` leads on encode and trails on decode, `medeia`
is the reverse — but every one of them is on the far side of a gap that comes from building an intermediate tree at
all, not from how well it is built.

In a typical CRUD workload the network round trip dwarfs all of this (see below); it matters for bulk paths — large
cursor streams, ETL, aggregation over big result sets.

Both tables above use an entity of `String`/`Int`/`Double`/`Boolean`/`List`/nested-object fields — every one of them
written natively by every backend. That is the best case for `WireCodec`, and the next table is the one that is not.

## Scalar fields — native vs bridged

A type `bson-direct` has no `ScalarWireCodec` for falls back to its `BsonEncoder`/`BsonDecoder`, which materializes one
`org.bson.BsonValue` per field — the very allocation the path exists to avoid.
[`ScalarWireCodecBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/ScalarWireCodecBenchmark.scala) measures what
that costs, on an entity of seven fields where four are `ObjectId`, `UUID`, `Instant` and `BigDecimal`:

```bash
sbt "benchmarks/Jmh/run -f 2 -prof gc ScalarWireCodecBenchmark"
```

| Direction | Native `ScalarWireCodec` | Through the `BsonValue` bridge |
| --- | ---: | ---: |
| Encode | **~2.58M ops/s**, 1768 B/op | ~2.24M ops/s, 2064 B/op |
| Decode | **~2.05M ops/s**, 1248 B/op | ~1.60M ops/s, 1736 B/op |

Decode is where it shows: **1.28×** the throughput and **28%** less garbage, four fields out of seven. Encode gains
much less (**1.15×**, 14% less garbage) because building a `BsonDateTime` or a `BsonObjectId` on the way out is cheap;
reading one back means allocating it *and* walking the `BsonDecoder`'s type checks.

The allocation figures are the durable half of this table. Bytes per operation are a property of the code rather
than of the machine: three of these four came back byte-for-byte identical to the run on the previous laptop
(2064, 1248 and 1736), while every throughput ratio moved. The fourth, native encode, is 1768 against 1800 there —
the codecs themselves changed in between.

Both columns write byte-identical BSON — the bridge is not a different format, only a slower way to the same bytes.

## Typed errors — what the translation costs

Since `3.0.0` every `Publisher` the driver hands back is wrapped so that failures arrive as `MongoError` rather than
as driver exceptions. That wrapper sits on the hot path of every operation and every stream, one extra `Subscriber`
per subscription and one extra virtual call per element, so it is worth knowing what it costs when nothing fails.

[`ErrorTranslationBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/ErrorTranslationBenchmark.scala) drains a
synchronous publisher of *n* elements with and without the wrapper — no server, no codec, nothing but the bridge:

```bash
sbt "benchmarks/Jmh/run -f 3 -prof gc ErrorTranslationBenchmark"
```

| Elements | Raw publisher, ops/µs | Through `translating`, ops/µs | Alloc |
| ---: | ---: | ---: | ---: |
| 1 | 355.7 ± 22.9 | 354.0 ± 21.4 | 24 → 24 B/op |
| 100 | 4.5 ± 0.240 | 4.4 ± 0.239 | 24 → 24 B/op |
| 10,000 | 0.035 ± 0.002 | 0.034 ± 0.002 | 24 → 40 B/op |

**Every difference is inside the error bars**, at one element and at ten thousand alike, and the wrapper's own
allocation is one object per *subscription* — not per element — which is why the per-operation figure is unchanged
until the 10,000-element case, where it is 16 B on a 24 B baseline. Typed errors are not paid for on the happy path.

## Talking to a server

The three sections that follow all cross a socket to `MongoDB`, and on this machine that socket is the whole story.
`MongoDB` runs in Docker Desktop, which on macOS is a Linux VM, and a round trip through it costs milliseconds. The
floor was measured directly, from a shell **inside the container**, with no port mapping in the path at all:

```
empty loop  0.000 ms/op
ping        2.40 ms/op → ~418 ops/s
findOne     2.17 ms/op → ~462 ops/s
```

An empty `mongosh` loop is free, so that 2.4 ms is the server round trip itself. Every number in the sections below
is bounded by it: `mongo4s` doing real work from the host lands around **200 ops/s**, roughly half the rate a bare
shell gets for a `ping` in the same container.

So read those tables as **comparisons, not as rates**. Which stack is faster than which, and by how much, is what
they measure; the absolute operations-per-second are a property of Docker on macOS. The same benchmarks on the
previous machine reported around **2400 ops/s** for the same single-document operations — a ~12× difference in the
environment, not in the library. A latency-bound measurement also *compresses* every ratio, because the constant
round trip is a larger share of each result: where a table below shows a gap, the real gap is at least that wide.

The codec sections above have no server in them and are unaffected.

## Aggregation through a cursor — real `MongoDB`

The tables above measure a codec in isolation. [`AggregateBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/AggregateBenchmark.scala)
asks whether any of it survives a real server: the same `$match` pipeline, the same collection, decoded three ways —
`aggregateDirect` (AST-free), `aggregate` through `DocumentCodecBridge` (the same `WireCodec`, forced to materialize a
`BsonDocument`), and `aggregate` through `medeia`.

```bash
docker run -d --name mongo4s-bench -p 27018:27017 mongo:7
sbt "benchmarks/Jmh/run -f 1 AggregateBenchmark"
```

| Documents returned | `aggregateDirect` | `aggregate` + bridge | `aggregate` + `medeia` |
| ---: | ---: | ---: | ---: |
| 10 | 194 ops/s | 199 ops/s | **205 ops/s** |
| 10000 | **67 ops/s** | 53 ops/s | 53 ops/s |

**Both rows are the point.** At ten documents the three are indistinguishable — they sit inside each other's
error bars (±7–10%), because the round trip is everything and the codec is noise, which is the honest answer for
most CRUD. At ten thousand the decode starts to tell instead, and `aggregateDirect` is
**1.27×** the bridged path and **1.26×** `medeia`, *including* the network round trip.

Read the absolute rates with the caveat in [Talking to a server](#talking-to-a-server) below — on this machine they
are latency-bound, which compresses the ratio. The direction survives; the size of the gap would be larger wherever
the round trip is cheaper.

So the AST-free claim for `aggregate` is worth what it says on bulk reads and nothing at all on small ones. Pick
`aggregateDirect` for cursors that return a lot; below that it is a wash and either call is fine.

## Runtime overhead — real MongoDB, every backend

[`RuntimeBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/RuntimeBenchmark.scala) runs the same
insert/find/update/delete/count workload against a real MongoDB through every `mongo4s` runtime, and `mongo4cats` as a
reference point:

```bash
docker run -d --name mongo4s-bench -p 27018:27017 mongo:7
sbt "benchmarks/Jmh/run -tu s .*RuntimeBenchmark.*"
```

**Throughput — operations per second, higher is better**

| Operation | mongo4s-cats | mongo4s-zio | mongo4s-rapid | mongo4s-kyo | mongo4cats-cats | mongo4cats-zio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `insertOne` | 203 | 184 | **211** | 167 | 201 | 185 |
| `find(filter).all` (~100 docs) | 199 | 186 | 201 | 158 | **208** | 177 |
| `find(filter).stream` (~100 docs) | 193 | 185 | **203** | 152 | 14 | 181 |
| `updateOne` | 202 | 186 | **211** | 162 | 200 | 184 |
| `count(filter)` | 199 | 181 | 200 | 160 | **202** | 180 |

With a clean database on every trial, every column lands in the same band for every operation — the round trip
dominates at this scale, and none of the six `RsBridge`/collection wrappers stands out. **Bold** is the highest cell
in each row, but read it as the measured extreme rather than a ranking: run-to-run error is ±4–41%, far wider than
the spread between the columns. The `kyo` and `zio` columns sit a few percent below the rest across the board, which
is inside that error and not a finding.

The one real outlier is `mongo4cats-cats`'s `find(filter).stream` — **14** *ops/s* against its own `.all`'s
**208** and **201** on its single-document ops, a **15×** gap that no round trip explains. It bridges through a
hand-rolled `cats.effect.std.Queue`-backed `Subscriber` instead of `fs2.interop.reactivestreams`, which
`mongo4s-cats` uses. Each `mongo4s` runtime uses its own interop — `zio-interop-reactivestreams`,
`kyo-reactive-streams`, and a `PublisherIterator` of mongo4s's own for `rapid`. Note `mongo4cats-zio` does *not*
share the problem (181 ops/s, level with its own `.all`), so this is one bridge rather than the library.

`deleteOne` reads about half of the other single-document operations because the benchmark inserts a document first,
so its number covers two round trips rather than one.

## Codec choice under real `MongoDB` — `mongo4s` vs `mongo4cats`

The same `RuntimeBenchmark` also isolates the *codec* dimension: four configs, all on cats-effect, against the same
real `MongoDB` — `mongo4s` with `bson-medeia` vs `bson-direct`, and `mongo4cats` with `circe` vs `zio-json`. Two
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
| `insertOne` | **203** | 201 | 201 | 202 |
| `insertMany` (10 docs) | **192** | 191 | 192 | 186 |
| `findOneById` | 200 | 198 | **203** | 202 |
| `findOneByFilter` | 199 | 197 | **205** | 197 |
| `findAll` (~100 docs) | 199 | 197 | **208** | 195 |
| `findStream` (~100 docs) | **193** | 191 | 14 | 14 |
| `updateOne` | **202** | 199 | 200 | 200 |
| `deleteOne`\* | 99 | **101** | 100 | 99 |
| `count` | 199 | 196 | **202** | 200 |

Same finding as the [runtime table above](#runtime-overhead--real-mongodb-every-backend): every operation but
`findStream` lands in the same band, inside the run-to-run error, so **bold** marks the measured extreme rather than
a ranking — the round trip dominates regardless of codec. The one outlier is `mongo4cats`' `find(filter).stream`,
**15×** _slower_ than its own single-document operations *and independent of codec*
(13.8 ops/s for circe and 13.8 for zio-json — the same number) — which confirms it is the runtime's
`Queue`-backed `Subscriber` bridge rather than the codec, exactly as found earlier.

On throughput the two `mongo4s` codecs are indistinguishable here, and so are they from `mongo4cats` on everything
but the stream: at ~200 ops/s a codec has nowhere to show. The allocation table below is where the codec choice is
actually visible, and it is the one to read.

**Memory allocated per single call, in *KB* — lower is less garbage, not less work done**

| Operation | mongo4s+medeia | mongo4s+bson-direct | mongo4cats+circe | mongo4cats+zio-json |
| --- | ---: | ---: | ---: | ---: |
| `insertOne` | 26.2 | **23.3** | 26.9 | 26.2 |
| `insertMany` (10 docs) | 66.7 | **40.4** | 81.1 | 72.8 |
| `findOneById` | 37.7 | **35.3** | 41.2 | 38.3 |
| `findOneByFilter` | 37.5 | **35.2** | 41.0 | 38.2 |
| `findAll` (~100 docs) | 326.6 | **157.8** | 935.5 | 642.6 |
| `findStream` (~100 docs) | 375.9 | **210.3** | 2830.5 | 2566.3 |
| `updateOne` | **23.5** | 23.6 | 24.1 | 23.9 |
| `deleteOne`\* | 48.8 | **45.7** | 48.8 | 48.2 |
| `count` | 33.2 | 33.2 | 33.0 | **32.9** |

Two things worth noting:

* **`bson-direct` allocates less than `bson-medeia` wherever a document actually passes through the codec** — the
  AST-free advantage measured in isolation ([above](#ast-free-wire-codec--all-the-way-to-real-bytes)) survives
  end-to-end through a real driver round trip, not just in a codec microbenchmark. The gap tracks how many documents
  a call encodes or decodes: **11%** for one, **39%** on `insertMany`, and **44–52%** on
  `findAll`/`findStream` — the more documents per call, the more the saved `BsonDocument` tree-walks compound. On
  `updateOne` and `count` the two are level to three digits (23.53 against 23.57 *KB*, 33.25 against
  33.20), which is the expected result rather than a surprise: neither operation encodes or decodes an entity,
  so there is nothing for a codec to do differently.
* **`mongo4s` now matches `mongo4cats` on single-document ops, and stays far lighter on bulk reads.** On
  `insertOne`/`updateOne`/`deleteOne`/`count`/`findOneById` all four configurations sit within a couple of percent
  of each other — the driver's own per-call buffers dominate, and both libraries pay them. On `findAll`/`findStream`
  `mongo4cats` allocates **2.0–13×** *more* than either `mongo4s` config — `mongo4cats.bson.BsonValue`, its own wrapper
  type,
  adds a full extra tree per document on top of `org.bson`'s, and that cost multiplies with document count. This is
  a change from the pre-1.0 numbers, where `mongo4s` was the heavier of the two on every single-document operation.

