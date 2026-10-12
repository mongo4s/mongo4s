# Benchmarks

[← back to the README](README.md)

Six JMH harnesses in [`benchmarks/`](benchmarks). Every number below was measured on one machine, in one evening
(2026-10-12), on `mongo4s` `4.1.0`:

| | |
| --- | --- |
| Machine | Apple M4 Max (`Mac16,5`), 14 cores, 36 GB |
| OS | macOS 26.7.1 (`25G241`) |
| JVM | OpenJDK 25.0.4.1 (Homebrew), 64-Bit Server VM, mixed mode, sharing |
| Scala | 3.9.0, sbt 2.0.8, JMH 1.37 |
| Docker | Docker Desktop, engine 29.8.2, Linux VM kernel `7.0.14-linuxkit` |
| Server | `mongo:8.2` (`8.2.12`) in Docker, `localhost:27018`, single node; `mongo:7` (`7.0.40`) where stated |

These are directional ballparks from one laptop, not hardware-independent authorities — an M4 Max has no
hyper-threading and a memory system unlike a typical server's, and the MongoDB numbers cross a loopback socket
rather than a network. Run them on your own hardware before making decisions on the numbers alone.

## What `mongo4s` is measured against

The reference point is `mongo4cats`, and the codec tables also name the third-party BSON codec libraries. These are
the versions on the benchmarks' classpath, as resolved — `sbt 'show benchmarks/Jmh/fullClasspath'` — not the ones the
build file asked for:

| | Version | Used for |
| --- | --- | --- |
| `mongo4cats` (`-core`, `-circe`, `-zio`, `-zio-json`) | **0.8.0** | the `mongo4cats` rows everywhere |
| `circe` | 0.14.17 | `mongo4cats-circe`'s codec |
| `zio-json` | 1.1.0 | `mongo4cats-zio-json`'s codec |
| `medeia` | 1.0.9 | `medeia` rows, and `mongo4s-bson-medeia` |
| `calypso` | 0.7.0 | `calypso` rows, and `mongo4s-bson-calypso` |
| `zio-bson` / `zio-schema` | 1.0.11 / 1.9.0 | `zio-bson` rows |
| `cats-effect` / `fs2` | 3.7.1 / 3.14.0 | the `cats` runtimes, both libraries |
| `zio` | 2.1.26 | the `zio` runtimes, both libraries |
| `kyo` / `rapid` | 1.0.0-RC7 / 2.9.9 | the `kyo` and `rapid` runtimes (`mongo4s` only) |
| `mongodb-driver-reactivestreams` | 5.13.0 | **both** libraries — one driver jar serves the whole JVM |

Two things follow from that last row. `mongo4s` and `mongo4cats` are on the *same* driver here, so a gap between them
is not a driver-version gap. And earlier revisions of this page compared against `mongo4cats` `0.7.18` and `circe`
`0.14.16`; `0.8.0` is what is measured now, and the two were also run against each other in the same environment — see
[Streams in `mongo4cats` 0.8.0](#streams-in-mongo4cats-080).

## Codec backends — to `org.bson.BsonDocument`

[`CodecBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/CodecBenchmark.scala) encodes/decodes a mid-size
entity (seven fields, one nested object) through each backend, straight to/from `org.bson.BsonDocument`, against
`mongo4cats`'s own codec modules (`mongo4cats-circe`, `mongo4cats-zio-json`):

```bash
sbt "benchmarks/Jmh/run -f 6 mongo4s.benchmarks.CodecBenchmark"
```

| Codec | Encode ops/s | Decode ops/s |
| --- | ---: | ---: |
| `mongo4s-bson-direct` (via `DocumentCodecBridge`) | ~5.14M | ~3.86M |
| `calypso` (`forProductN`) | **~7.22M** | **~6.76M** |
| `medeia` (`derives`) | ~4.57M | ~5.37M |
| `zio-bson` (`zio-schema` derived) | ~2.38M | ~4.55M |
| `mongo4cats-zio-json` | ~1.85M | ~1.94M |
| `mongo4cats-circe` | ~2.66M | ~1.98M |

Every `mongo4s` codec bridge beats both `mongo4cats` codecs on decode, by **1.9×** at the narrowest
(`bson-direct` against `circe`) and **3.5×** at the widest (`calypso` against `zio-json`) — the cost of
`case class ↔ circe/zio-json ↔ mongo4cats.Bson ↔ org.Bson` instead of straight to `org.bson`. On encode the
picture is less one-sided: `calypso`'s handwritten `forProductN` is **1.40×** the next backend (`bson-direct`), which is
what skipping derivation buys, but `mongo4cats-circe` encodes faster than `zio-bson` here (**1.12×**) and the
two `mongo4cats` encoders are 1.4× apart. Run-to-run error is ±0.5–3.4% across the table, narrow enough that every
neighbouring pair is separated by it.

`bson-direct` is in this table through `DocumentCodecBridge.toDocumentCodec` — the path `aggregate` takes on a
direct collection when the caller hands it that bridge, `WireCodec` forced to materialize the `BsonDocument` it
normally skips. Handicapped that way it is a middling backend rather than a fast one: **1.13×** `medeia` on encode,
and behind `medeia`, `zio-bson` and `calypso` on decode, which read the `BsonDocument` natively instead of through a
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
| hand-written, straight to the wire | ~4.49M ops/s | 1824 B/op |
| **`WireCodec`** | **~3.59M ops/s** | **1880 B/op** |
| `calypso` | ~2.53M ops/s | 3219 B/op |
| `medeia` | ~2.15M ops/s | 4592 B/op |
| `zio-bson` | ~1.49M ops/s | 4709 B/op |
| `mongo4cats-circe` | ~1.32M ops/s | 6331 B/op |
| `mongo4cats-zio-json` | ~1.11M ops/s | 5408 B/op |

**Decode — BSON bytes → case class**

| Backend | Throughput | Alloc |
| --- | ---: | ---: |
| **`WireCodec`** | **~2.64M ops/s** | **1248 B/op** |
| hand-written, straight to the wire | ~2.61M ops/s | 1088 B/op |
| `calypso` | ~1.74M ops/s | 3179 B/op |
| `medeia` | ~1.60M ops/s | 2760 B/op |
| `zio-bson` | ~1.50M ops/s | 2872 B/op |
| `mongo4cats-zio-json` | ~1.09M ops/s | 7127 B/op |
| `mongo4cats-circe` | ~974k ops/s | 8004 B/op |

Three things fall out of this.

`WireCodec` **wins both directions against every AST backend** — ahead of the best of them, `calypso`, by
**1.42×** on encode and **1.51×** on decode, and ahead of both `mongo4cats` codecs by **2.4–3.2×**. It allocates
**2.4×** less than `medeia` on encode, **2.2×** less on decode, and **6.4×** less than `mongo4cats-circe` on decode.

**Derivation is not free, and the hand-written row is the ceiling it is measured against.** A codec written by hand
straight into the `BsonWriter` encodes **1.25×** faster than the derived one and allocates
**3%** less; on decode the two are level within error. That gap is what `derives WireCodec` costs, and it is
the honest upper bound for the AST-free path — not a reason to hand-write one, but worth knowing it exists.

The `AST` backends rank the same way in both directions here — `calypso`, then `medeia`, then `zio-bson` — and every
one of them is on the far side of a gap that comes from building an intermediate tree at all, not from how well it is
built. (They did not rank the same on the previous revision of this page, where `calypso` led on encode and trailed
on decode; the order is a property of the run as much as of the library, and the gap to `WireCodec` is the part that
held.)

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
| Encode | **~4.51M ops/s**, 1800 B/op | ~3.84M ops/s, 2064 B/op |
| Decode | **~3.44M ops/s**, 1248 B/op | ~2.48M ops/s, 1736 B/op |

Decode is where it shows: **1.39×** the throughput and **28%** less garbage, four fields out of seven. Encode gains
much less (**1.17×**, 13% less garbage) because building a `BsonDateTime` or a `BsonObjectId` on the way out is cheap;
reading one back means allocating it *and* walking the `BsonDecoder`'s type checks.

The allocation figures are the durable half of this table. Bytes per operation are a property of the code rather
than of the machine: all four came back byte-for-byte identical to the run on the laptop before this one (1800,
2064, 1248 and 1736), while every throughput ratio moved. The native-encode figure was 1768 in the revision between
the two — the codecs themselves changed in that window, and changed back.

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
| 1 | 620.9 ± 3.5 | 614.9 ± 2.1 | 24 → 24 B/op |
| 100 | 8.12 ± 0.034 | 8.03 ± 0.026 | 24 → 24 B/op |
| 10,000 | 0.0638 ± 0.0001 | 0.0636 ± 0.0001 | 24 → 40 B/op |

**The wrapper is measurable, and it is about one percent**: **1.0%** at one element and **1.1%** at a hundred, just
outside the error bars (the intervals are a few tenths of a percent wide), and **0.3%** at ten thousand, inside them.
The previous revision found every difference inside its error bars; this run's intervals are narrow enough to see
that there is a difference at the small sizes, and that it is small. The wrapper's own
allocation is one object per *subscription* — not per element — which is why the per-operation figure is unchanged
until the 10,000-element case, where it is 16 B on a 24 B baseline. Typed errors cost about a percent on the happy
path, and nothing that grows with the stream.

## Talking to a server

The sections that follow all cross a socket to `MongoDB`, and the first thing to know is how long a round trip is.
`MongoDB` runs in Docker Desktop, which on macOS is a Linux VM. The floor was measured directly, from a shell **inside
the container** with no port mapping in the path at all (2,000 operations per line, `mongosh`):

```
empty loop  0.000 ms/op
ping        0.135 ms/op → ~7,400 ops/s
findOne     0.129 ms/op → ~7,750 ops/s
```

An empty `mongosh` loop is free, so that 0.13 ms is the server round trip itself. From the host, through the port
mapping, `mongo4s` does single-document work at **5,000–6,000 ops/s** — 0.17–0.20 ms an operation, within about
0.07 ms of the bare shell's floor — so the round trip is still most of what a single-document call costs, which is
why those operations come out level in the tables below. It is no longer all of it.

**That is a different environment from the earlier revisions of this page, and the difference is not the library.**
They recorded a floor of 2.40 ms and around **250 ops/s** for the same single-document operations, on the same
class of hardware. The cleanest evidence that it was the environment: `mongo4cats` `0.7.18` — a library that has
not changed — measured `insertOne` at 256 ops/s then and at **5,970** ops/s today, on the same benchmark. Docker
Desktop (engine `29.8.2` today) is what moved, ~23×; this page does not try to say which part of it. Read the
absolute rates here as belonging to *this* environment, and the ratios as the thing that travels.

A latency-bound measurement compresses every ratio, because the constant round trip is a larger share of each
result; at 0.13 ms that compression is much milder than at 2.4 ms. The earlier revision said the real gap on a
cheaper round trip would be wider than the one it showed. It is — the aggregation gap in the next section went from
1.37× to 1.40× over ten thousand documents and from 1.19× to 1.26× against `medeia`.

The codec sections above have no server in them and are unaffected.

**How much the machine alone moves these numbers**, measured rather than guessed: the 36 operations in the
`RuntimeBenchmark` throughput run were measured twice, an hour apart, once as the throughput run (`-f 3`) and once
beside the allocation profile (`-f 2`) — different forks, a different JMH session, the same container. The median
difference is **0.4%**, 30 of the 36 are within 1%, and the worst is **6.1%** (`count`, the noisiest row). Any
cross-run comparison narrower than that is reading noise; the tables below are far wider apart than that, except
where they say otherwise.

Which is why the server versions were compared by **interleaving** them — `7`, `8.2`, `7`, `8.2`, `7`, `8.2`, each
a fresh container and a fresh JMH run of `AggregateBenchmark` — rather than by running one after the other. Done that
way the difference reproduces in all three rounds, and goes both ways, as it did on the previous revision: `8.2`
(`8.2.12`) is **3–4% faster** than `7` (`7.0.40`) on the ten-document case, for all three decoders, and **8–19% slower**
on the ten-thousand-document one (`aggregateDirect` 0.81–0.86×, `medeia` 0.89–0.91×, the bridged path 0.91–0.92×).
The previous revision measured +9% and −8%; the direction held and the size did not, which is what a different
environment does to a server comparison. The *order* of the three decoders is the same on both servers.

The `mongo:8` tag is worth a note, because the previous revision of this page said it would not start. It pointed at
the `8.0` line, which refused to run on Linux kernels 6.19 and newer
([SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912)), and Docker Desktop's VM was past that. Today it does
not reproduce: Docker Desktop's VM is on kernel `7.0.14`, `mongo:8.0.30` starts on it, and the `mongo:8` tag has itself
moved to `8.3.9`, which starts too. The tag is still pinned to `8.2` here, but for a different reason — a floating tag
moved from one release line to another underneath this page without any change on our side, and a benchmark needs
a server that does not.

## Aggregation through a cursor — real `MongoDB`

The tables above measure a codec in isolation. [`AggregateBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/AggregateBenchmark.scala)
asks whether any of it survives a real server: the same `$match` pipeline, the same collection, decoded three ways —
`aggregateDirect` (AST-free), `aggregate` through `DocumentCodecBridge` (the same `WireCodec`, forced to materialize a
`BsonDocument`), and `aggregate` through `medeia`.

```bash
docker run -d --name mongo4s-bench -p 27018:27017 mongo:8.2
sbt "benchmarks/Jmh/run -f 1 AggregateBenchmark"
```

| Documents returned | `aggregateDirect` | `aggregate` + bridge | `aggregate` + `medeia` |
| ---: | ---: | ---: | ---: |
| 10 | 5,538 ops/s | 5,486 ops/s | 5,492 ops/s |
| 10000 | **101 ops/s** | 72 ops/s | 80 ops/s |

**Both rows are the point.** At ten documents the three are indistinguishable — they sit inside each other's
error bars (±3–4%), because the round trip and the server's work are everything and the codec is noise, which is the
honest answer for most CRUD. At ten thousand the decode starts to tell instead, and `aggregateDirect` is
**1.40×** the bridged path and **1.26×** `medeia`, *including* the network round trip.

Read the absolute rates with the caveat in [Talking to a server](#talking-to-a-server) above — the round trip is
cheap here, but it is not free, and it still compresses the ratio a little.

So the AST-free claim for `aggregate` is worth what it says on bulk reads and nothing at all on small ones. Pick
`aggregateDirect` for cursors that return a lot; below that it is a wash and either call is fine.

## Runtime overhead — real MongoDB, every backend

[`RuntimeBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/RuntimeBenchmark.scala) runs the same
insert/find/update/delete/count workload against a real MongoDB through every `mongo4s` runtime, and `mongo4cats` `0.8.0` as a
reference point:

```bash
docker run -d --name mongo4s-bench -p 27018:27017 mongo:8.2
sbt "benchmarks/Jmh/run -tu s -f 3 .*RuntimeBenchmark.*"
```

**Throughput — operations per second, higher is better**

| Operation | mongo4s-cats | mongo4s-zio | mongo4s-rapid | mongo4s-kyo | mongo4cats-cats | mongo4cats-zio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `insertOne` | 6,044 | 5,346 | **6,442** | 5,992 | 5,999 | 6,088 |
| `find(filter).all` (~100 docs) | **3,420** | 3,034 | 3,266 | 3,410 | 2,542 | 2,687 |
| `find(filter).stream` (~100 docs) | 3,174 | 3,044 | 3,179 | **3,258** | 1,626 | 2,218 |
| `updateOne` | 4,700 | 4,326 | **5,052** | 4,777 | 4,783 | 4,825 |
| `count(filter)` | **3,872** | 3,406 | 3,852 | 3,560 | 3,697 | 3,710 |

On single-document operations the stacks land in one band — within 8% of each other, `zio` aside — and the round
trip is why. **Bold** is the highest cell in each row; read it as the measured extreme rather than a ranking where
the cells are within error (run-to-run error is ±0.5–8.9% across this table). Two things do separate:

* **`mongo4s-zio` sits below the rest on every row — 4–12% under `mongo4s-cats`** — and `mongo4s-rapid` is
  nominally ahead of the field on `insertOne` and `updateOne` (7–8% over `cats`). Neither is large, but the `zio`
  gap is outside the error bars on every row.
* **On bulk reads `mongo4cats` is behind.** On the same effect system, `find(filter).all` is **1.35×** faster through
  `mongo4s` on `cats` and **1.13×** on `zio`, and `find(filter).stream` is **1.95×** (`cats`) and **1.37×** (`zio`)
  faster — outside the error bars on all four comparisons, the `zio` `.all` one narrowly.

`deleteOne` reads about half of the other single-document operations because the benchmark inserts a document first,
so its number covers two round trips rather than one.

### Streams in `mongo4cats` 0.8.0

The previous revision of this page had `mongo4cats-cats`'s `find(filter).stream` at **10** *ops/s* against its own
`.all`'s 229 — a 22× gap that no round trip explains — and attributed it to the hand-rolled
`cats.effect.std.Queue`-backed `Subscriber` it uses in place of `fs2.interop.reactivestreams`, which `mongo4s-cats`
uses (the other `mongo4s` runtimes each use their own: `zio-interop-reactivestreams`, `kyo-reactive-streams`, and a
`PublisherIterator` of `mongo4s`'s own for `rapid`). `mongo4cats` `0.8.0` changed that number a great deal. Why is not
something this page can say — the `0.8.0` release notes do not mention `find().stream` — so the claim here is only what
was measured: the **same benchmark, the same environment, the same container**, run once with `mongo4cats` `0.7.18`
(the benchmarks as they were before the upgrade) and once with `0.8.0`:

| `find(filter).stream`, ~100 docs | `0.7.18` | `0.8.0` | |
| --- | ---: | ---: | ---: |
| `mongo4cats-cats` (circe codec) | 392 ± 10 ops/s | 1,626 ± 107 ops/s | **4.15×** |
| `mongo4cats-cats` (zio-json codec) | 398 ± 6 ops/s | 1,777 ± 162 ops/s | **4.47×** |
| `mongo4cats-zio` | 2,118 ± 222 ops/s | 2,218 ± 197 ops/s | 1.05× (within error) |

Three readings of that.

**It improved ~4×, and only on the `cats` runtime.** The `zio` column did not move, which fits the earlier finding
that `mongo4cats-zio` never had the problem. Every row of the 27 `mongo4cats` benchmarks run both ways
other than those two `cats` streams is within about 5% of itself between the two versions — `insertOne`,
`findOneById`, `count`, `findAll`, `deleteOne` and the rest — so nothing else about the environment or the upgrade
moved. The stream is the only thing that did.

**The old 10 ops/s overstated the problem, because the environment amplified it.** `0.7.18` in *today's* environment
streams at ~390 ops/s, not 10, and its gap to its own `.all` is 6.8× (392 against 2,650), not 22× — the same library
version, with only the environment changed. The honest size of the `0.7.18` problem is roughly **8×** against
`mongo4s-cats` (392 against 3,138 on the same machine), and what `0.8.0` did is move that from 8× to **1.95×**.

**It is not fully closed.** `mongo4cats-cats` streams at 64% of its own `findAll` (1,626 against 2,542), where
`mongo4s-cats` streams at 93% of its own (3,174 against 3,420), and it allocates 1.3× its own `findAll` per call
against `mongo4s`'s 1.1×. That is the residual.

## Codec choice under real `MongoDB` — `mongo4s` vs `mongo4cats`

The same `RuntimeBenchmark` also isolates the *codec* dimension: four configs, all on cats-effect, against the same
real `MongoDB` — `mongo4s` with `bson-medeia` vs `bson-direct`, and `mongo4cats` `0.8.0` with `circe` vs `zio-json`. Two
separate runs, same four stacks, two different questions:

```bash
# how many operations per second
sbt "benchmarks/Jmh/run -tu s -f 3 .*RuntimeBenchmark\.cats.* .*RuntimeBenchmark\.mongo4catsCats.*"

# how much garbage each operation generates
sbt "benchmarks/Jmh/run -prof gc -f 2 .*RuntimeBenchmark\.cats.* .*RuntimeBenchmark\.mongo4catsCats.*"
```

**Throughput — operations per second, higher is better**

| Operation | mongo4s+medeia | mongo4s+bson-direct | mongo4cats+circe | mongo4cats+zio-json |
| --- | ---: | ---: | ---: | ---: |
| `insertOne` | **6,044** | **6,044** | 5,999 | 5,982 |
| `insertMany` (10 docs) | 4,978 | **5,041** | 4,871 | 4,807 |
| `findOneById` | 5,033 | **5,120** | 5,068 | 5,064 |
| `findOneByFilter` | 5,547 | **5,552** | 5,539 | 5,528 |
| `findAll` (~100 docs) | 3,420 | **3,706** | 2,542 | 2,914 |
| `findStream` (~100 docs) | 3,174 | **3,436** | 1,626 | 1,777 |
| `updateOne` | 4,700 | 4,770 | 4,783 | **4,870** |
| `deleteOne`\* | 2,824 | **2,825** | 2,802 | 2,808 |
| `count` | **3,872** | 3,663 | 3,697 | 3,773 |

Single-document operations land in the same band, inside the run-to-run error, so **bold** marks the measured
extreme rather than a ranking there — the round trip dominates regardless of codec. The codec and the library show
up once the call moves documents, and the table now says so where the previous revision could not:

* **`bson-direct` leads on bulk reads — `findAll` by 1.08× and `findStream` by 1.08× over `medeia`**, outside the
  error bars on both — the AST-free saving surviving a real round trip, now that the round trip is cheap enough to
  see it.
* **Both `mongo4s` codecs are ahead of both `mongo4cats` codecs on bulk reads.** `findAll`: `medeia` 1.17–1.35× and
  `bson-direct` 1.27–1.46×. `findStream`: `medeia` 1.79–1.95× and `bson-direct` 1.93–2.11×. The spread within each
  library is the codec; the gap between the libraries is the `Bson` wrapper tree and, for `findStream`, the bridge.
* **Everything else is within about 6% of each other with no consistent winner**, `count` and `updateOne` included —
  neither encodes nor decodes an entity.

**Memory allocated per single call, in *KB* — lower is less garbage, not less work done**

| Operation | mongo4s+medeia | mongo4s+bson-direct | mongo4cats+circe | mongo4cats+zio-json |
| --- | ---: | ---: | ---: | ---: |
| `insertOne` | 26.0 | **23.2** | 27.0 | 26.2 |
| `insertMany` (10 docs) | 70.3 | **39.5** | 81.0 | 74.1 |
| `findOneById` | 34.9 | **33.2** | 39.2 | 36.4 |
| `findOneByFilter` | 34.9 | **33.1** | 39.1 | 36.3 |
| `findAll` (~100 docs) | 307.3 | **155.6** | 925.3 | 657.4 |
| `findStream` (~100 docs) | 346.0 | **204.0** | 1233.4 | 969.0 |
| `updateOne` | 24.4 | 24.3 | **24.2** | 24.2 |
| `deleteOne`\* | 48.6 | **45.6** | 49.2 | 48.3 |
| `count` | 31.2 | **30.9** | 31.3 | 31.4 |

Two things worth noting:

* **`bson-direct` allocates less than `bson-medeia` wherever a document actually passes through the codec** — the
  AST-free advantage measured in isolation ([above](#ast-free-wire-codec--all-the-way-to-real-bytes)) survives
  end-to-end through a real driver round trip, not just in a codec microbenchmark. The gap tracks how many documents
  a call encodes or decodes: **11%** for one, **44%** on `insertMany`, and **41–49%** on
  `findAll`/`findStream` — the more documents per call, the more the saved `BsonDocument` tree-walks compound. On
  `updateOne` and `count` the two are level to within 1% (24.41 against 24.30 *KB*, 31.20 against 30.88), which is the
  expected result rather than a surprise: neither operation encodes or decodes an entity, so there is nothing for a
  codec to do differently.
* **`mongo4s` allocates less than `mongo4cats` wherever an entity passes through, and the same where none does.** On
  `updateOne`/`count` all four configurations sit within 2% of each other — the driver's own per-call buffers
  dominate, and both libraries pay them. On `insertOne`/`findOneById`/`findOneByFilter` `mongo4cats` allocates
  **1–18%** more than either `mongo4s` config (`circe` 4–18%, `zio-json` 1–13%), and on `findAll`/`findStream` it
  allocates **2.1–6.0×** *more* — `mongo4cats.bson.BsonValue`, its own wrapper type, adds a full extra tree per
  document on top of `org.bson`'s, and that cost multiplies with document count. `mongo4cats` `0.8.0` did bring
  its own `findStream` down (the previous revision measured 2,573–2,851 *KB*; it is 969–1,233 *KB* now), which is
  the same change as the throughput one in [Streams in `mongo4cats` 0.8.0](#streams-in-mongo4cats-080), and it
  is still 2.8–6.0× `mongo4s`.
