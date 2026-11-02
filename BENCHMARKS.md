# Benchmarks

[← back to the README](README.md)

Nine JMH harnesses in [`benchmarks/`](benchmarks). Every number below was measured on one machine, in one sitting
(2026-11-02), on the `mongo4s` `5.1.0` build, every harness in turn. Some numbers come from other runs, and each
says so where it appears: the before-and-after comparisons, measured A and B interleaved on 2026-11-01 and
2026-11-02; and two comparisons kept from 2026-10-12 on `4.1.0`, `mongo:7` against `mongo:8.2` and `mongo4cats`
`0.7.18` against `0.8.0`, which compare things this release did not touch. The same machine throughout:

| | |
| --- | --- |
| Machine | Apple M4 Max (`Mac16,5`), 14 cores, 36 GB |
| OS | macOS 26.7.2 (`25G332`) |
| JVM | OpenJDK 25.0.4.1 (Homebrew), 64-Bit Server VM, mixed mode, sharing |
| Scala | 3.9.0, sbt 2.0.11, JMH 1.37 |
| Docker | Docker Desktop, engine 29.9.2, Linux VM kernel `7.0.14-linuxkit` |
| Server | `mongo:8.2` (`8.2.12`) in Docker, `localhost:27018`, single node; `mongo:7` (`7.0.40`) where stated |

A run command's `-f` overrides the harness's own fork count; warmup and measurement come from its annotations unless
the command says otherwise — five one-second warmups and ten one-second measurements for the codec, scalar,
error-translation and effect harnesses, three and five of two seconds for the ones that talk to a server.

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
| `zio` | 2.1.27 | the `zio` runtimes, both libraries |
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
| `mongo4s-bson-direct` (via `DocumentCodecBridge`) | ~5.34M | ~3.75M |
| `calypso` (`forProductN`) | **~6.85M** | **~6.69M** |
| `medeia` (`derives`) | ~4.33M | ~5.27M |
| `zio-bson` (`zio-schema` derived) | ~2.30M | ~4.50M |
| `mongo4cats-zio-json` | ~1.85M | ~1.92M |
| `mongo4cats-circe` | ~2.68M | ~1.97M |

Every `mongo4s` codec bridge beats both `mongo4cats` codecs on decode, by **1.9×** at the narrowest
(`bson-direct` against `circe`) and **3.5×** at the widest (`calypso` against `zio-json`) — the cost of
`case class ↔ circe/zio-json ↔ mongo4cats.Bson ↔ org.Bson` instead of straight to `org.bson`. On encode the
picture is less one-sided: `calypso`'s handwritten `forProductN` is **1.28×** the next backend (`bson-direct`), which is
what skipping derivation buys, but `mongo4cats-circe` encodes faster than `zio-bson` here (**1.17×**) and the
two `mongo4cats` encoders are 1.45× apart. Run-to-run error is ±0.2–4.2% across the table, narrow enough that every
neighbouring pair is separated by it.

`bson-direct` is in this table through `DocumentCodecBridge.toDocumentCodec` — the path `aggregate` takes on a
direct collection when the caller hands it that bridge, `WireCodec` forced to materialize the `BsonDocument` it
normally skips. Handicapped that way it is a middling backend rather than a fast one: **1.23×** `medeia` on encode,
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
| hand-written, straight to the wire | ~4.47M ops/s | 1824 B/op |
| **`WireCodec`** | **~3.66M ops/s** | **1856 B/op** |
| `calypso` | ~2.64M ops/s | 3224 B/op |
| `medeia` | ~2.14M ops/s | 4592 B/op |
| `zio-bson` | ~1.34M ops/s | 4675 B/op |
| `mongo4cats-circe` | ~1.32M ops/s | 6320 B/op |
| `mongo4cats-zio-json` | ~1.03M ops/s | 5461 B/op |

**Decode — BSON bytes → case class**

| Backend | Throughput | Alloc |
| --- | ---: | ---: |
| **`WireCodec`** | **~2.47M ops/s** | **1184 B/op** |
| hand-written, straight to the wire | ~2.37M ops/s | 1088 B/op |
| `calypso` | ~1.75M ops/s | 3193 B/op |
| `zio-bson` | ~1.58M ops/s | 2752 B/op |
| `medeia` | ~1.55M ops/s | 2768 B/op |
| `mongo4cats-zio-json` | ~1.03M ops/s | 7352 B/op |
| `mongo4cats-circe` | ~985k ops/s | 8040 B/op |

Three things fall out of this.

`WireCodec` **wins both directions against every AST backend** — ahead of the best of them, `calypso`, by
**1.38×** on encode and **1.42×** on decode, and ahead of both `mongo4cats` codecs by **2.4–3.5×**. It allocates
**2.5×** less than `medeia` on encode, **2.3×** less on decode, and **6.8×** less than `mongo4cats-circe` on decode.

**Derivation is not free, and the hand-written row is the ceiling it is measured against.** A codec written by hand
straight into the `BsonWriter` encodes **1.22×** faster than the derived one and allocates **2%** less; on decode
the derived one is 4% ahead and allocates 96 bytes more. The encode gap is what `derives WireCodec` costs, and it is
the honest upper bound for the AST-free path — not a reason to hand-write one, but worth knowing it exists.

The `AST` backends rank `calypso`, `medeia`, `zio-bson` on encode, and `calypso` first on decode with `zio-bson` and
`medeia` level behind it, their intervals touching. Every one of them is on the far side of a gap that comes from
building an intermediate tree at all, not from how well it is built.

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
| Encode | **~4.44M ops/s**, 1800 B/op | ~3.82M ops/s, 2064 B/op |
| Decode | **~3.45M ops/s**, 1208 B/op | ~2.31M ops/s, 1568 B/op |

Decode is where it shows: **1.49×** the throughput and **23%** less garbage, four fields out of seven. Encode gains
much less (**1.16×**, 13% less garbage) because building a `BsonDateTime` or a `BsonObjectId` on the way out is cheap;
reading one back means allocating it *and* walking the `BsonDecoder`'s type checks.

Both columns write byte-identical BSON — the bridge is not a different format, only a slower way to the same bytes.

## A consumer-shaped entity — wide, optional, nested

The entity above has seven fields, none of them optional, no sum and no map. The services this library was written
for store something else: [`WideEntityBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/WideEntityBenchmark.scala)
is modelled on the hottest of their writes — twenty fields with some `Option`s left `None`, a sum for the event's
status, a list of nested periods, an `Instant` and a `BigDecimal`, and a `Map` keyed by an enum holding three
twenty-six-field scores, all under `snakeCase`. It measures the two ways such an entity reaches bytes: `WireCodec`
straight to the wire, and the same codec bridged through a `BsonDocument`, which is the path a repository built with
`create` or `withoutId` over `DocumentCodecBridge` runs.

```bash
sbt "benchmarks/Jmh/run -f 2 -i 5 -wi 3 -r 2 -w 1 -prof gc WideEntityBenchmark"
```

| Direction | `WireCodec`, direct | `WireCodec` through a `BsonDocument` |
| --- | ---: | ---: |
| Encode | **~307k ops/s**, 5848 B/op | ~195k ops/s, 13552 B/op |
| Decode | **~254k ops/s**, 10680 B/op | ~182k ops/s, 19112 B/op |

On an entity this size the direct path is **1.57×** on encode and **1.40×** on decode, with **57%** and **44%** less
garbage. That gap is what moving a repository from `withoutId` over a bridged codec to `BaseMongoRepository.direct`
buys, before the network round trip, which on a single write is still the larger cost.

Decoding this entity got cheaper in `5.1.0`: a derived product hands its fields to the constructor straight from the
array it read them into, rather than copying them into a tuple first, and a sum whose `_type` comes first reads its
case without marking and rewinding the document. Measured A and B interleaved, twice, because this laptop drifts by
a few percent between runs minutes apart:

| `wireDecode` | `5.0.1` | `5.1.0` |
| --- | ---: | ---: |
| first pair | ~254k ops/s, 11272 B/op | **~282k ops/s**, 10680 B/op |
| second pair | ~265k ops/s, 11272 B/op | **~281k ops/s**, 10680 B/op |

About **1.09×**, and 592 bytes less per document — one tuple for each of the seven products in it, the
twenty-six-field scores' the largest. `wireEncode` ran alongside as
the control, its code untouched, and moved by less than 1%.

## Rendering a query

A `Filter` or an `Update` is rendered once per query, not once per document, so it rarely shows; it does on a bulk
write, where every model renders its own. [`RenderBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/RenderBenchmark.scala)
renders a five-condition filter and a ten-operator update, every field renamed by `snakeCase`, run with
`sbt "benchmarks/Jmh/run -f 2 -i 5 -wi 3 -r 2 -w 1 -prof gc RenderBenchmark"`. The two columns are
`5.0.1` and `5.1.0`, which renames without boxing a `Char` per letter and checks an update's paths for overlap without
splitting every path into arrays, measured side by side on 2026-11-01:

| | `5.0.1` | `5.1.0` |
| --- | ---: | ---: |
| Filter | ~2.56M ops/s, 2968 B/op | **~3.89M ops/s**, 2840 B/op |
| Update | ~600k ops/s, 8048 B/op | **~1.47M ops/s**, 3808 B/op |

A filter on a type that has only a `ScalarWireCodec` — an opaque id, say — renders its value through that codec's
`BsonEncoder`, and the same harness measures an `$in` over a thousand of them against one over a thousand plain
strings. `5.0.1` cannot render the first at all, since a `ScalarWireCodec` did not yet answer as a `BsonEncoder`, so
the comparison is between the `5.1.0` commit that taught it to and the one after, which reuses one capturing writer
per thread instead of building one per value, A and B interleaved:

| `$in` over 1,000 values | before the reuse | `5.1.0` |
| --- | ---: | ---: |
| opaque ids, through a `ScalarWireCodec` | ~53k ops/s, 232544 B/op | **~112k ops/s**, 48544 B/op |
| plain strings, the control | ~158k ops/s, 48512 B/op | ~155k ops/s, 48512 B/op |

**2.1×**, and the opaque ids allocate what the strings do, to within 32 bytes over a thousand values; they render at
0.72× the strings' rate.

## Typed errors — what the translation costs

Since `3.0.0` every `Publisher` the driver hands back is wrapped so that failures arrive as `MongoError` rather than
as driver exceptions. That wrapper sits on the hot path of every operation and every stream, one extra `Subscriber`
per subscription and one extra virtual call per element, so it is worth knowing what it costs when nothing fails.

[`ErrorTranslationBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/ErrorTranslationBenchmark.scala) drains a
synchronous publisher of *n* elements with and without the wrapper — no server, no codec, nothing but the bridge:

```bash
sbt "benchmarks/Jmh/run -f 3 -prof gc ErrorTranslationBenchmark"
```

| Elements | Raw publisher, ops/µs | Through `translating`, ops/µs | Cost | Alloc |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 623.0 ± 0.6 | 614.9 ± 1.4 | 1.3% | 24 → 24 B/op |
| 100 | 7.88 ± 0.034 | 7.65 ± 0.057 | 3.0% | 24 → 24 B/op |
| 10,000 | 0.0636 ± 0.0003 | 0.0611 ± 0.0003 | 3.8% | 24 → 40 B/op |

**The wrapper costs a few percent at most**: 1.3% at one element, 3.0% at a hundred and 3.8% at ten thousand. Its own
allocation is one object per *subscription* — not per element — which is why the per-operation figure is unchanged
until the 10,000-element case, where it is 16 B on a 24 B baseline. Typed errors cost a few percent at most on the
happy path, and nothing that grows with the stream.

## The `Effect` instances — what a step costs

Everything `mongo4s` builds runs as steps in the caller's effect, composed through that runtime's `Effect` instance:
a `flatMap` for each step, a `bracket` for each session, a `guaranteeCase` for each transaction's commit or abort.
[`EffectBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/EffectBenchmark.scala) measures those three on their
own, with no server, no driver and no codec — a chain of a thousand `flatMap`s, a thousand `bracket`s in a row, a
thousand `guaranteeCase`s in a row — each program run to its end on the runtime's own runner:

```bash
sbt "benchmarks/Jmh/run -f 2 -prof gc EffectBenchmark"
```

**Per step — each program's time and allocation divided by its thousand steps**

| Step | `cats` | `zio` | `kyo` | `rapid` |
| --- | ---: | ---: | ---: | ---: |
| `flatMap` | 23 ns, 81 B | 15 ns, 119 B | **12 ns, 16 B** | 31 ns, 175 B |
| `bracket` | 246 ns, 895 B | **190 ns**, 933 B | 2,474 ns, 6,291 B | 228 ns, 1,620 B |
| `guaranteeCase` | **150 ns, 577 B** | 172 ns, 775 B | 1,597 ns, 4,588 B | 185 ns, 1,223 B |

The runner's own start-up is in each figure once per program, a thousandth of it per step. Error is ±0.1–5.5%.

**A `flatMap` is tens of nanoseconds on every runtime**, against a server round trip of 130,000 — what a repository
method composes is not where its time goes. **`kyo`'s `bracket` and `guaranteeCase` cost about ten times the
others'**, 2.5 µs and 1.6 µs, and 4.6–6.3 KB. Its `guaranteeCase` is a `kyo` `Scope` with the finalizer ensured on it,
and its `bracket` adds an uninterruptible acquire and a handshake that still releases what was acquired when an
interrupt lands mid-acquire. Either is paid once per session or transaction, not per document: about 2% of one round
trip.

## Talking to a server

The sections that follow all cross a socket to `MongoDB`, and the first thing to know is how long a round trip is.
`MongoDB` runs in Docker Desktop, which on macOS is a Linux VM. The floor was measured directly, from a shell **inside
the container** with no port mapping in the path at all (2,000 operations per line, `mongosh`):

```
empty loop  0.000 ms/op
ping        0.137 ms/op → ~7,300 ops/s
findOne     0.128 ms/op → ~7,800 ops/s
```

An empty `mongosh` loop is free, so that 0.13 ms is the server round trip itself. From the host, through the port
mapping, `mongo4s` does single-document work at **4,700–6,000 ops/s** — 0.17–0.21 ms an operation, within about
0.04–0.08 ms of the bare shell's floor — so the round trip is still most of what a single-document call costs, which
is why those operations come out level in the tables below. It is no longer all of it.

**That is a different environment from the earlier revisions of this page, and the difference is not the library.**
They recorded a floor of 2.40 ms and around **250 ops/s** for the same single-document operations, on the same
class of hardware. The cleanest evidence that it was the environment: `mongo4cats` `0.7.18` — a library that has
not changed — measured `insertOne` at 256 ops/s then and at **5,970** ops/s on 2026-10-12, on the same benchmark.
Docker Desktop is what moved, ~23×; this page does not try to say which part of it. Read the absolute rates here as
belonging to *this* environment, and the ratios as the thing that travels.

A latency-bound measurement compresses every ratio, because the constant round trip is a larger share of each
result; at 0.13 ms that compression is much milder than at 2.4 ms. The earlier revision said the real gap on a
cheaper round trip would be wider than the one it showed. It is — the aggregation gap in the next section went from
1.37× to 1.42× over ten thousand documents and from 1.19× to 1.23× against `medeia`.

The codec sections above have no server in them and are unaffected.

**How much the machine alone moves these numbers**, measured rather than guessed: the 36 operations in the
`RuntimeBenchmark` throughput run were measured twice, an hour apart, once as the throughput run (`-f 3`) and once
beside the allocation profile (`-f 2`) — different forks, a different JMH session, the same container. The median
difference is **0.5%**, 28 of the 36 are within 1% and 32 within 2%, and the worst are `count` (5.0% and 6.6%, the
noisiest row) and `mongo4cats`'s `findAll` through `circe` (7.0%). Any cross-run comparison narrower than that is
reading noise; the tables below are far wider apart than that, except where they say otherwise.

Which is why the server versions were compared by **interleaving** them — `7`, `8.2`, `7`, `8.2`, `7`, `8.2`, each
a fresh container and a fresh JMH run of `AggregateBenchmark` — rather than by running one after the other. That
comparison is from 2026-10-12, on `4.1.0`: it compares two servers, not two builds of this library. Done that way the
difference reproduces in all three rounds, and goes both ways, as it did on the revision before it: `8.2`
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
| 10 | 5,527 ops/s | 5,516 ops/s | 5,472 ops/s |
| 10000 | **99 ops/s** | 70 ops/s | 81 ops/s |

**Both rows are the point.** At ten documents the three are indistinguishable — they sit inside each other's
error bars (±3–3.5%), because the round trip and the server's work are everything and the codec is noise, which is the
honest answer for most CRUD. At ten thousand the decode starts to tell instead, and `aggregateDirect` is
**1.42×** the bridged path and **1.23×** `medeia`, *including* the network round trip — outside the error bars on
both.

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

**Throughput — operations per second, higher is better.** The `mongo4s` columns read through `getCollection` with
the `medeia` bridge, the `mongo4cats` columns through its circe codec; the workload is otherwise the same.

| Operation | mongo4s-cats | mongo4s-zio | mongo4s-rapid | mongo4s-kyo | mongo4cats-cats | mongo4cats-zio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `insertOne` | 5,879 | 5,279 | **6,378** | 5,885 | 5,915 | 6,022 |
| `find(filter).all` (~100 docs) | 3,333 | 3,022 | 3,203 | **3,344** | 2,445 | 2,704 |
| `find(filter).stream` (~100 docs) | 3,109 | 3,029 | **3,262** | 3,221 | 1,471 | 2,192 |
| `updateOne` | 4,713 | 4,281 | **5,011** | 4,716 | 4,725 | 4,774 |
| `count(filter)` | 3,860 | 3,494 | 3,849 | **3,879** | 3,676 | 3,686 |

On single-document operations the stacks land in one band — within 9% of each other, `zio` aside — and the round
trip is why. **Bold** is the highest cell in each row; read it as the measured extreme rather than a ranking where
the cells are within error (run-to-run error is ±0.6–10.5% across this table). Two things do separate:

* **`mongo4s-zio` sits below the rest on every row — 3–10% under `mongo4s-cats`** — and `mongo4s-rapid` is ahead of
  the field on `insertOne` and `updateOne` (8.5% and 6.3% over `cats`), as it is by 7–8% on the four single-document
  operations this table leaves out (`insertMany`, `findOneById`, `findOneByFilter`, `deleteOne`). Neither is large,
  but both are outside the error bars, the `zio` gap on every row but the stream.
* **On bulk reads `mongo4cats` is behind.** On the same effect system, `find(filter).all` is **1.36×** faster through
  `mongo4s` on `cats` and **1.12×** on `zio`, and `find(filter).stream` is **2.11×** (`cats`) and **1.38×** (`zio`)
  faster — outside the error bars on all four comparisons.


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
(the benchmarks as they were before the upgrade) and once with `0.8.0`, on 2026-10-12:

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

**The old 10 ops/s overstated the problem, because the environment amplified it.** `0.7.18` in the 2026-10-12
environment streams at ~390 ops/s, not 10, and its gap to its own `.all` is 6.8× (392 against 2,650), not 22× — the
same library version, with only the environment changed. The honest size of the `0.7.18` problem is roughly **8×**
against `mongo4s-cats` (392 against 3,138 on the same machine), and what `0.8.0` did is move that from 8× to about
**2×**.

**It is not fully closed.** `mongo4cats-cats` streams at 60% of its own `findAll` (1,471 against
2,445), where `mongo4s-cats` streams at 93% of its own (3,109 against 3,333), and it allocates 1.33× its own `findAll`
per call against `mongo4s`'s 1.16×. That is the residual.

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
| `insertOne` | 5,879 | **5,959** | 5,915 | 5,907 |
| `insertMany` (10 docs) | 4,879 | **4,963** | 4,823 | 4,761 |
| `findOneById` | 4,947 | 4,991 | **5,019** | 4,988 |
| `findOneByFilter` | 5,438 | **5,486** | 5,455 | 5,464 |
| `findAll` (~100 docs) | 3,333 | **3,626** | 2,445 | 2,907 |
| `findStream` (~100 docs) | 3,109 | **3,386** | 1,471 | 1,664 |
| `updateOne` | 4,713 | 4,722 | 4,725 | **4,776** |
| `deleteOne`\* | 2,768 | 2,778 | **2,778** | 2,772 |
| `count` | **3,860** | 3,851 | 3,676 | 3,673 |

Single-document operations land in the same band, inside the run-to-run error, so **bold** marks the measured
extreme rather than a ranking there — the round trip dominates regardless of codec. The codec and the library show
up once the call moves documents, and the table now says so where the previous revision could not:

* **`bson-direct` leads on bulk reads — `findAll` by 1.09× and `findStream` by 1.09× over `medeia`**, outside the
  error bars on both — the AST-free saving surviving a real round trip, now that the round trip is cheap enough to
  see it.
* **Both `mongo4s` codecs are ahead of both `mongo4cats` codecs on bulk reads.** `findAll`: `medeia` 1.15–1.36× and
  `bson-direct` 1.25–1.48×. `findStream`: `medeia` 1.87–2.11× and `bson-direct` 2.03–2.30×. The spread within each
  library is the codec; the gap between the libraries is the `Bson` wrapper tree and, for `findStream`, the bridge.
* **Everything else is within about 5% of each other**, `count` and `updateOne` included — neither encodes nor
  decodes an entity. `count` has both `mongo4s` configurations 5% ahead of both `mongo4cats` ones, but the intervals
  touch; nothing else has a consistent winner.

**Memory allocated per single call, in *KB* — lower is less garbage, not less work done**

| Operation | mongo4s+medeia | mongo4s+bson-direct | mongo4cats+circe | mongo4cats+zio-json |
| --- | ---: | ---: | ---: | ---: |
| `insertOne` | 26.1 | **23.2** | 26.7 | 26.3 |
| `insertMany` (10 docs) | 70.7 | **41.2** | 81.1 | 74.1 |
| `findOneById` | 35.3 | **32.8** | 39.1 | 36.4 |
| `findOneByFilter` | 34.7 | **33.1** | 39.0 | 36.4 |
| `findAll` (~100 docs) | 310.3 | **152.5** | 926.9 | 652.2 |
| `findStream` (~100 docs) | 360.5 | **202.1** | 1236.4 | 970.6 |
| `updateOne` | 23.9 | **23.8** | 24.2 | 24.3 |
| `deleteOne`\* | 48.4 | **45.4** | 49.1 | 48.1 |
| `count` | **31.2** | 31.3 | 31.3 | 31.3 |

Two things worth noting:

* **`bson-direct` allocates less than `bson-medeia` wherever a document actually passes through the codec** — the
  AST-free advantage measured in isolation ([above](#ast-free-wire-codec--all-the-way-to-real-bytes)) survives
  end-to-end through a real driver round trip, not just in a codec microbenchmark. The gap tracks how many documents
  a call encodes or decodes: **5–11%** for one, **42%** on `insertMany`, and **44–51%** on
  `findAll`/`findStream` — the more documents per call, the more the saved `BsonDocument` tree-walks compound. On
  `updateOne` and `count` the two are level to within 1% (23.91 against 23.82 *KB*, 31.24 against 31.28), which is the
  expected result rather than a surprise: neither operation encodes or decodes an entity, so there is nothing for a
  codec to do differently.
* **`mongo4s` allocates less than `mongo4cats` wherever an entity passes through, and the same where none does.** On
  `updateOne`/`count` all four configurations sit within 2% of each other — the driver's own per-call buffers
  dominate, and both libraries pay them. On `insertOne`/`findOneById`/`findOneByFilter` `mongo4cats` allocates
  **1–19%** more than either `mongo4s` config (`circe` 2–19%, `zio-json` 1–13%), and on `findAll`/`findStream` it
  allocates **2.1–6.1×** *more* — `mongo4cats.bson.BsonValue`, its own wrapper type, adds a full extra tree per
  document on top of `org.bson`'s, and that cost multiplies with document count. `mongo4cats` `0.8.0` did bring
  its own `findStream` down (the revision before last measured 2,573–2,851 *KB* on `0.7.18`; it is 971–1,236 *KB*
  now), which is the same change as the throughput one in [Streams in `mongo4cats` 0.8.0](#streams-in-mongo4cats-080),
  and it is still 2.7–6.1× `mongo4s`.
