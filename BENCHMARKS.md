# Benchmarks

[← back to the README](README.md)

Three separate JMH harnesses in [`benchmarks/`](benchmarks), one developer machine — `JDK 25`, `MongoDB 7` — directional
ballparks, not hardware-independent authorities. Run them on your own hardware before making decisions on the numbers
alone.

## Codec backends — to `org.bson.BsonDocument`

[`CodecBenchmark`](benchmarks/src/main/scala/mongo4s/benchmarks/CodecBenchmark.scala) encodes/decodes a mid-size
entity (seven fields, one nested object) through each backend, straight to/from `org.bson.BsonDocument`, against
`mongo4cats`'s own codec modules (`mongo4cats-circe`, `mongo4cats-zio-json`):

```bash
sbt "benchmarks/Jmh/run -f 6 mongo4s.benchmarks.CodecBenchmark"
```

| Codec | Encode ops/s | Decode ops/s |
| --- | ---: | ---: |
| `mongo4s-bson-direct` (via `DocumentCodecBridge`) | ~5.64M | ~4.19M |
| `calypso` (`forProductN`) | **~8.12M** | **~6.65M** |
| `medeia` (`derives`) | ~2.76M | ~5.97M |
| `zio-bson` (`zio-schema` derived) | ~1.86M | ~5.20M |
| `mongo4cats-zio-json` | ~2.02M | ~1.66M |
| `mongo4cats-circe` | ~1.95M | ~1.33M |

Every `mongo4s` codec bridge beats both `mongo4cats` codecs by **2.5–5×** on decode — the cost of
`case class ↔ circe/zio-json ↔ mongo4cats.Bson ↔ org.Bson` instead of straight to `org.bson`. On encode `calypso`'s
handwritten `forProductN` is **1.4×** the next backend, which is what skipping derivation buys; it leads on decode too,
though that is the one number here with real spread (±5.3% within the run and ~5% between runs, against ±0.5% for
`medeia`).

`bson-direct` is in this table through `DocumentCodecBridge.toDocumentCodec` — the path `aggregate` and `distinct`
take on a direct collection, `WireCodec` forced to materialize the `BsonDocument` it normally skips. Even handicapped
that way it encodes **2×** *faster* than `medeia`; on decode it falls behind `medeia` and `zio-bson`, which read the
`BsonDocument` natively instead of through a `BsonDocumentReader`. Its AST-free numbers are in the next
table — but note the two tables stop in different places, this one at a `BsonDocument` and the next at real bytes, so
rows are not comparable across them. What the intermediate tree costs shows up *inside* the next table, where every
backend is measured to the same endpoint.

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
| **`WireCodec`** | **~2.91M ops/s** | **1895 B/op** |
| `calypso` | ~2.01M ops/s | 3224 B/op |
| `medeia` | ~1.22M ops/s | 4672 B/op |
| `zio-bson` | ~995k ops/s | 5020 B/op |
| `mongo4cats-circe` | ~907k ops/s | 6173 B/op |
| `mongo4cats-zio-json` | ~876k ops/s | 5480 B/op |

**Decode — BSON bytes → case class**

| Backend | Throughput | Alloc |
| --- | ---: | ---: |
| **`WireCodec`** | **~2.65M ops/s** | **1248 B/op** |
| `medeia` | ~1.62M ops/s | 3168 B/op |
| `zio-bson` | ~1.53M ops/s | 2800 B/op |
| `calypso` | ~1.49M ops/s | 3311 B/op |
| `mongo4cats-zio-json` | ~792k ops/s | 6008 B/op |
| `mongo4cats-circe` | ~704k ops/s | 8552 B/op |

Two things fall out of this.

`WireCodec` **wins both directions** — ahead of the best `AST` backend by **1.45×** on encode (`calypso`) and **1.64×**
on decode (`medeia`), and ahead of both `mongo4cats` codecs by **3.2–3.8×**. It allocates **2.5×** *less* than `medeia`
in
both directions, and up to **6.9×** *less* than `mongo4cats-circe` on decode.

The `AST` backends do not rank the same in both directions — `calypso` leads on encode and trails on decode, `medeia`
is the reverse — but every one of them is on the far side of a gap that comes from building an intermediate tree at
all, not from how well it is built.

In a typical CRUD workload the network round trip dwarfs all of this (see below); it matters for bulk paths — large
cursor streams, ETL, aggregation over big result sets.

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
| `insertOne` | 2469 | 2118 | **2588** | 2469 | 2465 | 2550 |
| `find(filter).all` (~100 docs) | **1584** | 1406 | 1581 | 1563 | 1339 | 1369 |
| `find(filter).stream` (~100 docs) | 1446 | 1393 | **1559** | 1528 | 393 | 1215 |
| `updateOne` | 2155 | 1960 | 2257 | 2189 | 2199 | **2300** |
| `count(filter)` | 1958 | 1768 | 2035 | 1995 | 1969 | **2051** |

With a clean database on every trial, all six columns land within the same band for every operation — the `TCP` round
trip to `MongoDB` dominates at this scale, and none of the six `RsBridge`/collection wrappers stands out. **Bold** is the
highest cell in each row, but read it as the measured extreme rather than a ranking: run-to-run error is ±3–15%,
wider than the spread between the columns. The one real outlier is `mongo4cats-cats`'s `find(filter).stream` (**393**
*ops/s*
against its own `.all`'s **1339**, and against **~2200** on its own single-document ops) — it bridges through a
hand-rolled
`cats.effect.std.Queue`-backed `Subscriber` instead of `fs2.interop.reactivestreams`, which every `mongo4s` `.stream()`
uses. Full table and methodology notes are in the benchmark source.

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
| `insertOne` | **2449** | 2419 | 2414 | 2417 |
| `insertMany` (10 docs) | **2177** | 2146 | 2173 | 2134 |
| `findOneById` | 2213 | 2202 | **2224** | 2200 |
| `findOneByFilter` | **2337** | 2302 | 2290 | 2293 |
| `findAll` (~100 docs) | 1565 | **1632** | 1328 | 1308 |
| `findStream` (~100 docs) | 1438 | **1465** | 397 | 397 |
| `updateOne` | 2177 | 2174 | 2141 | **2188** |
| `deleteOne`\* | **1160** | 1152 | 1134 | 1108 |
| `count` | **1965** | 1956 | 1961 | 1925 |

Same finding as the [runtime table above](#runtime-overhead--real-mongodb-every-backend): every operation but the
bulk reads lands in the same band, inside the ~±5–15% run-to-run error, so **bold** marks the measured extreme rather
than a ranking — the
network round trip dominates regardless of codec. `findAll`/`findStream` are where `mongo4s` pulls ahead, and that is
the decode path rather than the bridge. The one outlier is `mongo4cats`' `find(filter).stream`, **~5.5×** _slower_ than
its
own single-document operations *and independent of codec* (397 ops/s for circe and 397 for zio-json — the same
number to the unit) — confirms it's the runtime's `Queue`-backed `Subscriber` bridge, not the codec,
exactly as found earlier.

**Memory allocated per single call, in *KB* — lower is less garbage, not less work done**

| Operation | mongo4s + medeia | mongo4s + bson-direct | mongo4cats + circe | mongo4cats + zio-json |
| --- | ---: | ---: | ---: | ---: |
| `insertOne` | 23.8 | **21.2** | 24.6 | 24.0 |
| `insertMany` (10 docs) | 63.6 | **37.7** | 79.1 | 72.8 |
| `findOneById` | 35.1 | **33.0** | 38.5 | 35.7 |
| `findOneByFilter` | 35.0 | **32.8** | 38.4 | 35.7 |
| `findAll` (~100 docs) | 361.7 | **155.4** | 923.2 | 659.1 |
| `findStream` (~100 docs) | 409.4 | **205.4** | 3069.7 | 2613.4 |
| `updateOne` | 21.6 | **21.5** | 21.9 | 21.8 |
| `deleteOne`\* | 44.3 | **41.7** | 44.1 | 43.6 |
| `count` | 31.1 | 31.1 | 30.8 | **30.8** |

Two things worth noting:

* **`bson-direct` allocates less than `bson-medeia` wherever a document actually passes through the codec** — the
  AST-free advantage measured in isolation ([above](#ast-free-wire-codec--all-the-way-to-real-bytes)) survives
  end-to-end through a real driver round trip, not just in a codec microbenchmark. The gap tracks how many documents
  a call encodes or decodes: **~6–11%** for one, **41%** on `insertMany`, **50–57%** on `findAll`/`findStream` — the more
  documents per call, the more the saved `BsonDocument` tree-walks compound. On `updateOne` and `count` the two are
  identical to three digits (**21.52** against **21.57** *KB*, **31.12** against **31.13**), which is the expected
  result rather than a
  surprise: neither operation encodes or decodes an entity, so there is nothing for a codec to do differently.
* **`mongo4s` now matches `mongo4cats` on single-document ops, and stays far lighter on bulk reads.** On
  `insertOne`/`updateOne`/`deleteOne`/`count`/`findOneById` all four configurations sit within a couple of percent
  of each other — the driver's own per-call buffers dominate, and both libraries pay them. On `findAll`/`findStream`
  `mongo4cats` allocates **2.6–15×** *more* than either `mongo4s` config — `mongo4cats.bson.BsonValue`, its own wrapper
  type,
  adds a full extra tree per document on top of `org.bson`'s, and that cost multiplies with document count. This is
  a change from the pre-1.0 numbers, where `mongo4s` was the heavier of the two on every single-document operation.

