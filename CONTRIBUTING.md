# Contributing to mongo4s

Thanks for taking the time to contribute! This document covers what you need to build the project,
run its tests, and where to make changes for common kinds of contributions.

## Building

Tested against JDK 25 (LTS) — that's what CI uses. Other recent JDKs likely work too, but JDK 25 is
the one to reach for if something looks version-specific.

The project is built with [sbt](https://www.scala-sbt.org/). Modules are split across two Scala 3
versions — most of the codebase targets the LTS release, while `kyo`, `rapid`, `bson-calypso`, and
anything that depends on them (`repositories-tests`, `examples`, `benchmarks`, `it`) target the latest
Scala 3 release instead. sbt handles this transparently; you don't need to do anything special.

```bash
sbt compile          # compile everything
sbt test             # run unit tests for the aggregated modules (bson, core, runtime, repositories)
sbt scalafmtCheckAll  # verify formatting
sbt scalafmtAll        # fix formatting
```

To run a single module's tests:

```bash
sbt core/test
sbt "repositoriesTests/testOnly mongo4s.repositories.CodecRepositorySpec"
```

## Project layout

| Path | What lives there |
| --- | --- |
| `bson/core` | `BsonEncoder`/`BsonDecoder`/`BsonDocumentCodec` — the scalar + document codec seam every bridge targets |
| `bson/medeia`, `bson/zio`, `bson/calypso` | bridges from a third-party codec library into `mongo4s.bson.BsonDocumentCodec` |
| `bson/direct` | `WireCodec` — mongo4s's own AST-free codec, no third-party dependency |
| `core` | `MongoClient`/`MongoDatabase`/`MongoCollection`, `Field`/`Filter`/`Update`/`PrimaryKey`/`WithId` — runtime- and codec-agnostic |
| `runtime/cats`, `runtime/zio`, `runtime/kyo`, `runtime/rapid` | `given Effect[F]` + `given RsBridge[F, S]` per runtime |
| `repositories` | `BaseMongoRepository`, `Repository`, `Page` |
| `testkit` | `FakeMongoCollection` and `FakeRepository`, published so consumers can unit-test against them too |
| `repositories-tests` | cross-cutting tests that need more than one runtime or codec bridge in scope at once (hence pinned to the latest Scala 3, so `kyo`/`rapid`/`calypso` can all be included) |
| `it` | tests against a real MongoDB via [Testcontainers](https://testcontainers.com/) — needs Docker |
| `examples` | runnable end-to-end examples, one per runtime/codec combination |
| `benchmarks` | JMH benchmarks comparing runtimes, codecs, and mongo4s against `mongo4cats` |

## Adding a new BSON codec bridge

A bridge only needs to produce `mongo4s.bson.BsonEncoder[A]` / `BsonDecoder[A]` for scalars, and
`BsonDocumentCodec[A]` for whole documents, from whatever typeclasses the target library already
exposes — see `bson/medeia`, `bson/zio`, or `bson/calypso` for the pattern. Each bridge module also
gets a `domain.scala` exposing `type`/`val` aliases for the target library's own types (e.g.
`mongo4s.bson.calypso.CalypsoEncoder`), so users importing `mongo4s.bson.<bridge>.*` never need to
write `import some.library.Encoder as ...` themselves.

## Adding a new runtime backend

A runtime backend needs a `given Effect[F]` (`pure`/`delay`/`map`/`flatMap`/`raiseError`/
`handleErrorWith`/`guaranteeCase` — those seven are the abstract members; everything else has a
default and may be overridden) and a `given RsBridge[F, S]` (`one`/`option`/`list`/`unit`/`stream`,
bridging a `org.reactivestreams.Publisher[A]` into `F[A]`/`S[A]`) — see `runtime/cats` for the
reference implementation.

**Override `bracketCase` if the runtime has a real bracket primitive.** Its default is
`flatMap(acquire)(a => guaranteeCase(use(a))(…))`, which registers the finalizer only *after*
acquisition has already returned — a fiber cancelled in that window drops the resource. `cats` and
`zio` override it with primitives that acquire uninterruptibly; `kyo` and `rapid` do not, because
their own bracket helpers (`Sync.acquireReleaseWith`, and rapid's `guarantee`) are built the same
`acquire.map(register)` way, so overriding would buy nothing. Check what the runtime actually offers
before assuming the default is safe there. If the runtime can't derive a `Tag`-like witness for a fully generic type parameter
(as with kyo), gate `stream` behind `mongo4s.Streamable[S, A]` rather than throwing — see
`runtime/kyo/KyoBridgeInstance.scala` for how that constraint is satisfied and threaded through.

## Tests

- Unit tests for `core` and the repository layer run against `FakeMongoCollection`, an in-memory
  implementation interpreting the same `Filter`/`Update` AST the real driver does - no MongoDB
  required. The repository specs live in `repositories-tests` rather than `repositories`, because
  `testkit` depends on `repositories` and a test dependency the other way would be a cycle.
- `it/` and the `*DirectRepositoryItSpec`/`RepositoryItSpec` families run against a real MongoDB
  started via Testcontainers - you need Docker running locally to execute these
  (`sbt it/test`).
- New runtime backends or codec bridges should be exercised through the shared spec traits
  (`RepositoryBackendSpec` in `repositories-tests`, `DirectRepositoryItSpec` in `it`) rather than
  one-off tests, so they get the same coverage as every existing backend for free.

## Code style

Formatting is enforced by [scalafmt](https://scalameta.org/scalafmt/) (`.scalafmt.conf`) and checked
in CI. Run `sbt scalafmtAll` before opening a PR.

## Releasing

Two steps are easy to forget because nothing fails when they are missed:

- **Move the MiMa baseline.** `binaryCompatibleWith` in `build.sbt` names the releases the artifacts are checked
  against. It is `Set.empty` while a major version is being prepared, because there is nothing compatible to compare
  to; the moment that version is on Maven Central it must become `Set("<that version>")`, or the compatibility
  promise is asserted and never checked.
- **Record any deliberate break.** A break inside a major version needs a filter in `mima.sbt` and an entry in
  [COMPATIBILITY.md](COMPATIBILITY.md). A major release needs no filters, but its migration guide still belongs
  there.
- **Add the release to [CHANGELOG.md](CHANGELOG.md)**, in Added / Changed / Fixed order. The GitHub release notes
  can be the same content in prose; the changelog is the version that stays with the source.

## Opening a pull request

- Keep PRs focused - one runtime backend, one codec bridge, or one bug fix per PR is easier to review
  than a mix.
- Add or extend tests for anything behavioral you change.
- `sbt scalafmtCheckAll test` should pass locally before you push (integration tests need Docker and
  aren't required for most changes, but CI will run them too).
