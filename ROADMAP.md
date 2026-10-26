# Roadmap

[← back to the README](README.md)

What is deliberately not in `mongo4s` yet, and why. Nothing here is a known defect — these are gaps in coverage of
the MongoDB surface, each with a workaround available today.

Two escape hatches soften most of them. `Stage.raw`, `Filter.Raw` and `Update.Raw` carry any BSON the typed AST does
not model, and `underlying` on the client, database and collection drops you to the driver object, where the driver's
full API applies.

## Wanted, not yet scheduled

| | Why it is not here yet | Workaround today |
| --- | --- | --- |
| **GridFS** | A module of its own, with its own streaming story on four runtimes. Nobody has asked for it. | The driver's `GridFSBuckets` over `client.underlying`. |
| **Streaming bulk inputs** | `insertMany` and `bulkWrite` take a `Seq`; taking an `S` would need `RsBridge` to fold a stream into `F`, which the four runtimes share no shape for. | Chunk the stream with your runtime and `insertMany` each chunk. |
| **The rest of Atlas Search** | `$search` has more operators than the typed core — `near`, `geoWithin`, `moreLikeThis`, `queryString`, `embeddedDocument`, highlighting, facets in `$searchMeta` — and index mappings beyond dynamic and vector ones. The core is what most search code reaches for; the rest waits for someone who needs it. | `SearchOperator.raw`, `SearchIndex.of` with a mapping written by hand. |
| **The fake evaluating expressions** | Faithful evaluation means copying the server's rules for every operator — numeric widening, null handling, date arithmetic — and a fake that gets one subtly wrong is worse than one that refuses. | An integration test against a container. |

## How these land

Most of the list is **additive** — new methods, new options on the existing options values, new `Stage` cases, a new
codec-bridge module. `5.0.0` moved what it needed to move (see [COMPATIBILITY.md](COMPATIBILITY.md)); from here the
shape of every operation signature is frozen again, so those additions can ship in a `5.x` minor release without
breaking anybody, and will as demand appears.

Several of them are less additive than they look, though — a new `Stage` or `Filter` case breaks an exhaustive
match, a new abstract method breaks anything implementing the trait, and a new parameter on an existing method is
never binary-compatible. Those are cheapest inside a major release, so they are worth pulling forward while one is
open rather than deferring on principle.

If you need one of these, open an issue saying what you are building — demand is what moves an item up this list.
