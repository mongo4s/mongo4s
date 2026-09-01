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
| **Geospatial operators** | `$near`, `$geoWithin` and `$geoIntersects` need a small geometry vocabulary to be typed honestly. The index types (`2dsphere`, `2d`) already exist. | `Filter.Raw`. |
| **`TransactionOptions` and retry semantics** | The driver's own `withTransaction` retries `TransientTransactionError` and `UnknownTransactionCommitResult`; `mongo4s`'s does not. Adopting that changes how a session is used rather than adding to it. | Retry around `withTransaction` yourself, or use `client.underlying`. |
| **`CreateCollectionOptions`** | capped, validator, timeseries, clustered. | `database.runCommand`. |
| **Aggregation stages** | `$bucket`, `$setWindowFields`, `$densify`, Atlas `$search`. | `Stage.raw`. |
| **Change-stream extras** | `showExpandedEvents`, `wallTime`, `splitEvent`. | `WatchOptions` covers the rest; the driver's publisher is reachable through `underlying`. |
| **Client-level `bulkWrite`** | Driver 5.3+ can write across collections in one command. | Per-collection `bulkWrite`. |
| **A circe bridge** | No `mongo4s-bson-circe` module, so a model already on circe has no first-class path. A gap rather than a decision. | Hand-write a `BsonDocumentCodec[A]` from circe's `Encoder`/`Decoder`; it is short. |

## How these land

Most of the list is **additive** — new methods, new options on the existing options values, new `Stage` cases, a new
codec-bridge module. Since `2.0.0` froze the shape of every operation signature, those can ship in a `2.x` minor
release without breaking anybody, and will as demand appears.

The exception is transaction retry semantics, which changes how a session is used rather than adding to it. That one
waits for `3.0.0`.

If you need one of these, open an issue saying what you are building — demand is what moves an item up this list.
