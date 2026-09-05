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
| **`CreateCollectionOptions`** | capped, validator, timeseries, clustered. | `database.runCommand`. |
| **Aggregation stages** | `$bucket`, `$setWindowFields`, `$densify`, Atlas `$search`. | `Stage.raw`. |
| **Client-level `bulkWrite`** | Driver 5.3+ can write across collections in one command. | Per-collection `bulkWrite`. |

## How these land

Most of the list is **additive** — new methods, new options on the existing options values, new `Stage` cases, a new
codec-bridge module. Since `2.0.0` froze the shape of every operation signature — and `3.0.0` changed only the
Scala version, not the API — those can ship in a `3.x` minor release without breaking anybody, and will as demand
appears.

Several of them are less additive than they look, though — a new `Stage` or `Filter` case breaks an exhaustive
match, a new abstract method breaks anything implementing the trait, and a new parameter on an existing method is
never binary-compatible. Those are cheapest inside a major release, so they are worth pulling forward while one is
open rather than deferring on principle.

If you need one of these, open an issue saying what you are building — demand is what moves an item up this list.
