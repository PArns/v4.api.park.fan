# The merge path's E2E schema

How the E2E suite builds a schema the park and attraction merge can run against: the tables with no
entity, hypertables, the guards that fail loudly, and the precondition that silently switched a merge
spec off once. Open it before adding a table to `src/parks/utils/merge-dependencies.ts`, a hypertable, or
a merge E2E case. Why the schema comes from the entities at all: [the entities are the
schema](../rules/the-entities-are-the-schema.md).

## Tables without an entity: read the DDL, never copy it

`ATTRACTION_DEPENDENCIES`, `PARK_DEPENDENCIES`, `PARK_INLINE_DEPENDENCIES` and `PARK_TABLES_HANDLED_INLINE`
list every table a merge touches. `synchronize: true` in `test/global-setup.ts` creates the entity tables.
Four are written by the Python services and have no entity:

| Table                          | DDL read from                                   |
| ------------------------------ | ----------------------------------------------- |
| `pcn_forecasts`                | `pcn-service/db.py`                             |
| `tft_forecasts`                | `nf-service/db.py`                              |
| `tft_forecasts_shadow`         | `nf-service/db.py` (`LIKE tft_forecasts`, PAR-1041) |
| `tft_forecasts_shadow_drivers` | `nf-service/db.py` (`LIKE tft_forecasts`, PAR-1057) |

`test/helpers/ml-forecast-tables.ts` (`ML_FORECAST_TABLE_SOURCES`) **extracts the `CREATE TABLE IF NOT
EXISTS` statement from the writer's own file**: a plain text scan that matches parentheses by counting,
after stripping line comments, so it works on a Python `text("""…""")` block and a TypeScript template
alike. A copied DDL would drift the first time a service added a column, and the suite would keep passing
against a table production no longer has. If the statement moves, setup throws naming the file, instead of
a 42P01 inside the merge transaction that reads like a merge bug.

Without these tables the first merge E2E aborted with `relation "pcn_forecasts" does not exist` before any
assertion ran, which is why `repairDuplicates` had no E2E coverage until 2026-09-10. A unit test with a
recorded manager proves a statement is issued; only a real database proves the transaction commits.
(PAR-100 and PAR-121 counted eight entity-less tables, including the four `*_p50/p90_baselines`, which
have entities: their measurement used `CREATE TABLE IF NOT EXISTS`, a silent no-op on a table
`synchronize` had already made.)

**No foreign keys are added**, matching production: these writers do not share the entity graph, and a
stricter test schema would turn a forgotten dependency into a loud 23503 instead of the quiet orphan the
merge code exists to prevent.

**In production these tables appear only on the service's first write.** `tft_forecasts_shadow` did not
exist between the API deploy and the first shadow run (and never with `NF_GRID_SHADOW=false`), so the
merge's move on it rolled back every attraction merge, `repairDuplicates` included, while the suite stayed
green. Dependencies on these raw-SQL tables therefore carry `mayBeAbsent: true`, and
`applyMergeDependencies` skips them when `to_regclass` is null. A new service-written table needs the
flag; an entity table never gets it (a missing one is a bug, and skipping it would orphan rows).

## Hypertables come from one list

Production has ten compressed hypertables. The merge writes to seven of them: `queue_data`,
`forecast_data`, `weather_data`, `show_live_data`, `restaurant_live_data`, `queue_data_aggregates`,
`wait_time_predictions`. The site statistic's three (`site_pageviews`, `site_events`,
`site_analytics_flushes`) are insert-only: a merge records `loser → winner` in the plain table
`site_entity_merges` and the reports resolve it when they read
([site statistic](../frontend/site-analytics.md#storage-insert-only-no-dead-tuples)).
`synchronize` makes none of them a hypertable. They are `HYPERTABLES` in
`src/database/hypertables.ts` (table, partitioning column, `chunk_time_interval`, compression settings),
which `TimescaleInitService.initializeHypertables` applies in production and `global-setup.ts` imports.
Before that list existed the test setup converted one, then three, tables by hand, so cases about
compressed rows were measuring plain tables. Adding a hypertable is one entry there.

Two consequences that are easy to miss:

1. **`TimescaleInitService.enableCompression` is a silent no-op on a plain table**, and the first suite
   to boot decides the compression state later ones inherit. A test that toggles compression reads the
   current state and restores only what it changed (`park-merge.e2e-spec.ts` does); a hard
   `SET (timescaledb.compress = false)` disables it for every later file and orphans the policy.
2. **`create_hypertable` failures only warn**, so a table can quietly stay plain.
   `park-merge.e2e-spec.ts` asserts every `HYPERTABLES` entry against
   `timescaledb_information.dimensions` (name, column, interval compared in Postgres) and the list's own
   length first.

Compression settings are fields on the same constant: only `wait_time_predictions` has a `segmentBy`,
as in production. Segmenting by a column the merge rewrites would make the test easier than production.

## Guards a new dependency must pass

1. **Schema guard**: `test/e2e/park-merge.e2e-spec.ts` checks every table in the four lists with
   `to_regclass`. A dependency on a table with neither an entity nor an entry in `ml-forecast-tables.ts`
   fails there, by name.
2. **Truncation**: `test/setup-e2e.ts` empties the entity tables plus `ML_FORECAST_TABLES`. A table in
   neither is never emptied, and one spec's rows become the next spec's start.

## A gate in front of the path can switch a spec off

`repairDuplicates` merges only pairs `ParkValidatorService.findDuplicates` returns as `safe`
(`sharedEntityId && nameSimilarity >= 0.95`, `AUTO_MERGE_NAME_SIMILARITY`, since PAR-262). The fixture's
`winner-park` / `ghost-park` scored 0.4211, so no merge ran, and every assertion failed against an
untouched database in a way that read like a rollback. `expectPairIsMergeable()` now asserts the
precondition before the call. **Where the path under test sits behind a gate, the fixture satisfying the
gate is part of the contract and is asserted like one.**

A spec is also only trusted once it has been seen red: with only `parks.service.ts` and
`merge-dependencies.ts` rolled back, `a045850~1` fails with 42703 and `a045850` with 23503 (each one
statement further in), and `main` passes.
