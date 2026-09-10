# The merge path's E2E gate

> **2026-09-10** — why `repairDuplicates` had no end-to-end coverage until now,
> and the rule that keeps the test schema from drifting away from production.

## Why there was no test

`ATTRACTION_DEPENDENCIES`, `PARK_DEPENDENCIES`, `PARK_INLINE_DEPENDENCIES` and
`PARK_TABLES_HANDLED_INLINE` (`src/parks/utils/merge-dependencies.ts`) name
**34 tables** between them. Thirty are TypeORM entities, so `synchronize: true`
in `test/global-setup.ts` creates them. Four are not:

```
pcn_forecasts · shape_forecasts · tft_forecasts · catboost_daily_forecasts
```

They exist in production, created at runtime by the service that writes them —
three Python sub-services and one NestJS processor — not by TypeORM.
`applyMergeDependencies` walks its list straight through, so on the test schema
the first one it reached aborted the whole merge:

```
QueryFailedError: relation "pcn_forecasts" does not exist
  at applyMergeDependencies (src/parks/utils/merge-dependencies.ts:392)
```

That is a wall on the first run, not an oversight. Anyone writing a merge E2E
hit it before a single assertion could execute, and the path stayed uncovered —
which is precisely why the three faults PAR-90 fixed lived as long as they did.
A unit test with a recorded manager proves a statement is **issued**; only a
real database proves the transaction **commits**.

> Note for anyone reading the older tickets: PAR-100 and PAR-121 both say
> **eight** tables lack an entity, listing the four `*_p50/p90_baselines`
> alongside these. That is wrong — all four baseline tables have had entities
> since before `e099daa`. The count is four. The earlier measurement used
> `CREATE TABLE IF NOT EXISTS`, which is a silent no-op on a table `synchronize`
> had already made.

## The rule: read the DDL, never copy it

`test/helpers/ml-forecast-tables.ts` creates the four tables by **extracting the
`CREATE TABLE` statement from the file that issues it**:

| Table | Source of truth |
| -- | -- |
| `pcn_forecasts` | `pcn-service/db.py` |
| `shape_forecasts` | `shape-service/db.py` |
| `tft_forecasts` | `nf-service/db.py` |
| `catboost_daily_forecasts` | `src/queues/processors/nf-forecast.processor.ts` |

A copied DDL is a second definition of a schema this repo owns no migration
for, and it drifts the first time a sub-service adds a column: the suite keeps
passing against a table production no longer has. Reading the writer's own
statement cannot drift. If the statement is renamed or moved, the extraction
finds nothing and throws with the file it looked in — loudly, at setup, rather
than as a 42P01 inside the merge transaction that reads like a merge bug.

The extraction is a plain text scan, so it works on a Python `text("""…""")`
block and a TypeScript template literal alike. Parentheses are matched by
counting (`varchar(16)`, `(now() AT TIME ZONE 'UTC')::date`), with line comments
stripped first so an unbalanced one inside a comment cannot throw the count off.

**No foreign keys are added**, matching production. These tables are written by
services that do not share this schema's entity graph — which is exactly why
`ATTRACTION_DEPENDENCIES` describes them as "no FK, so silent orphans if
forgotten". A test schema stricter than production would turn a forgotten
dependency into a loud 23503 instead of the quiet orphan the merge code exists
to prevent.

## Two things a new dependency must not slip past

1. **The schema guard.** `test/e2e/park-merge.e2e-spec.ts` checks every table in
   the four dependency lists against `to_regclass`. A new dependency on a table
   with neither an entity nor an entry in `ml-forecast-tables.ts` fails there,
   named, instead of surfacing as 42P01 mid-merge.
2. **Truncation.** `test/setup-e2e.ts` empties `entityMetadatas` plus
   `ML_FORECAST_TABLES`. A table in neither list is never emptied between
   tests, and one spec's rows become the next spec's starting state.

## The spec is not empty

A green test proves nothing until it is shown it can go red. Rolling only
`parks.service.ts` and `merge-dependencies.ts` back, with the test harness left
as it is:

| Commit | Result | Where it dies |
| -- | -- | -- |
| `a045850~1` — before #245 | **42703** `column "attractionId" does not exist` | `parks.service.ts:632`, `UPDATE prediction_accuracy SET "attractionId"` |
| `a045850` — after #245, before #246 | **23503** `violates foreign key constraint "FK_f6d…014" on table "park_occupancy"` | the `DELETE FROM parks` |
| `main` | **green** | — |

Each stand dies on a *different* statement, one level further in. That is the
answer to why this survived so long: without a real database, none of the three
is visible.

The schema guard has to come out for that counter-check — it imports
`PARK_INLINE_DEPENDENCIES`, which #245 introduced, so on the older commits the
suite fails to compile instead of failing an assertion. A compile error is not
the counter-check.

## Related

- [Attraction Status & Seasonality](../architecture/attraction-status-and-seasonality.md) — duplicate detection and the review marks that keep a settled verdict settled.
- `src/parks/utils/merge-dependencies.ts` — the lists themselves, with the reasoning per table.
