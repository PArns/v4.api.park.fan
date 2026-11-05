# Cast the parameter, never the uuid column

A filter on a uuid column compares uuid with uuid. Open this page before writing SQL with an id in its
`WHERE` or `JOIN`, in the API (`src/`) or in any Python service (`ml-service`, `nf-service`,
`pcn-service`). The measurements are in
[ml-service query performance](../ml/performance-optimizations.md#the-uuid-cast-fix-measured-par-871).

## The rule

```sql
-- WRONG: the column is hidden from its index
WHERE qd."attractionId"::text = ANY(:attraction_ids)
WHERE a."parkId"::text = $1

-- RIGHT
WHERE qd."attractionId" = ANY(CAST(:attraction_ids AS uuid[]))   -- SQLAlchemy
WHERE a."parkId" = $1::uuid      /  = ANY($1::uuid[])             -- TypeORM, pg
```

- **Filter the ids before you bind them.** A cast parameter raises `22P02` on a value that is not a
  uuid and fails the whole statement, where the text comparison matched nothing. Use
  `isCanonicalUuid` (`src/common/utils/uuid-param.util.ts`) or `uuid_list` / `uuid_or_none`
  (`uuid_params.py`, one identical copy in each Python service). They keep only the lowercase
  hyphenated form `uuid::text` prints, so they drop nothing the old comparison could have matched.
- **Casting an output is fine** (`SELECT qd."attractionId"::text AS "attractionId"`): it runs after
  the filter.
- **A text column is compared as text.** `queue_data_aggregates."attractionId"` and `"parkId"` are
  `text`, so a join onto them reads `a.id::text = qda."attractionId"`. Check the column type before
  copying a join: `queue_data."attractionId"` and `wait_time_predictions."attractionId"` are uuid.

## Why

A cast on the column turns the predicate into an expression that no index covers. On `queue_data` it
is worse than a missed index: TimescaleDB filters compressed chunks on their segment-by column, and
with a text expression it decompresses every chunk in the time window instead. An expression index
does not help. Two serving reads in `ml-service/predict.py` carried the cast and were about 80 % of
all database time on 2026-10-12 (PAR-871).

The opposite mistake fails outright: a uuid compared with text without a cast is
`operator does not exist: text = uuid`. Postgres parses raw SQL only when it runs, so neither the build
nor a unit test with a mocked repository sees either problem.

## How to check

- `grep -rnE '::text\s*(=|IN\b|<>)' src ml-service pcn-service nf-service` lists every comparison of a
  text cast. Each hit has to be a join onto a real text column or a comment.
- The Python tests fail on such a comparison in the services' query files:
  `cd ml-service && python3 -m pytest tests/test_uuid_params.py -q`, and `pcn-service/test_uuid_params.py`
  and `nf-service/tests/test_uuid_params.py` the same way, which also check that `uuid_params.py` is
  identical in all three. CI does not run them (`.github/workflows/ci.yml` runs lint, `pnpm test` and
  the build), so run them yourself. The API side has no such scan; only specs for single queries
  (`ml.service.spec.ts`, `children-metadata-mapping-reclaim.spec.ts`) pin it.
- `pnpm test:e2e` runs the SQL against a real database, which is where a `22P02` or a `text = uuid`
  shows.
- For a query on a large table, compare `EXPLAIN (ANALYZE, BUFFERS)` before and after through
  `scripts/prod-psql.sh`: the filtered column should reach an index, and on `queue_data` the buffer
  count should drop with it.
