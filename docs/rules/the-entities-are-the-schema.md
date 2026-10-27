# The entities are the schema

The repo owns no migrations. TypeORM `synchronize` creates and alters tables from the entities at
every boot — in production too. Open this page before adding, renaming or removing an entity
column, an index or an enum value.

## What an entity change does in production

- **`synchronize` follows `DB_SYNCHRONIZE`** (`src/config/database.config.ts`), and both
  `.env.example` and `.env.production.example` set it to `true` (production checked on the running
  container, 2026-09-22, PAR-399). An entity change reaches the production schema at the next
  container start, before any code can prepare for it.
- **A new column on a populated table is nullable or has a default.** Anything else fails at boot.
  A seed or backfill for it runs after the deploy that adds it.
- **There is no rename.** A column renamed or removed in an entity is dropped at boot, with its data.
  Move data by adding the new column, copying, and removing the old one in a later deploy.
- **A value set that grows is `text` plus a TypeScript list, not a Postgres enum.** A new enum value
  makes `synchronize` recreate the type and convert the column, which rewrites the table under an
  ACCESS EXCLUSIVE lock: adding `working_day` to `holidays.holidayType` rewrote ~199k rows while
  every holiday read waited. `attraction_kind` and `indoor_outdoor` are text for this reason.
- **An index removed from an entity is dropped at boot** with a blocking `DROP INDEX`; drop a large
  one by hand before the deploy ([DB health runbook](../troubleshooting/db-health-runbook.md)). An
  index `synchronize` cannot express is declared `{ synchronize: false }` and created in code (the
  trigram indexes in `SearchService.initializeFuzzySearchIndices`).

## What `synchronize` does not create

- **Hypertables and compression** come from the list in `src/database/hypertables.ts`, applied at
  boot by `TimescaleInitService`. A new hypertable is one line there; the E2E schema reads the same
  list.
- **Tables written by the Python services have no entity** (`pcn_forecasts`, `tft_forecasts`,
  `tft_forecasts_shadow`, `tft_forecasts_shadow_drivers`). Their DDL lives in the writer
  (`pcn-service/db.py`, `nf-service/db.py`), and `test/helpers/ml-forecast-tables.ts` reads it from
  there rather than copying it ([E2E merge coverage](../development/e2e-merge-coverage.md)).

## Scripts

A script that opens its own DataSource against production sets `synchronize: false`
([production one-offs](automated-retirement-and-identity.md#7-production-one-offs-run-the-deployed-code-from-a-clean-checkout)).
