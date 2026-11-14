# Scripts and package commands

What lives in `scripts/`, which `package.json` commands exist and what replaced the ones that
did not work on a fresh clone, and how to run a local one-off script. Open it before running or adding a script.

## What is tracked

`scripts/` is gitignored (`.gitignore`: `scripts/*`) so local scripts with database credentials stay
uncommitted. Tracked:

| Path                                                     | What                                                                                                                                                                                          |
| -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/prod-psql.sh`                                   | Ad-hoc SQL against production with per-session time limits ([DB health runbook §0](../troubleshooting/db-health-runbook.md#0-time-limits-on-ad-hoc-sql)). Never `docker exec … psql` by hand. |
| `scripts/backup/parkfan-backup.sh`, `backup.env.example` | The nightly host backup ([backup](../deployment/backup.md)).                                                                                                                                  |
| `scripts/python-deps/`                                   | Artefact compatibility checks for a Python dependency upgrade ([Python dependencies](python-dependencies.md#re-running-the-check)).                                                           |
| `scripts/generate-swagger-spec.ts`                       | Runs as `postbuild` and writes the Swagger spec; skipped with a warning when it fails.                                                                                                        |
| `scripts/trip-regions/` | `build-travel-matrix.mjs <region>` rebuilds one OSRM region's travel matrix and keeps the others' (a region whose travel is curated, Japan's trains, has none); `import-seasons.mjs` imports researched park seasons through the admin API, a dry run unless `--apply` (the lists researched for 2026/27, with their sources per row, are `seasons/orlando-2026-27.json`, `orlando-day-trips-2026-27.json`, `southern-california-2026-27.json`, `japan-2026-27.json` and `paris-2026-27.json`, whose rows without an end date the import skips, and none is imported into production without the owner's approval; a row's `region` names the trip region whose park it means, required where a slug repeats, as Disneyland Park does in Anaheim and Paris); `dev-server.ts` is the local stand-in for the trip-region routes (below) ([trip regions](../frontend/trip-regions.md)) |
| [`scripts/README.md`](../../scripts/README.md)           | A pointer to this page.                                                                                                                                                                       |

## Package commands

| Command                                                         | Notes                                                                                  |
| --------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `pnpm dev`, `pnpm start`, `pnpm start:debug`, `pnpm start:prod` | Nest with watch, plain, debug, or `node dist/main`                                     |
| `pnpm build`                                                    | `nest build` plus the Swagger spec                                                     |
| `pnpm test`, `test:watch`, `test:cov`, `test:e2e`, `test:all`   | Jest unit and E2E suites ([setup](setup.md#test-and-lint))                             |
| `pnpm lint`, `lint:check`, `format`                             | ESLint (fixing / checking), Prettier over `src` and `test` TypeScript                  |
| `pnpm changelog:merge`, `changelog:check`                       | Fold `docs/changelog.d/` fragments into `docs/changelog.md` / report what they contain |
| `pnpm docker:up`, `docker:down`, `docker:logs`, `db:reset`      | Local compose stack; `db:reset` removes its volumes                                    |
| `pnpm db:test:create`, `db:test:drop`, `db:test:reset`          | A `parkfan_test` database in the local `parkfan_postgres` container                    |

### Removed commands and what replaces them

Eight commands pointed to local scripts that were never in the repository (four were tracked once
and untracked in `e7078c08`) and failed on a fresh clone. They are gone from `package.json` (PAR-1110);
the same jobs run from tracked code:

| Removed command                               | Instead                                                                                                                                           |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `job:ml-train`                                | `POST /v1/admin/train-ml-model`                                                                                                                   |
| `job:clear-cache`, `job:clear-calendar-cache` | `POST /v1/admin/flush-cache` (owner); `calendar:*` is one of its patterns                                                                         |
| `job:children-sync`                           | Bull Board: `fetch-all-children` on `children-metadata`; or `POST /v1/admin/cache/reset?confirm=true` for the full rebuild                        |
| `job:stats-backfill`                          | Bull Board: `backfill-stats` on `stats` ([recalculating aggregates](../analytics/data-recalculation.md))                                          |
| `job:baselines-recalc`                        | Bull Board: `calculate-park-baselines` / `calculate-attraction-baselines` on `p50-baseline`                                                       |
| `clear-queues`                                | Bull Board, per queue ([job queues](../architecture/job-queues.md))                                                                               |
| `repair:schedule-dates`                       | A one-off, run on 2026-07-27; `normalizeClosingTime` now repairs misdated closes on write. Audit old rows with SQL through `scripts/prod-psql.sh` |

## The trip-region dev shim

`pnpm trip-regions:dev` serves `http://localhost:3001` for the frontend while the trip-region
routes and the day optimizer are not deployed. It answers `GET /v1/trip-regions` and, for every
region of `regions.ts`, `GET /v1/trip-regions/:region`, `/calendar`, `/best-days`, `/plan`,
`/when`, `/school-breaks` and `/tickets`, and `POST
/v1/parks/…/plan/optimize`, by running the API's own modules (`trip-region.assembly`,
`trip-best-days`, `trip-plan.assembly`, `trip-evening`, `trip-optimizer`, `trip-when`, `trip-when.assembly`,
`trip-school-breaks`, the day optimizer's `engine`) on production's public answers (`/calendar`,
`/schedule`, `/seasons`, `/plan/day`, read in spans of at most 90 days) and, for the school
breaks, on OpenHolidays and the curated sets through the sync's own `openHolidaysSchoolRows`, and
passes every other path to `https://api.park.fan` unchanged. It adds fetching
and wiring and no rule of its own, so its answers are the API's for the same data.

- **Key:** the calendar, plan day and plan routes need a data key. In the Claude Code container the
  HTTPS proxy injects one; elsewhere set `DEV_API_KEY` (sent as `X-API-Key`, never printed).
- **Proxy:** Node's `fetch` reaches a proxy only with `NODE_USE_ENV_PROXY=1`; the shim restarts
  itself with it when `HTTPS_PROXY` is set (without it production answers 401).
- **Seasons:** production holds no Orlando seasons yet. `DEV_SEASONS_FILE=<file>` adds the rows of a
  JSON file in the shape `import-seasons.mjs` reads, skipping a row without an end date, one
  production already has (same park, kind and name, overlapping dates) and one whose `region` is
  another region's.
- **Cache:** each production answer is kept 15 minutes in memory; a cold calendar takes a few
  seconds.
- **Tickets:** the shim has no database, so its ticket catalog is the seed (`tickets.ts` with
  `ticket-seed-links.ts`, `seedCatalog`), put on the region the way `TicketCatalogService` puts
  the table's. An edit made in production's admin shows on production's `/tickets`, not here.
- **Where it differs from the deployed API**, because production does not serve the input:
  no `meta.forecastUntil` (the warmup's Redis key); group names are the curated ones
  (`SeaWorld Orlando`), not the `destinations` rows (`SeaWorld Parks and Resorts Orlando`);
  production's `/schedule` carries no `description`, so a gap-filled CLOSED day past the published
  hours reads as stated; `headlinerWait` ranks on the rounded figure until the calendar's
  `exactWait` is deployed; the day optimizer gets no `ridesInWorks` and its request is not
  validated; `expectedError` reads `/plan/day`'s `leadTimeMae` per planned date, and the
  evenings' night shows `/plan/day`'s `shows` for each park and evening the plan asks for.

## Running a local script

```bash
npx ts-node -r tsconfig-paths/register scripts/<name>.ts
# against production, with a local, untracked .env.live_debug:
npx dotenv -e .env.live_debug -- ts-node -r tsconfig-paths/register scripts/<name>.ts
```

Before a script touches production:
[production one-offs](../rules/automated-retirement-and-identity.md#7-production-one-offs-run-the-deployed-code-from-a-clean-checkout).
