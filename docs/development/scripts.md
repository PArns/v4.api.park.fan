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
| `scripts/trip-regions/` | `build-travel-matrix.mjs` rebuilds the trip regions' OSRM travel matrix; `import-seasons.mjs` imports researched park seasons through the admin API, a dry run unless `--apply` ([trip regions](../frontend/trip-regions.md)) |
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

## Running a local script

```bash
npx ts-node -r tsconfig-paths/register scripts/<name>.ts
# against production, with a local, untracked .env.live_debug:
npx dotenv -e .env.live_debug -- ts-node -r tsconfig-paths/register scripts/<name>.ts
```

Before a script touches production:
[production one-offs](../rules/automated-retirement-and-identity.md#7-production-one-offs-run-the-deployed-code-from-a-clean-checkout).
