# Scripts and package commands

What lives in `scripts/`, which `package.json` commands exist and which of them work on a fresh clone,
and how to run a local one-off script. Open it before running or adding a script.

## What is tracked

`scripts/` is gitignored (`.gitignore`: `scripts/*`) so local scripts with database credentials stay
uncommitted. Tracked:

| Path                                   | What                                                                                                  |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `scripts/prod-psql.sh`                 | Ad-hoc SQL against production with per-session time limits ([DB health runbook §0](../troubleshooting/db-health-runbook.md#0-time-limits-on-ad-hoc-sql)). Never `docker exec … psql` by hand. |
| `scripts/backup/parkfan-backup.sh`, `backup.env.example` | The nightly host backup ([backup](../deployment/backup.md)).                                       |
| `scripts/python-deps/`                 | Artefact compatibility checks for a Python dependency upgrade ([Python dependencies](python-dependencies.md#re-running-the-check)). |
| `scripts/generate-swagger-spec.ts`     | Runs as `postbuild` and writes the Swagger spec; skipped with a warning when it fails.                |
| [`scripts/README.md`](../../scripts/README.md) | A pointer to this page.                                                                               |

## Package commands

| Command                                                            | Notes                                                                 |
| ------------------------------------------------------------------ | --------------------------------------------------------------------- |
| `pnpm dev`, `pnpm start`, `pnpm start:debug`, `pnpm start:prod`    | Nest with watch, plain, debug, or `node dist/main`                    |
| `pnpm build`                                                       | `nest build` plus the Swagger spec                                    |
| `pnpm test`, `test:watch`, `test:cov`, `test:e2e`, `test:all`      | Jest unit and E2E suites ([setup](setup.md#test-and-lint))            |
| `pnpm lint`, `lint:check`, `format`                                | ESLint (fixing / checking), Prettier over `src` and `test` TypeScript |
| `pnpm changelog:merge`, `changelog:check`                          | Fold `docs/changelog.d/` fragments into `docs/changelog.md` / report what they contain |
| `pnpm docker:up`, `docker:down`, `docker:logs`, `db:reset`         | Local compose stack; `db:reset` removes its volumes                   |
| `pnpm db:test:create`, `db:test:drop`, `db:test:reset`             | A `parkfan_test` database in the local `parkfan_postgres` container   |

These commands point to scripts that are **not in the repository** and fail on a fresh clone; they work
only where someone keeps the local copy: `clear-queues`, `job:ml-train`, `job:stats-backfill`,
`job:baselines-recalc`, `job:clear-calendar-cache`, `job:children-sync`, `job:clear-cache`,
`repair:schedule-dates`. The tracked alternatives for the common cases are the admin endpoints
(`POST /v1/admin/flush-cache`, `POST /v1/admin/train-ml-model`, …) and Bull Board
([recalculating aggregates](../analytics/data-recalculation.md)).

## Running a local script

```bash
npx ts-node -r tsconfig-paths/register scripts/<name>.ts
# against production, with a local, untracked .env.live_debug:
npx dotenv -e .env.live_debug -- ts-node -r tsconfig-paths/register scripts/<name>.ts
```

A script that opens its own DataSource against production sets `synchronize: false`, and a one-off that
writes to production runs the deployed code from a clean checkout
([production one-offs](../rules/automated-retirement-and-identity.md#7-production-one-offs-run-the-deployed-code-from-a-clean-checkout)).
