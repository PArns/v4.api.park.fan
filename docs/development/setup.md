# Local development setup

Prerequisites, install, running the API and the Python services locally, and the test and lint
commands. Open it when setting up a checkout. Production runs differently:
[production deployment](../deployment/coolify.md).

## Prerequisites

- **Node.js 26** (`engines: ">=26"`; CI and the Docker image run 26) and **pnpm 11.5.0**
  (`packageManager` in `package.json`).
- **Docker**, for PostgreSQL/TimescaleDB, Redis and the Python services.
- **Python** only to work on a Python service outside Docker: ml-service runs 3.13, nf-service,
  pcn-service and ml-bench 3.11 ([Python dependencies](python-dependencies.md)).

## Install and run

```bash
pnpm install
cp .env.example .env              # defaults match docker-compose.yml
docker compose up -d postgres redis bull-board   # add ml-service nf-service pcn-service for forecasts
pnpm dev                          # nest start --watch on http://localhost:3000
```

The API starts without the Python services; each forecast read degrades on its own until they run.
Bull Board shows the job queues on http://localhost:3001. `pnpm docker:up` starts the whole stack,
the `api` container on port 3000 included — use it instead of `pnpm dev`, not alongside it.
`.env.example` sets `DB_SYNCHRONIZE=true`, so the schema is created from the entities on first boot
([the entities are the schema](../rules/the-entities-are-the-schema.md)). `pnpm db:reset` takes the
local stack down **with its volumes** (`docker-compose down -v`) and starts Postgres and Redis again.

To debug ml-service outside its container:

```bash
cd ml-service
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

## Test and lint

| Command         | What                                                                                                                              |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `pnpm test`     | Unit tests (Jest).                                                                                                                |
| `pnpm test:e2e` | E2E tests against a Testcontainers TimescaleDB ([which version](e2e-database-version.md), [merge schema](e2e-merge-coverage.md)). |
| `pnpm lint`     | ESLint with `--fix`; `pnpm lint:check` without.                                                                                   |
| `pnpm build`    | Nest build plus the Swagger spec (`scripts/generate-swagger-spec.ts`).                                                            |
| `ruff check .`  | Python lint, per service ([rule sets differ](python-dependencies.md#lint)).                                                       |

### E2E runs: one worker, recycled

Every E2E suite boots `AppModule` against one shared TimescaleDB and Redis, which `test/global-setup.ts`
starts once per run. The suites therefore run one at a time (`maxWorkers: 1` in `test/jest-e2e.json`),
never in parallel. They run in a child worker rather than in band: each suite leaves some heap behind,
and in one process (`--runInBand`) 26 suites reached about 2 GB and died with `JavaScript heap out of
memory` after 9 to 12 minutes (PAR-1162). `workerIdleMemoryLimit: "1GB"` makes Jest replace the worker
once it is above that after a suite. Do not add `--runInBand` back: it disables the limit. The
container addresses reach the worker through the environment `global-setup.ts` sets before the worker
starts.

**Leftover containers.** Testcontainers' reaper (Ryuk) removes the containers when the run ends,
also after a crash. With `TESTCONTAINERS_RYUK_DISABLED=true` (needed on some Docker setups) nothing
does that after a crash, because `globalTeardown` never runs; remove them by hand:

```bash
docker ps -a --filter label=org.testcontainers=true -q | xargs -r docker rm -f
```

Other commands: [scripts](scripts.md).
