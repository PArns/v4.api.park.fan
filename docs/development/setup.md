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
docker compose up -d postgres redis ml-service
pnpm dev                          # nest start --watch on http://localhost:3000
```

`.env.example` sets `DB_SYNCHRONIZE=true`, so the schema is created from the entities on first boot
([the entities are the schema](../rules/the-entities-are-the-schema.md)). `docker-compose.yml` also
defines `nf-service`, `pcn-service`, `api` and `bull-board`; start them by name when you need them.
`pnpm db:reset` takes the local stack down **with its volumes** (`docker-compose down -v`) and starts
Postgres and Redis again.

To debug ml-service outside its container:

```bash
cd ml-service
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

## Test and lint

| Command          | What                                                                                                                 |
| ---------------- | -------------------------------------------------------------------------------------------------------------------- |
| `pnpm test`      | Unit tests (Jest).                                                                                                   |
| `pnpm test:e2e`  | E2E tests against a Testcontainers TimescaleDB ([which version](e2e-database-version.md), [merge schema](e2e-merge-coverage.md)). |
| `pnpm lint`      | ESLint with `--fix`; `pnpm lint:check` without.                                                                      |
| `pnpm build`     | Nest build plus the Swagger spec (`scripts/generate-swagger-spec.ts`).                                               |
| `ruff check .`   | Python lint, per service ([rule sets differ](python-dependencies.md#lint)).                                          |

Other commands: [scripts](scripts.md).
