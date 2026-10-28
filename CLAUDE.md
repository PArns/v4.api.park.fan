# park.fan API (v4)

NestJS 12 API on PostgreSQL 18 + TimescaleDB and Redis 8, plus three Python services:
`ml-service` (CatBoost), `nf-service` (TFT, Chronos-2) and `pcn-service` (intraday PCN). It serves
park.fan, whose repo `PArns/park.fan` is often checked out next to this one. Public routes live under
`/v1`, Swagger under `/api`; the root `README.md` is also the page served at api.park.fan/.

## Find what your task needs

This file is a router: the rules below apply to every change, the routes lead to the background.

- **A field or endpoint a client reads** → [API contracts](docs/frontend/README.md); they own every
  contract the frontend relies on
- **Ride status, seasons, identity, duplicates, retirement** →
  [attraction status](docs/architecture/attraction-status/README.md) ·
  [season contract](docs/frontend/seasonality.md)
- **Opening hours, calendar, schedule sync** →
  [schedule sync](docs/architecture/schedule-sync-and-calendar.md) ·
  [calendar rules](docs/architecture/calendar-schedule-and-ml-rules.md)
- **Crowd levels, headliners, statistics** → [crowd-level mechanism](docs/analytics/crowd-levels.md) ·
  [analytics](docs/README.md#analytics)
- **Forecasts and models** → [what serves what](docs/ml/model-overview.md)
- **Jobs, crons, caching** → [job queues](docs/architecture/job-queues.md) ·
  [caching](docs/architecture/caching-strategy.md)
- **Admin, curation** → [authentication](docs/admin/authentication.md) ·
  [curation](docs/admin/curation.md)
- **SQL against production, DB health** → [runbook](docs/troubleshooting/db-health-runbook.md)
- **Deploys** → [Coolify](docs/deployment/coolify.md)
- **Anything else** → [docs/README.md](docs/README.md), the map of every page

## Applies to every change

One line per standing rule; each links its page in `docs/rules/` with the reason and the checks.

- **[Park timezone, always](docs/rules/park-timezone.md):** never `new Date()` for business logic;
  "today" and date ranges come from `park.timezone` through `src/common/utils/date.util.ts`.
- **[The entities are the schema](docs/rules/the-entities-are-the-schema.md):** there are no
  migrations and `DB_SYNCHRONIZE=true` runs in production, so an entity change alters the
  production schema on deploy.
- **[Unified crowd levels](docs/rules/crowd-levels.md):** typical-day-peak for days, ratio-vs-P50
  live, never mixed on one surface; without a baseline the level is `unknown`, never `moderate`.
- **[An absent fact never becomes a confident one](docs/rules/absent-facts.md):** a ride no source
  reports is `UNKNOWN`, not `CLOSED`; curated columns sit beside the synced one.
- **[Publish a write outside-in](docs/rules/publish-outside-in.md):** write, evict Redis,
  revalidate the frontend, and revalidate again after `CDN_SETTLE_MS`.
- **[Automated retirement, upstream identity and production one-offs](docs/rules/automated-retirement-and-identity.md):**
  identity is the upstream `externalId`; count a destructive job's first run; ad-hoc SQL only
  through `scripts/prod-psql.sh`.
- **[Cast the parameter, never the uuid column](docs/rules/uuid-casts.md):** `col = $1::uuid`,
  never `col::text = $1`, in the API and every Python service; filter ids with `isCanonicalUuid` /
  `uuid_list` before binding.
- **Changelog:** one fragment per pull request in `docs/changelog.d/` (`PAR-<issue>.md`, first
  line `### <Type> — <title>`, links relative to `docs/`; [format](docs/changelog.d/README.md)),
  never `docs/changelog.md` directly. `pnpm changelog:check` reads the fragments,
  `pnpm changelog:merge` folds them in at release.
- **Before finishing:** what CI runs — `pnpm lint:check`, `pnpm test`, `pnpm build` — plus
  `pnpm test:e2e` when the change touches SQL, and `ruff check .` in a Python service you changed.
  Routes are `/v1/<resources>`, shared types live in `src/common/types/`.

## Keeping the docs in shape

This file is indexed, not grown: new rules and background go where
[docs/README.md](docs/README.md#keeping-the-docs-in-shape) says, never here. Open work is a Linear
issue (team PAR), not a list in the repo.
