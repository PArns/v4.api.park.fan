# Scripts Overview

The `scripts/` directory contains one-off and operational scripts (triggers, checks, backfills, tests). **The directory is gitignored** so scripts that use DB credentials or are local-only are not committed. A `scripts/README.md` (if present) documents what exists and how to run them.

## NPM-run scripts (package.json)

These are the only scripts that are part of the repo and run from the project root:

| Command | Script | Purpose |
|--------|--------|--------|
| `npm run build` (postbuild) | `scripts/generate-swagger-spec.ts` | Generate Swagger spec after build |
| `npm run clear-queues` | `scripts/clear-queues.ts` | Clear BullMQ queues |
| `npm run job:ml-train` | `scripts/trigger-ml-training.ts` | Trigger ML training job |
| `npm run job:children-sync` | `scripts/trigger-children-sync.ts` | Trigger children sync |
| `npm run job:clear-cache` | `scripts/clear-cache.ts` | Clear Redis cache (e.g. park integrated) |

Run with:

```bash
npm run <script-name>
# or for TS scripts that need paths:
npx ts-node -r tsconfig-paths/register scripts/<name>.ts
```

## Script categories (see scripts/README.md)

If you have a local `scripts/` copy, see `scripts/README.md` for:

- **Trigger**: One-off job triggers (sync, ML train, geocoding, etc.)
- **Check / Debug**: DB and cache checks, holiday/schedule debug (some need `.env.live_debug`)
- **Backfill**: Daily stats, geo slugs, etc.
- **Test / Verify**: Plausibility checks, analytics/crowd-level tests
- **Util**: Clear cache, generate Swagger, geographic slugs

Scripts that connect to production or use credentials (e.g. `check-toverland-stats-db.ts`) typically use `dotenv -e .env.live_debug` and are kept local only.
