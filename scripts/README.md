# Scripts Directory

Operational and one-off scripts. **This directory is gitignored** except this README; scripts that use DB credentials or are local-only stay uncommitted.

Run TS scripts from repo root:

```bash
npx ts-node -r tsconfig-paths/register scripts/<name>.ts
```

Scripts that need production DB use `.env.live_debug`:

```bash
npx dotenv -e .env.live_debug -- ts-node -r tsconfig-paths/register scripts/<name>.ts
```

## NPM commands (package.json)

| Command | Script | Purpose |
|--------|--------|--------|
| `npm run build` (postbuild) | `generate-swagger-spec.ts` | Swagger spec after build |
| `npm run clear-queues` | `clear-queues.ts` | Clear BullMQ queues |
| `npm run job:ml-train` | `trigger-ml-training.ts` | Trigger ML training |
| `npm run job:children-sync` | `trigger-children-sync.ts` | Trigger children sync |
| `npm run job:clear-cache` | `clear-cache.ts` | Clear Redis cache |

## Categories

### Trigger (one-off job runs)

| Script | Purpose |
|--------|--------|
| `trigger-ml-training.ts` | Trigger ML training job |
| `trigger-children-sync.ts` | Trigger children sync |
| `trigger-full-sync.ts` | Full sync |
| `trigger-geocoding.ts` | Geocode missing parks |
| `trigger-live-sync.ts` | Live wait-time sync |
| `trigger-park-enrichment.ts` | Park enrichment job |
| `trigger-park-sync.ts` | Park sync |
| `trigger-percentile-backfill.ts` | Percentile backfill |
| `trigger-prod-sync.ts` | Production sync |
| `manual-sync-holidays.ts` | Manual holiday sync |

### Cache / util

| Script | Purpose |
|--------|--------|
| `clear-cache.ts` | Clear park integrated cache (npm: `job:clear-cache`) |
| `clear-cache-phl.ts` | Clear cache for Phantasialand |
| `clear-queues.ts` | Clear BullMQ queues |
| `clear-redis-cache.ts` | Clear Redis cache |
| `generate-swagger-spec.ts` | Generate Swagger spec (postbuild) |
| `generate-geographic-slugs.ts` | Generate geo slugs |

### Check / debug (DB or live)

| Script | Purpose |
|--------|--------|
| `check-global-status.ts` | Global status check |
| `check-holidays-db.ts` | Holiday DB check |
| `check-live-holidays.ts` | Live holiday check |
| `check-nager-school-coverage.ts` | Nager school coverage |
| `check-park-duplicates-with-knowledge.ts` | Park duplicates with knowledge |
| `check-phl-holidays.ts` | Phantasialand holidays |
| `check-toverland-stats-db.ts` | Toverland stats in DB (uses .env.live_debug) |
| `debug-holidays-db.ts` | Holiday DB debug |
| `debug-holidays.ts` | Holiday debug |
| `debug-next-schedule.ts` | Next schedule debug |
| `debug-park-region.ts` | Park region debug |
| `debug-taron-trend.ts` | Taron trend debug |
| `debug-wz-names.js` | Wartezeiten names debug |

### Backfill

| Script | Purpose |
|--------|--------|
| `backfill-daily-stats.ts` | Backfill park_daily_stats |
| `backfill-geo-slugs.sql` | SQL for geo slugs |
| `apply-geocoded-data.sql` | Apply geocoded data |

### Test / validate / verify

| Script | Purpose |
|--------|--------|
| `test-analytics-consistency.sh` | Analytics consistency |
| `test-cache-ttl.sh` | Cache TTL |
| `test-crowd-levels.ts` | Crowd levels |
| `test-geo-discovery.sh` | Geo discovery |
| `test-holiday-utils.ts` | Holiday utils |
| `test-matching-algo.ts` | Matching algo |
| `test-ml-training.sh` | ML training |
| `test-ml.ts` | ML |
| `test-park-duplicates.ts` | Park duplicates |
| `test-park-trend.ts` | Park trend |
| `test-park-validator.ts` | Park validator |
| `test-phantasialand-count.ts` | Phantasialand count |
| `test-schedule-holiday-fields-simple.ts` | Schedule holiday fields (simple) |
| `test-schedule-holiday-fields.ts` | Schedule holiday fields |
| `validate-p50-baselines.ts` | P50 baselines |
| `validate-wartezeiten-matches.ts` | Wartezeiten matches |
| `verify-crowd-level.ts` | Crowd level |
| `verify-queue-types.ts` | Queue types |
| `plausibility-check-toverland-efteling.sh` | Plausibility check Toverland/Efteling API |

### Analysis / fix (one-off)

| Script | Purpose |
|--------|--------|
| `analyze-park-duplicates.ts` | Analyze park duplicates |
| `detailed-park-analysis.ts` | Detailed park analysis |
| `final-park-duplicate-analysis.ts` | Final duplicate analysis |
| `quick-park-analysis.ts` | Quick park analysis |
| `show-park-duplicates-summary.ts` | Park duplicates summary |
| `fix-critical-park-issues.ts` | Fix critical park issues |
| `fix-known-park-issues.ts` | Fix known park issues |
| `repro-holiday-date.ts` | Reproduce holiday date |
| `repro-matching.ts` | Reproduce matching |
| `geocode-missing-parks.sh` | Geocode missing parks |
| `inspect-wiki-parks.js` | Inspect wiki parks |
| `reset-and-monitor.sh` | Reset and monitor |
