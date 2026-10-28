# park.fan API — documentation map

Every page under `docs/`, one line each; a folder is one line here and lists its own pages in its
README. Coding sessions start at [CLAUDE.md](../CLAUDE.md). The frontend's side of each contract
lives in the park.fan repo: [docs/api/](https://github.com/PArns/park.fan/tree/main/docs/api).

## Keeping the docs in shape

- **CLAUDE.md is indexed, not grown.** A new rule is a new file in `rules/` plus one line under
  "Applies to every change" in CLAUDE.md. New background goes into the page that owns the subject,
  or a new file plus one line here or in its folder's README.
- **One fact, one place.** A rule file owns its rule, `frontend/*` owns each API contract (park.fan's
  `docs/api/` only describes how the frontend reads it). Everywhere else links.
- **One subject per page.** When a page starts covering a second subject, split it and index the parts.
- **No to-do lists.** Open work is a Linear issue (team PAR); a page may name a known limit with its issue id.

## Standing rules

One line each in [CLAUDE.md](../CLAUDE.md#applies-to-every-change); every rule has its page in `rules/`.

## Architecture

- [system-overview](architecture/system-overview.md) — The services (API, ml/nf/pcn-service, Postgres, Redis) and how a wait time flows; first read
- [data-ingestion](architecture/data-ingestion.md) — Live wait-time sources, how several are merged per park, five-minute rounding of stored waits
- [reverse-reconciliation](architecture/reverse-reconciliation.md) — How rides no source reports get CLOSED rows, last-seen keys, and the hourly heartbeat rules
- [job-queues](architecture/job-queues.md) — Every scheduled Bull job with queue and UTC time, self-healing crons, failed-job monitoring
- [caching-strategy](architecture/caching-strategy.md) — Redis keys and TTLs, publishing order, park open/close webhook, cache warmup
- [schedule-sync-and-calendar](architecture/schedule-sync-and-calendar.md) — Opening-hours sync, wiki rate limit, silent/withdrawn-day guards, gap fill, calendar cache
- [calendar-schedule-and-ml-rules](architecture/calendar-schedule-and-ml-rules.md) — Calendar day status and crowd level rules, measured past days, ML schedule filter and hourly gate
- [weather](architecture/weather.md) — Open-Meteo sync, weather_data storage (noon UTC), climate normals, nowcast cadence, limits
- [school-holiday-coverage](architecture/school-holiday-coverage.md) — Past a region's last school row the flag is unknown: coverage rule and each model's handling
- [holiday-sources-outside-europe](architecture/holiday-sources-outside-europe.md) — KR/SG/AU/US/GB/JP/CN holiday sources, curated files, yearly upkeep table, district choices
- [location-resolution](architecture/location-resolution.md) — User coordinates for nearby/favorites: lat/lng, client IP, GeoLite2, no CDN caching
- [search](architecture/search.md) — /v1/search: in-process vs Postgres matching, ranking tiers, typo tolerance, literal-hit rule
- [covariate-source](architecture/covariate-source.md) — Where each model's covariates come from today, and the two write-only as-of logs (plan: PAR-1147)
- [attraction-status/](architecture/attraction-status/README.md) — Index of attraction status, seasonality, identity and duplicate pages; old § numbers mapped

## API contracts for clients

- [frontend/](frontend/README.md) — Index of the API contracts clients read, plus the conventions they share (null keys stripped, unknown)

## Analytics

- [crowd-levels](analytics/crowd-levels.md) — Crowd-level mechanism: formulas per surface, thresholds, baselines, jobs, ratability gate, calibration checks
- [headliner-logic](analytics/headliner-logic.md) — How headliners are chosen (tiers, data filters, tier-1 cap) and what to check when a crowd level looks wrong
- [sparklines](analytics/sparklines.md) — Today's per-ride wait-time arrays: content, which batch function to call, per-park effective start time
- [smart-gaps](analytics/smart-gaps.md) — Opening hours and open/closed inferred from ride activity for parks without a schedule; isEstimated flags
- [data-recalculation](analytics/data-recalculation.md) — What to rerun after a calculation change: backfill-stats, baseline recompute, cache flush, in order
- [ride-downtime/](analytics/ride-downtime/README.md) — Ride downtime index: what is answered vs refused, what is built and where, links to the five sub-pages
- [site statistic](frontend/site-analytics.md) — park.fan's own cookieless page views, visits and events: the collect endpoint, guards, insert-only storage, admin and SEO reports, measurements

## Machine learning

Start at the first page: which model serves which surface today.

- [model-overview](ml/model-overview.md) — Entry page: which model serves which surface today, flags, services, shadows, retired models — read first
- [catboost](ml/catboost.md) — CatBoost (ml-service): what it predicts, the 75 features, /predict contract, train/serve parity, traps
- [catboost-training](ml/catboost-training.md) — CatBoost nightly training: rows and filters, dropouts, hold-out, tree count, champion gate, activation
- [tft](ml/tft.md) — TFT daily level (nf-service): what it serves for days 1–60, panel, covariates, nightly run, evidence
- [tft-grid-shadows](ml/tft-grid-shadows.md) — The two nightly TFT shadows: calendar-day grid (PAR-1041) and grid + level drivers (PAR-1057)
- [pcn](ml/pcn.md) — PCN intraday nowcaster: GraphWaveNet model, targets, training, forecast tick, pcn_forecasts, boards
- [intraday-serving](ml/intraday-serving.md) — API side of the served 15-min curve: PCN override, persistence blend, Chronos window, raw vs served reads
- [quantile-serving-and-calibration](ml/quantile-serving-and-calibration.md) — Which quantile becomes the shown wait, crowd level and band; why; TFT measured band; hourly band factor
- [performance-optimizations](ml/performance-optimizations.md) — ml-service query performance: the uuid-cast rule for every service, recent-history reads, caches
- [accuracy-scoring-rule](ml/accuracy-scoring-rule.md) — Which prediction_accuracy slot counts as an error (score_class, scoredSlotSql), stored quantiles, readers
- [reader-facing-accuracy](ml/reader-facing-accuracy.md) — Badge, leadTimeMae and day-curve forecastError: which model each measures and how it is scored
- [long-range-forecasting](ml/long-range-forecasting.md) — Measured forecast horizons per model, TFT lead-time error, outcome-conditioning trap, served error grid
- [level-drivers](ml/level-drivers.md) — What explains the daily level: driver analysis, covariates TFT lacks, driver_level results and its prod shadow
- [ml-overview](ml/ml-overview.md) — Admin ML overview: switch decisions and evaluateOn, frozen decision reports, component status, drift watch
- [forward-archive/](ml/forward-archive/README.md) — Forward archive of served curves: what it answers, code, queues, tables, when scores appear; index
- [ml-bench/](ml/ml-bench/README.md) — ml-bench offline benchmark: what it is, layout, results dirs, verdict per model family; start here
- [foundation-models/](ml/foundation-models/README.md) — Foundation models in ml-bench: status and licence per model, shared adapter, licence enforcement, lessons
- [pcn-service/README](../pcn-service/README.md) — pcn-service directory map: endpoints and schedule, files, local run commands; model in docs/ml/pcn.md

## Database

- [schema](database/schema.md) — Where the schema is defined, core/precomputed/forecast tables, wait_time_predictions write path

## Admin

- [endpoints](admin/endpoints.md) — Every `/v1/admin` route by purpose with required role and what it triggers, linking the owning docs
- [authentication](admin/authentication.md) — Admin accounts, Redis sessions, scrypt, TOTP, roles, login limits, Turnstile, legacy pass
- [curation](admin/curation.md) — Curated columns on parks and attractions, resolvers, editor descriptors, publish, undo, merges

## Development

- [setup](development/setup.md) — Local setup: Node 26/pnpm, compose services, pnpm dev, test and lint commands
- [scripts](development/scripts.md) — Tracked scripts, package.json commands (and which need untracked local scripts), running scripts on prod
- [datetime-handling](development/datetime-handling.md) — Park-time mechanism: date.util helpers, park-local day queries, date-only values, US-park traps, pandas
- [python-dependencies](development/python-dependencies.md) — Python pin policy for the four services, held pins and why, artefact compatibility checks, ruff
- [e2e-merge-coverage](development/e2e-merge-coverage.md) — E2E schema for the merge path: entity-less tables read from writer DDL, HYPERTABLES, guards, gate trap
- [e2e-database-version](development/e2e-database-version.md) — Why E2E pins timescaledb 2.24.0-pg18, the PostgreSQL patch trade-off, and how to move the pin
- [scripts/README](../scripts/README.md) — Pointer: which files in the gitignored scripts/ are tracked; details in docs/development/scripts.md

## Deployment

- [coolify](deployment/coolify.md) — Production deployment: Coolify resource, containers and limits, host data paths, env, post-deploy checks
- [edge-and-throttling](deployment/edge-and-throttling.md) — Cache headers per route, the global throttler, CF_ORIGIN_SECRET and which client address each limiter trusts
- [container-memory-limits](deployment/container-memory-limits.md) — Open owner decision (PAR-1054): container memory limits vs the 29 GB host, measure-first, recommendations
- [backup](deployment/backup.md) — Nightly NAS backup: pg_basebackup, CatBoost and nf models, install on the host, restore steps
- [cors](deployment/cors.md) — Which browser origins get CORS headers, CORS_ORIGINS, and why only no-store endpoints are safe cross-origin

## Troubleshooting

- [common-issues](troubleshooting/common-issues.md) — Known symptoms with cause and fix: stale stats, crowd level, park day, ML errors, schedule rows, cron outcome
- [db-health-runbook](troubleshooting/db-health-runbook.md) — Production DB checks: prod-psql.sh limits (§0), sizes, indexes, non-blocking index drop, slow logs, OOM
- [timescaledb-lock-queues](troubleshooting/timescaledb-lock-queues.md) — Why a TimescaleDB job queued for ACCESS EXCLUSIVE took the API down (2026-10), the fixes, diagnosis SQL
- [nightly-db-maintenance](troubleshooting/nightly-db-maintenance.md) — Nightly DB windows: 04:50 VACUUM, TFT retention, Sunday REINDEX, cleanup-old vs compressed chunks, XID age

## History

- [changelog](changelog.md) — versioned changes; `pnpm changelog:merge` folds the fragments in at release time
- [changelog.d/](changelog.d/README.md) — one fragment per pull request: file name, first line, the two commands
