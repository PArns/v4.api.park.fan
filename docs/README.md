# park.fan API — documentation map

Every page under `docs/`, one line each. Coding sessions start at [CLAUDE.md](../CLAUDE.md), which
routes a task to its rules and to the section below. The frontend's side of each contract lives in
the park.fan repo: [docs/api/](https://github.com/PArns/park.fan/tree/main/docs/api).

## Standing rules

Read [rules/README.md](rules/README.md) before any change; each rule is one page.

- [rules](rules/README.md) — Index of the backend's standing rules, one line each; start here before any change
- [absent-facts](rules/absent-facts.md) — Rule: an absent fact never becomes a confident one — UNKNOWN over CLOSED, curated beside synced
- [automated-retirement-and-identity](rules/automated-retirement-and-identity.md) — Rules for jobs that retire/merge/move rows, upstream identity, per-child try/catch, prod one-offs
- [crowd-levels](rules/crowd-levels.md) — Rule: typical-day-peak daily vs ratio-vs-P50 live, sum minutes then divide, unknown never moderate
- [park-timezone](rules/park-timezone.md) — Rule: park-local "today" and dates via date.util.ts, never new Date() for business logic
- [publish-outside-in](rules/publish-outside-in.md) — Rule: write, evict Redis, revalidate the frontend, revalidate again after CDN_SETTLE_MS
- [the-entities-are-the-schema](rules/the-entities-are-the-schema.md) — Rule: no migrations, synchronize runs in production — nullable columns, no renames, text over enums

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
- [covariate-source](architecture/covariate-source.md) — Design for as-of covariates (PAR-1061); two write-only logs built, the rest proposed

### Attraction status and seasonality

- [attraction-status](architecture/attraction-status/README.md) — Index of attraction status, seasonality, identity and duplicate pages; old § numbers mapped
- [duplicates](architecture/attraction-status/duplicates.md) — Attraction and park duplicate detection: map numbers, sharedPoint, merge gate, attractionsAgree
- [feed-drops](architecture/attraction-status/feed-drops.md) — When a cluster of rides goes quiet: three events, the cluster query, the two checks
- [identity](architecture/attraction-status/identity.md) — externalId vs slug, name grouping in the park payload, which name-group row payload and sitemap keep
- [maze-rule](architecture/attraction-status/maze-rule.md) — The MAZE rule in detect-seasonal: name tokens, autumn season, scare-zone rubric, not_a_maze
- [reclassification-and-absence](architecture/attraction-status/reclassification-and-absence.md) — How the children sync retires/un-retires rows: entityType changes, absence, re-issues, park moves
- [review-marks](architecture/attraction-status/review-marks.md) — attraction_review_marks and how to triage retirement candidates before researching them
- [seasonality](architecture/attraction-status/seasonality.md) — is_seasonal/season_months, MIN_OBSERVED_DAYS, detect-seasonal, curating free-flow seasons
- [status-regimes](architecture/attraction-status/status-regimes.md) — Why a ride reads OPERATING/CLOSED/UNKNOWN: feed, free-flow, source absence, fallback, silent park

## API contracts for clients

What a field means, when it is absent, and what a client may not conclude from it.

- [frontend](frontend/README.md) — Index of the API contracts clients read, plus the conventions they share (null keys stripped, unknown)
- [attraction-kind](frontend/attraction-kind.md) — attractionKind (RIDE/TRANSPORT/SHOW/WALKTHROUGH/MAZE): null is not RIDE, independent of attractionType
- [best-days-endpoint](frontend/best-days-endpoint.md) — /best-days: the precomputed today→+90 d projection, its 13 h snapshot, rebuild and revalidation webhook
- [calendar-schedule-status](frontend/calendar-schedule-status.md) — Calendar status for clients: UNKNOWN vs CLOSED, isEstimated, hasOperatingSchedule, scheduleCoverage
- [effective-status](frontend/effective-status.md) — A ride's status vs effectiveStatus: which to read, how each is derived, what a client may not conclude
- [fast-pass](frontend/fast-pass.md) — fastPass: name, price (0 = free), currency, termId; how to render it and its absence
- [indoor-outdoor](frontend/indoor-outdoor.md) — indoorOutdoor for the rain plan: indoor / covered_queue / outdoor, null is neither
- [live-wait-times-availability](frontend/live-wait-times-availability.md) — liveWaitTimes: parks we cannot read, what the API withholds, silent feeds, single UNKNOWN rides
- [not-run-today](frontend/not-run-today.md) — notRunToday: a ride that has not run yet today — when set, what lastRunAt is, never an outage
- [page-views](frontend/page-views.md) — /v1/page-views most-read counter: kinds, per-locale keys, caps, privacy, endpoints, measured storage
- [park-hourly-profile](frontend/park-hourly-profile.md) — /stats/hourly day shape: window params, shape, which hours/rides make the table, the OFFSET 0 fence
- [park-stats-and-peak-time](frontend/park-stats-and-peak-time.md) — /stats history (by month/weekday, extreme days, top rides) and the peak-hour fields on the park payload
- [push](frontend/push.md) — /v1/push*: subscribe, topics (next-up, morning-plan, rain-soon), ride alerts, show follows, quiet hours
- [retired-parks](frontend/retired-parks.md) — retiredAt on a park: rendering, which lists drop it, POST /v1/admin/retire-parks
- [ride-day-curve](frontend/ride-day-curve.md) — /stats/day: one ride's P25/P50/P90, today, forecast and how to draw the forecastError band
- [ride-glossary-link](frontend/ride-glossary-link.md) — rideProfile and the glossary→rides lookup (sort=popularity); curating profiles; sourcing rules
- [ride-live-crowd-level](frontend/ride-live-crowd-level.md) — A ride's live crowdLevel (measurement, unknown when unratable) and bestVisitTimes' current slot
- [ride-typical-waits](frontend/ride-typical-waits.md) — typicalWaits: a ride's typical (P50) vs busy (P90) daily peak wait, the displayable gate
- [seasonality](frontend/seasonality.md) — isSeasonal/seasonMonths/isCurrentlyInSeason: three values, status fallback, counters and the SQL twin
- [trips](frontend/trips.md) — /v1/trips stored plans: id as credential, full replace, payload floor, expiry, write limits and 429s
- [weather-warnings](frontend/weather-warnings.md) — weather.warnings (MeteoGate/DWD): where, shape, de/en text, severity colours, EU-only coverage
- [works-period](frontend/works-period.md) — worksPeriod: a curated rebuild window — shape, never an outage, wording, the client's "today" check

### /plan/day

- [plan-day](frontend/plan-day/README.md) — /plan/day contract entry: request, caching, response fields, context incl. early entry; links the parts
- [accuracy](frontend/plan-day/accuracy.md) — /plan/day accuracy.basis, expectedError grid and leadTimeMae: how wrong the day typically is
- [availability](frontend/plan-day/availability.md) — /plan/day: rides out of season or in a works window are absent; ridesUnavailable reasons; coverage job
- [midnight](frontend/plan-day/midnight.md) — /plan/day on a day past midnight: unfolded hours, unfoldedCloseHour, the four server-side fixes
- [opens-at](frontend/plan-day/opens-at.md) — /plan/day opensAt, opensAtConfidence and runsOnWeekdays: when a ride (not the gate) opens and runs
- [rides](frontend/plan-day/rides.md) — /plan/day ride fields, minimumHeight/mayGetWet, which rides a day carries, the SHAPE_RIDES cap
- [shows](frontend/plan-day/shows.md) — /plan/day shows: scheduled vs projected, how far ahead, past-midnight performances, positions
- [slots-and-h5](frontend/plan-day/slots-and-h5.md) — /plan/day 15-minute slots and the H5 composer behind PLAN_DAY_H5_COMPOSER; bench vs production
- [tiers](frontend/plan-day/tiers.md) — /plan/day tier ladder, climatology look-back, per-hour source, dayPeak, observed opening hours

## Analytics

- [crowd-levels](analytics/crowd-levels.md) — Crowd-level mechanism: formulas per surface, thresholds, baselines, jobs, ratability gate, calibration checks
- [headliner-logic](analytics/headliner-logic.md) — How headliners are chosen (tiers, data filters, tier-1 cap) and what to check when a crowd level looks wrong
- [sparklines](analytics/sparklines.md) — Today's per-ride wait-time arrays: content, which batch function to call, per-park effective start time
- [smart-gaps](analytics/smart-gaps.md) — Opening hours and open/closed inferred from ride activity for parks without a schedule; isEstimated flags
- [data-recalculation](analytics/data-recalculation.md) — What to rerun after a calculation change: backfill-stats, baseline recompute, cache flush, in order

### Ride downtime

- [ride-downtime](analytics/ride-downtime/README.md) — Ride downtime index: what is answered vs refused, what is built and where, links to the five sub-pages
- [closure-gap-signal](analytics/ride-downtime/closure-gap-signal.md) — The closed_gap signal for parks that never emit DOWN: filters, standing closures, window rules
- [publishing](analytics/ride-downtime/publishing.md) — Live outage line, park regimes, per-ride gates and withheld reasons, published fields, non-negotiables
- [reconstruction](analytics/ride-downtime/reconstruction.md) — The outage interval, exposure denominator, downtime tables, nightly job, deadlines, staged fill, traps
- [recovery-estimate](analytics/ride-downtime/recovery-estimate.md) — How much longer a running outage lasts: measured curve, calibration, per-park fallback, recoveryWindow
- [refused](analytics/ride-downtime/refused.md) — Why no forecast of the next outage, the 2026-10-22 erasure measurement, and the rejected-ideas list

## Machine learning

Start at [model-overview](ml/model-overview.md): which model serves which surface today.

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

### Forward archive

- [forward-archive](ml/forward-archive/README.md) — Forward archive of served curves: what it answers, code, queues, tables, when scores appear; index
- [capture](ml/forward-archive/capture.md) — Archive capture: origins, surfaces 1–8, curve columns, source codes, size/DB impact, retention
- [chronos-serving](ml/forward-archive/chronos-serving.md) — Chronos-2 served on 45–120 min slots (PAR-911): lead routing, serving cache, switch-on checklist, rollback
- [chronos-shadow](ml/forward-archive/chronos-shadow.md) — Chronos-2 shadow (PAR-852): variants, covariates, information cut, paired board, kill switches and cost
- [model-comparison](ml/forward-archive/model-comparison.md) — Admin model comparison (PAR-855): paired scoring, shadow levels, questions, cell states, verdicts, endpoints
- [scoring](ml/forward-archive/scoring.md) — Archive truth (heartbeats dropped), daily scores per use case × lead × source, admin board, not built yet

### ml-bench (offline benchmark)

- [ml-bench](ml/ml-bench/README.md) — ml-bench offline benchmark: what it is, layout, results dirs, verdict per model family; start here
- [harness](ml/ml-bench/harness.md) — ml-bench export, 15-min truth, information cut, baselines, D1–D9, outputs and the win / usable-horizon rules
- [results-baselines](ml/ml-bench/results-baselines.md) — Reference baseline run results: H5, TFT level only d0–d2, persistence, season split, heartbeat effect
- [results-neuralforecast](ml/ml-bench/results-neuralforecast.md) — TiDE/NHITS/TSMixerx results (PAR-829) and TiDE vs Chronos-2 head to head (PAR-869); not pursued
- [running](ml/ml-bench/running.md) — Adding a plug-in model, running/reporting on celestrial, GPU lock and quiet window, NF precompute, queued runs
- [served-arm](ml/ml-bench/served-arm.md) — ml-bench served arm (PAR-1043): candidates vs archived served curves, weekly accumulation, verdict generator

### Foundation models

- [foundation-models](ml/foundation-models/README.md) — Foundation models in ml-bench: status and licence per model, shared adapter, licence enforcement, lessons
- [chronos-2](ml/foundation-models/chronos-2.md) — Chronos-2 offline results (UC1, planner, layout, covariates, D8/D9), production status, LoRA variant
- [granite-tabpfn](ml/foundation-models/granite-tabpfn.md) — PatchTST-FM-r2, TTM-r2, TabPFN-TS (PAR-1067): licence verdicts with quotes, covariate support, adapters
- [timesfm](ml/foundation-models/timesfm.md) — TimesFM 3.0 (ruled out: no q95, non-production licence) and TimesFM 2.5 adapter (XReg covariates, not run)
- [tirex-toto](ml/foundation-models/tirex-toto.md) — TiRex-2 and Toto 2.0 results (PAR-853): drop for serving, keep TiRex-2 as plug-in; construction traps

## Database

- [schema](database/schema.md) — Where the schema is defined, core/precomputed/forecast tables, wait_time_predictions write path

## Admin

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
- [pcn-service/README](../pcn-service/README.md) — pcn-service directory map: endpoints and schedule, files, local run commands; model in docs/ml/pcn.md

## Deployment

- [coolify](deployment/coolify.md) — Production deployment: Coolify resource, containers and limits, host data paths, env, post-deploy checks
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
