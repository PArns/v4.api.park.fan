# Park Fan API - AI Knowledge Bootstrap

> **Purpose**: This document bootstraps AI knowledge for the Park Fan API codebase.
> **Detailed Documentation**: See the `docs/` directory for in-depth guides.


> **Documentation Strategy**: This file is an **Index**. When adding new knowledge:
> 1. Create a detailed markdown file in the `docs/` directory (e.g., `docs/troubleshooting/my-issue.md`).
> 2. Link it in the "Documentation Index" below.
> 3. Keep this file (`claude.md`) concise.
> 4. Record notable changes in [Changelog](docs/changelog.md) with version and date.

---

## ✅ Open TODOs
- [todo.md](todo.md) – Active follow-ups (e.g. the ride-profile safety nets that went with the seed: nothing validates glossary term ids, nothing evicts caches after a curation write).

## 📚 Documentation Index

### 📋 Changelog
- [Changelog](docs/changelog.md) – Versioned changes (date, version, added/changed/fixed).

### 🏗️ Architecture & Infrastructure
- [System Overview](docs/architecture/system-overview.md) - High-level component design.
- [Job Queues & Processors](docs/architecture/job-queues.md) - Background BullMQ infrastructure.
- [Data Ingestion](docs/architecture/data-ingestion.md) - Multi-source data pipelines.
- [Schedule Sync & Calendar](docs/architecture/schedule-sync-and-calendar.md) - Opening hours sync (ThemeParks Wiki), on-demand refresh, calendar first-request slowness.
- [Calendar, Schedule & ML Rules](docs/architecture/calendar-schedule-and-ml-rules.md) - Status/crowd rules (past vs future, UNKNOWN vs CLOSED), schedule sync, ML alignment.
- [Caching Strategy](docs/architecture/caching-strategy.md) - Redis keys and TTLs, the outside-in publishing order (evict here before telling the frontend, and again after the un-purgeable CDN window), and the park open/close webhook that keeps the frontend's day-old park snapshot from claiming a park has no shows today.
- [Location Resolution & GeoIP](docs/architecture/location-resolution.md) - User location from lat/lng or IP (GeoLite2-City); used by nearby and favorites.
- [Attraction Status & Seasonality](docs/architecture/attraction-status-and-seasonality.md) - **2026-08-15/17**: who owns which cell and what an absent fact may become. The three status regimes (real feed / free-flow `open_with_park` / `UNKNOWN` when no source reports a ride), `is_seasonal` vs `season_months` and why months derived from under a year of history are just our recording window (`MIN_OBSERVED_DAYS`), the two-writers curated-column rule, the 73-day `detect-seasonal` outage and the ThemeParks.wiki feed drops. Also: identity comes from the externalId and not the slug (§4a), review marks so a settled verdict is not re-litigated (§4b), and the duplicate-detection traps — map numbers in ride names, one source naming two things alike. Includes diagnostic SQL.
- [Weather](docs/architecture/weather.md) - Open-Meteo sync (16-day forecast), park timezone handling, why parks may have empty weather (missing coordinates).

### 📊 Analytics & Logic
- [Crowd Levels](docs/analytics/crowd-levels.md) - The core logic for crowd calculations.
- [Typical-Day-Peak Baseline](docs/analytics/crowd-level-typical-day-peak.md) - Calendar crowd calibration: a day's peak ÷ a typical day's peak (median of daily peaks). Why P90/P50 and pooled-P90 fail; ML alignment; deploy steps.
- [Headliner Identification](docs/analytics/headliner-logic.md) - How attractions are selected for baselines.
- [Sparklines](docs/analytics/sparklines.md) - Wait-time history for ride cards: two-layer API (`getBatchAttractionWaitTimeHistory` vs `getAttractionSparklinesBatch`), when to use which, and park-timezone handling.

### 🤖 Machine Learning
- [PCN Intraday Review](docs/ml/pcn-intraday-review.md) - **2026-07-02**: Befunde & Priorisierung zum neuen Intraday-Modell — Crowd-Level-Quantil-Regression im Champion-Swap (q0.5 statt q0.8), Scorer-Rolling-Window verzerrt die Shadow-Boards (pcn + shape), `pcn_forecasts` ohne Retention, 548-Tage-Fetch pro Forecast-Tick, GraphWaveNet-Receptive-Field = 1 h, fehlende DOW/Holiday/Wetter-Kanäle; plus allgemeine ML-Empfehlungen (KPIs aufs servierte Modell, Shape offline-vs-live reconcilen). **§8: Punkte 1–5 umgesetzt (gleicher PR) — inkl. einmaligem Board-Reset-SQL + Kanal-Evolutions-Contract.**
- [Model Overview](docs/ml/model-overview.md) - CatBoost model, features, and training. Schedule/status behaviour: [Calendar, Schedule & ML Rules](docs/architecture/calendar-schedule-and-ml-rules.md).
- [Quantile Serving & Calibration](docs/ml/quantile-serving-and-calibration.md) - **2026-06-17**: which quantile becomes which user-facing number (CatBoost q0.5 display / q0.8 crowd / q0.95 uncertainty-only), the non-crossing monotonic fix, `predicted_peak` = E[daily-P90] (median forecast of a P90 target, NOT P90-of-distribution), and the single-flight stampede guard on the serving rebuild.
- [Performance Optimizations](docs/ml/performance-optimizations.md) - ML service caching, query optimization, and vectorization (60-90% faster).
- [Prediction Quality Issues](docs/ml/prediction-quality-issues.md) - Known bugs and fixes (5-min prediction bug, weekend underprediction, feature importance analysis).
- [Training Roadmap](docs/ml/training-roadmap.md) - Next training steps, UNKNOWN park data strategy, known issues and fix plans.
- [TFT vs CatBoost — clean comparison & TFT optimization](docs/ml/tft-vs-catboost-clean-comparison.md) - **2026-05-30**: clean daily scoreboard (symmetric durable snapshot + matched population — the raw board overstated TFT), intraday 15-min nowcast backtest (TFT beats naive baselines but no busy-tail edge; occupancy hist_exog doesn't help; quantile-forcing does at a quiet cost), signal-not-force + feed-not-remove. Stage-2 settings/algo bake-off deferred.
- [TFT vs CatBoost — daily forecast split](docs/ml/neuralforecast-tft-evaluation.md) - **PRODUCTION SPLIT (2026-05-24)**: TFT (nf-service) serves the near-term daily calendar (≤30d, headliners; ~2× better on busy peaks); CatBoost serves far-daily (31–365) + intraday 15-min slots. Loss=studentt (quantile + weather/holiday-dist/dow covariates measured & rejected). **Re-evaluate every few weeks** (next ~2026-06-14) as history grows. See the doc's "FINAL DECISION" section.

### 💾 Database
- [Schema & Entities](docs/database/schema.md) - Postgres schema and TimescaleDB usage.

### 🔐 Admin
- [Admin Authentication](docs/admin/authentication.md) - Accounts, opaque Redis sessions, scrypt passwords, TOTP, the two login rate-limit buckets, and the deprecated shared pass (`ADMIN_LEGACY_PASS`) with its switch-off.
- [Curation](docs/admin/curation.md) - The curated columns, the field-descriptor contract the editor is generated from, the four-step publish order (write → evict Redis → revalidate → revalidate again after the CDN window), and undo.

### 💻 Development
- [Setup Guide](docs/development/setup.md) - Local development instructions.
- [Date & Time Rules](docs/development/datetime-handling.md) - **CRITICAL**: Timezone handling rules.
- [Timezone Audit](docs/development/timezone-audit.md) - Audit of all time operations against park timezone (2026-02-08).
- [Scripts Overview](docs/development/scripts.md) - Script categories and npm-run commands.
- [Full-DB Validation Checklist](docs/development/full-db-validation-checklist.md) - **2026-06-17**: post-deploy/staging checks for the P50/P90 + caching work — crowd-level calibration invariants, the new SQL (per-attraction & historical-stats typical-day-peak), cache invalidation, single-flight, quantile monotonicity. The dev/CI container has no DB, so these self-calibrating changes are verified against real parks here.

### 🖥️ Frontend
- [Precomputed best-days endpoint (`/best-days`)](docs/frontend/best-days-endpoint.md) - Lean today→+90d projection (status/crowd/holiday flags + optional weekday aggregate) served from a materialized Redis snapshot the calendar warmup refreshes; p99 < 300 ms, CDN-cached, fires the on-change revalidation webhook.
- [Calendar: status (UNKNOWN vs CLOSED)](docs/frontend/calendar-schedule-status.md) - How to display opening hours and status in the calendar UI.
- [Parks we cannot read (`liveWaitTimes`)](docs/frontend/live-wait-times-availability.md) - Parks that publish wait times only inside their own app/WLAN (Hansa-Park). Curated in `parks/data/live-wait-time-sources.ts`; `available: false` is about the source, not freshness. The API skips the optimistic ride fallback (rides read `UNKNOWN`), rates crowds `unknown` and withholds forecasts — but a client that ignores the flag still renders an empty park as a quiet one.
- [The park's day shape (`/stats/hourly`)](docs/frontend/park-hourly-profile.md) - Median and busy wait per hour of the operating day, ride by ride, in ~2 KB where the attraction detail endpoint costs ~53 KB per ride. `hours` is derived from the data (an hour needs 10 measured days to be a column), the ranking is by each ride's **busiest hour** not its daily average, and the bucket is read in the park's timezone. Also documents the `topAttractions` sample floor that stopped a one-day average from leading a park's top-ten.
- [Ride P50/P90 stats (`typicalWaits`)](docs/frontend/ride-typical-waits.md) - The typical (P50) vs busy (P90) peak-wait stats on the attraction detail endpoint: shape, `displayable` gate, weekday/weekend + per-day breakdown, record peak.
- [Ride ↔ Glossary link (`rideProfile`)](docs/frontend/ride-glossary-link.md) - Curated per-ride track figures, ride type, manufacturer and opening year, stored as **glossary term ids** so a ride page can link into the glossary and the glossary can list the rides that feature a term. **The `attraction_ride_profiles` rows ARE the source of truth** — edited directly in the DB, no seed file and no apply job; reverse lookup at `/v1/glossary/terms/:termId/attractions`.
- [Queue-jump passes (`fastPass`)](docs/frontend/fast-pass.md) - The curated fast-pass product per ride: `{ name, price, currency, termId }`. The name is a park-wide brand (Phantasialand: QuickPass) with a per-ride override; **`price: 0` means free** (Europa-Park's Virtual Line) and `null` means unknown, so never test it for truthiness. An absent object covers both "nobody checked" and "the park sells none" — never render it as "no fast pass". Composed in the frontend, because the currency format is a locale decision.
- [One day, ride by ride (`/plan/day`)](docs/frontend/plan-day-endpoint.md) - The trip-planner series: per-ride hourly waits for one date plus that day's context. `tier` is derived from the curves that were built (never from the distance), a day inside the model's 24-hour window is part measured and part composed with `source` on the hours that differ, `dayPeak` is the same statistic on every tier, and past the operator's publishing horizon the open window falls back to measured hours (`hoursSource: "observed"`).
- [Stored plans & web push (`/v1/trips`, `/v1/push`)](docs/frontend/trips-and-push.md) - The id IS the credential (96 random bits, no accounts), full-replace writes, the payload floor that keeps an unauthenticated write endpoint from being a file host, and the push contract: ask `GET /v1/push` before offering the switch, endpoints only at known push services, one topic (`next-up`) and the language stored per subscriber.
- [Severe-weather warnings (`weather.warnings`)](docs/frontend/weather-warnings.md) - The `WeatherWarningDto[]` on the park weather + nowcast (MeteoGate/DWD): shape, de/en localization, severity→colour, validity, banner rendering, EU-only coverage.

### 🔧 Troubleshooting
- [Common Issues](docs/troubleshooting/common-issues.md) - Stale cache, occupancy, timezone, ML.
- [DB Health Runbook](docs/troubleshooting/db-health-runbook.md) - Copy-paste SQL for table sizes, unused indexes, dead tuples, slow queries, OOM checks.

### 🚀 Deployment
- [Coolify Deployment](docs/deployment/coolify.md) - Production deployment guide.
- [Backup Strategy](docs/deployment/backup.md) - Daily DB + ML model backups to Samba NAS, retention, and restore steps.

---

## 🏗️ Project Overview

**Stack**: NestJS (TypeScript) API + Python ML Microservice + PostgreSQL (TimescaleDB) + Redis
**Domain**: Theme Park wait time tracking, predictions, and analytics.

### Directory Structure

```
src/
├── analytics/         # 🧠 P50, Crowd Levels (The Brain)
├── parks/             # Park entities & logic
├── attractions/       # Attraction entities & logic
├── queue-data/        # Raw wait time ingestion
├── geoip/             # GeoLite2-City for IP → coordinates (nearby, favorites)
├── ml/                # ML Service Client
└── common/            # Shared Utilities & Types

ml-service/            # 🐍 Python CatBoost Service
```

---

## ⚠️ Critical Rules (DO NOT IGNORE)

### 1. Park Timezone (ALWAYS)

**⚠️ NEVER use `new Date()` directly for business logic.** Parks are global; "today" and date ranges must be in **park timezone** for all time operations (schedule, calendar, gap-fill, ML, analytics).

- **Detailed Guide**: [Date & Time Handling](docs/development/datetime-handling.md)
- Always use `park.timezone` and the date utils for "today", ranges, and formatting.
- **Utils**: `src/common/utils/date.util.ts` (`getCurrentDateInTimezone`, `getStartOfDayInTimezone`, `formatInParkTimezone`).

### 2. TypeORM AutoSync

- `synchronize: true` is ON in development.
- Entity changes immediately alter the DB schema.

### 3. Unified Crowd Levels (Typical-Day-Peak Daily / Ratio-vs-P50 Live)

- **Detailed Guide**: [Typical-Day-Peak](docs/analytics/crowd-level-typical-day-peak.md) · [Crowd Levels](docs/analytics/crowd-levels.md)
- **Boundary**: daily/historical aggregates compare a day's peak to a **typical day's peak**; point-in-time/live signals use **ratio-vs-P50**. Never mix the two on one surface.
- **Calendar daily**: a day's value = **AVG across headliner rides** of each ride's daily P90; denominator = **typical-day-peak baseline** = the **median over operating days** of that same day value (548-day window, headliner-only). Future/predicted days use the same baseline (AVG of predicted headliner waits ÷ typical-day-peak).
  - **Predicted-wait source (2026-05-24)**: those predicted headliner waits come from **TFT for days 1–30** (nf-service, ~2× better on busy peaks), **CatBoost for days 31–365** — merged in `MLService.getServingDailyPredictions` (serving only; the writer stays pure CatBoost). See [TFT vs CatBoost split](docs/ml/neuralforecast-tft-evaluation.md).
  - **Formula**: `(day_value / typical_day_peak) * 100` — 100% = a statistically typical day = `moderate`; busy seasons (Wintertraum, Easter, promos) correctly read high/very_high/extreme. The pooled P90 baseline is NOT used (it's inflated by the busiest season and compresses the top).
- **Live overview / `calculateParkOccupancy` (ratio-vs-P50)**: the **baseline-weighted mean** across headliners reporting in the last 60 min. The calendar "today" cell and hourly within-a-day predictions stay on ÷P50 too.
  - **Formula**: `Σ latest_wait / Σ attraction_p50 * 100` (`getHeadlinerLoad`). Falls back to `avg(latest) / park_p50 * 100` when no per-ride baselines exist.
  - **The ML feature is a DIFFERENT function.** `park_occupancy_pct` comes from `getCurrentOccupancy`, which deliberately stays on the older park-wide `avg(latest) / park_p50 * 100` shape — trained models depend on that exact feature distribution, so moving it to the weighted mean needs a retrain cycle. Don't "unify" the two.
  - **Sum the minutes, then divide — never average or take a percentile of the per-ride ratios.** A percentile across ratios is an extreme-value estimator: over a ten-ride headliner set its P90 is just the second-busiest ride, it can only push the reading up, and a 10-minute-baseline ride outvotes a marquee. That is what made Phantasialand read `high` (123%) with Taron at 20/45 min. Same rule applies to any new cross-ride aggregate.
  - **`breakdown.typicalAvgWait`** must be the baseline of exactly the rides in `currentAvgWait`, so the displayed pair divides out to the displayed percentage.
- **No made-up ratings**: anything that cannot rate against a real baseline emits **`unknown`**, never a placeholder `moderate` — `rateOrUnknown`, the `isParkRatable` gate, `getLoadRating(_, baseline<=0)`, `calculateParkOccupancy` with no live sample at all, and the callers of `getAttractionCrowdLevel`. A 0-minute wait against a real baseline is a walk-on, not missing data, and still rates (`very_low`) — only an *absent* wait is "no data".
  - Consumers must read the **gated** value (`occupancy.crowdLevel`), never re-derive a tier from `occupancy.current`: the recompute bypasses the ratability gate, and at `current = 0` it silently yields `very_low`.
  - Every Swagger `enum:` for a crowd-level field comes from `CROWD_LEVEL_VALUES` / `CROWD_LEVEL_WITH_CLOSED_VALUES` (`common/types/crowd-level.type.ts`). Never hand-write the list — that drift is how `unknown` stayed out of the published contract while the API had been sending it for months.
- **No calendar fallback**: typical-day-peak is written atomically with P50/P90 (`park_p50_baselines.typicalDayPeak` + Redis), so a missing value means no usable baseline → `unknown`. P50 stays load-bearing (live + ML feature); P90 is computed for free but no longer a calendar reference.
- **Past and future days must carry the same statistic** where they share a response field. `headlinerForecast.avgWait` is a day-peak on both sides: forecasts come from a per-day MAX in `predict.py`, history from `getHeadlinerDailyPeaks` (per-ride day-P90). A mean on one side and a peak on the other reads as the park getting busier next week when only the statistic changed.

---

### 4. An Absent Fact Never Becomes a Confident One

- **Detailed Guide**: [Attraction Status & Seasonality](docs/architecture/attraction-status-and-seasonality.md)
- The `unknown` crowd-level rule (§3) is the same rule as these, and they have
  all been broken the same way — **our own bookkeeping served as somebody
  else's statement**:
  - A ride **no source reports** reads `UNKNOWN`, never `CLOSED`. Reverse-
    reconciliation's row records that our data stopped arriving, not that the
    operator shut the ride. ~140 attractions across ten parks read "closed" for
    weeks this way.
  - **`season_months` may not be derived from less than a year of watching**
    (`MIN_OBSERVED_DAYS` = 330). Below that the months are the observation
    window, not a season — every list in the database was one.
  - A **free-flow** attraction (`open_with_park`) is not seasonal just because
    its feed never says OPERATING; that is a playground's normal state.
- **Two writers, never one cell.** `curated_may_get_wet`, `curated_minimum_height`
  and `curated_stats` sit *beside* the synced column, never in it, because the
  sync overwrites its own cell on every run. Read via `resolveCuratedFacts`.
- **`AttractionStatus` includes `UNKNOWN`** and every Swagger `enum:` for it
  comes from `ATTRACTION_STATUS_VALUES` — hand-written lists are how `unknown`
  stayed out of the published contract for months.
- **Research, never recall.** Facts about real rides and parks (heights, wet
  flags, whether something is free-flow, seasons) come from the operator's own
  pages. A name pattern is not evidence.
- **Identity is the `externalId`, never the slug.** A slug is a frozen name that
  renames deliberately do not move, so 281 rows carry a slug that no longer
  matches — that is the system working. Resolve a disputed name against the
  upstream entity, not against what its slug implies.
- **A behavioural detector cannot remember.** Duplicate and retirement detection
  describe the feed, so a cleared candidate returns tomorrow unless the verdict
  is written to `attraction_review_marks`. Give it a `recheck_after` whenever the
  answer can change.

---

## 📦 Key Types

All shared types are in `src/common/types/`.

- **CrowdLevel**: `very_low` | `low` | `moderate` | `high` | `very_high` | `extreme` | `unknown` (no usable baseline — "keine Prognose")
- **ParkStatus**: `OPERATING` | `CLOSED`
- **AttractionStatus**: `OPERATING` | `CLOSED` | `DOWN` | `REFURBISHMENT` | `UNKNOWN` (no source reports this ride — an absence of information, never a closure)

---

## 📝 Coding Conventions

- **Naming**: `*ResponseDto`, `*RequestDto`, `/v1/resources`
- **Linting**: `npm run lint` (TS), `ruff check .` (Python)
- **Verification**: Ensure `npm run build` passes before finishing.
