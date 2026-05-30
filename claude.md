# Park Fan API - AI Knowledge Bootstrap

> **Purpose**: This document bootstraps AI knowledge for the Park Fan API codebase.
> **Detailed Documentation**: See the `docs/` directory for in-depth guides.


> **Documentation Strategy**: This file is an **Index**. When adding new knowledge:
> 1. Create a detailed markdown file in the `docs/` directory (e.g., `docs/troubleshooting/my-issue.md`).
> 2. Link it in the "Documentation Index" below.
> 3. Keep this file (`claude.md`) concise.
> 4. Record notable changes in [Changelog](docs/changelog.md) with version and date.

---

## 📚 Documentation Index

### 📋 Changelog
- [Changelog](docs/changelog.md) – Versioned changes (date, version, added/changed/fixed).

### 🏗️ Architecture & Infrastructure
- [System Overview](docs/architecture/system-overview.md) - High-level component design.
- [Job Queues & Processors](docs/architecture/job-queues.md) - Background BullMQ infrastructure.
- [Data Ingestion](docs/architecture/data-ingestion.md) - Multi-source data pipelines.
- [Schedule Sync & Calendar](docs/architecture/schedule-sync-and-calendar.md) - Opening hours sync (ThemeParks Wiki), on-demand refresh, calendar first-request slowness.
- [Calendar, Schedule & ML Rules](docs/architecture/calendar-schedule-and-ml-rules.md) - Status/crowd rules (past vs future, UNKNOWN vs CLOSED), schedule sync, ML alignment.
- [Caching Strategy](docs/architecture/caching-strategy.md) - Redis keys and TTLs.
- [Location Resolution & GeoIP](docs/architecture/location-resolution.md) - User location from lat/lng or IP (GeoLite2-City); used by nearby and favorites.
- [Weather](docs/architecture/weather.md) - Open-Meteo sync (16-day forecast), park timezone handling, why parks may have empty weather (missing coordinates).

### 📊 Analytics & Logic
- [Crowd Levels](docs/analytics/crowd-levels.md) - The core logic for crowd calculations.
- [Typical-Day-Peak Baseline](docs/analytics/crowd-level-typical-day-peak.md) - Calendar crowd calibration: a day's peak ÷ a typical day's peak (median of daily peaks). Why P90/P50 and pooled-P90 fail; ML alignment; deploy steps.
- [Headliner Identification](docs/analytics/headliner-logic.md) - How attractions are selected for baselines.
- [Sparklines](docs/analytics/sparklines.md) - Wait-time history for ride cards: two-layer API (`getBatchAttractionWaitTimeHistory` vs `getAttractionSparklinesBatch`), when to use which, and park-timezone handling.
- [Data Recalculation & Correction Jobs](docs/analytics/data-recalculation.md) - Manual backfills for stats and baselines.
- [Smart Gaps: Hours & Status Inference](docs/analytics/smart-gaps.md) - Algorithm for reconstructing historical hours and seasonal detection.

### 🤖 Machine Learning
- [Model Overview](docs/ml/model-overview.md) - CatBoost model, features, and training. Schedule/status behaviour: [Calendar, Schedule & ML Rules](docs/architecture/calendar-schedule-and-ml-rules.md).
- [Performance Optimizations](docs/ml/performance-optimizations.md) - ML service caching, query optimization, and vectorization (60-90% faster).
- [Prediction Quality Issues](docs/ml/prediction-quality-issues.md) - Known bugs and fixes (5-min prediction bug, weekend underprediction, feature importance analysis).
- [Training Roadmap](docs/ml/training-roadmap.md) - Next training steps, UNKNOWN park data strategy, known issues and fix plans.
- [Busy-day Prediction Challenger](docs/ml/busy-day-prediction-challenger.md) - Living experiment: fixing busy/holiday future under-prediction (feature-forcing, sample-weighting, quantile/uncertainty levers). Champion/challenger log.
- [TFT vs CatBoost — clean comparison & TFT optimization](docs/ml/tft-vs-catboost-clean-comparison.md) - **2026-05-30**: clean daily scoreboard (symmetric durable snapshot + matched population — the raw board overstated TFT), intraday 15-min nowcast backtest (TFT beats naive baselines but no busy-tail edge; occupancy hist_exog doesn't help; quantile-forcing does at a quiet cost), signal-not-force + feed-not-remove. Stage-2 settings/algo bake-off deferred.
- [TFT vs CatBoost — daily forecast split](docs/ml/neuralforecast-tft-evaluation.md) - **PRODUCTION SPLIT (2026-05-24)**: TFT (nf-service) serves the near-term daily calendar (≤30d, headliners; ~2× better on busy peaks); CatBoost serves far-daily (31–365) + intraday 15-min slots. Loss=studentt (quantile + weather/holiday-dist/dow covariates measured & rejected). **Re-evaluate every few weeks** (next ~2026-06-14) as history grows. See the doc's "FINAL DECISION" section.

### 💾 Database
- [Schema & Entities](docs/database/schema.md) - Postgres schema and TimescaleDB usage.

### 💻 Development
- [Setup Guide](docs/development/setup.md) - Local development instructions.
- [Date & Time Rules](docs/development/datetime-handling.md) - **CRITICAL**: Timezone handling rules.
- [Timezone Audit](docs/development/timezone-audit.md) - Audit of all time operations against park timezone (2026-02-08).
- [Scripts Overview](docs/development/scripts.md) - Script categories and npm-run commands.

### 🖥️ Frontend
- [Calendar: status (UNKNOWN vs CLOSED)](docs/frontend/calendar-schedule-status.md) - How to display opening hours and status in the calendar UI.

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
- **Live overview / `getCurrentOccupancy` (ratio-vs-P50)**: current short-window peak ÷ park P50 baseline. Also an ML feature. The calendar "today" cell and hourly within-a-day predictions stay on ÷P50 too.
  - **Formula**: `(current_peak / p50_baseline) * 100`.
- **No calendar fallback**: typical-day-peak is written atomically with P50/P90 (`park_p50_baselines.typicalDayPeak` + Redis), so a missing value means no baseline at all → neutral default. P50 stays load-bearing (live + ML feature); P90 is computed for free but no longer a calendar reference.

---

## 📦 Key Types

All shared types are in `src/common/types/`.

- **CrowdLevel**: `very_low` | `low` | `moderate` | `high` | `very_high` | `extreme`
- **ParkStatus**: `OPERATING` | `CLOSED`
- **AttractionStatus**: `OPERATING` | `CLOSED` | `DOWN` | `REFURBISHMENT`

---

## 📝 Coding Conventions

- **Naming**: `*ResponseDto`, `*RequestDto`, `/v1/resources`
- **Linting**: `npm run lint` (TS), `ruff check .` (Python)
- **Verification**: Ensure `npm run build` passes before finishing.
