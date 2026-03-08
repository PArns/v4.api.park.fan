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

### 📊 Analytics & Logic
- [P50 Crowd Levels](docs/analytics/p50-crowd-levels.md) - The core logic for crowd calculations.
- [Headliner Identification](docs/analytics/headliner-logic.md) - How attractions are selected for baselines.

### 🤖 Machine Learning
- [Model Overview](docs/ml/model-overview.md) - CatBoost model, features, and training. Schedule/status behaviour: [Calendar, Schedule & ML Rules](docs/architecture/calendar-schedule-and-ml-rules.md).
- [Performance Optimizations](docs/ml/performance-optimizations.md) - ML service caching, query optimization, and vectorization (60-90% faster).
- [Prediction Quality Issues](docs/ml/prediction-quality-issues.md) - Known bugs and fixes (5-min prediction bug, weekend underprediction, feature importance analysis).

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

### 🚀 Deployment
- [Coolify Deployment](docs/deployment/coolify.md) - Production deployment guide.

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

### 3. Unified Crowd Levels (P50 Baseline)

- **Detailed Guide**: [P50 Crowd Levels](docs/analytics/p50-crowd-levels.md)
- **Baseline**: Static P50 (Median) of **Headliner Attractions** (548-day window); attractions use per-ride P50 from `attraction_p50_baselines`.
- **Current**: Median wait time of operating attractions (park) or current wait (attraction).
- **Formula**: `(current / p50) * 100` (100% = Normal Day).
- **Never** mix P90 with P50 logic.

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
