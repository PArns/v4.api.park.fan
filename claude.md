# Park Fan API - AI Knowledge Bootstrap

> **Purpose**: This document bootstraps AI knowledge for the Park Fan API codebase.
> **Detailed Documentation**: See the `docs/` directory for in-depth guides.


> **Documentation Strategy**: This file is an **Index**. When adding new knowledge:
> 1. Create a detailed markdown file in the `docs/` directory (e.g., `docs/troubleshooting/my-issue.md`).
> 2. Link it in the "Documentation Index" below.
> 3. Keep this file (`claude.md`) concise.

---

## 📚 Documentation Index

### 🏗️ Architecture & Infrastructure
- [System Overview](docs/architecture/system-overview.md) - High-level component design.
- [Job Queues & Processors](docs/architecture/job-queues.md) - Background BullMQ infrastructure.
- [Data Ingestion](docs/architecture/data-ingestion.md) - Multi-source data pipelines.
- [Caching Strategy](docs/architecture/caching-strategy.md) - Redis keys and TTLs.

### 📊 Analytics & Logic
- [P50 Crowd Levels](docs/analytics/p50-crowd-levels.md) - The core logic for crowd calculations.
- [Headliner Identification](docs/analytics/headliner-logic.md) - How attractions are selected for baselines.

### 🤖 Machine Learning
- [Model Overview](docs/ml/model-overview.md) - CatBoost model, features, and training.

### 💾 Database
- [Schema & Entities](docs/database/schema.md) - Postgres schema and TimescaleDB usage.

### 💻 Development
- [Setup Guide](docs/development/setup.md) - Local development instructions.
- [Date & Time Rules](docs/development/datetime-handling.md) - **CRITICAL**: Timezone handling rules.

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
├── ml/                # ML Service Client
└── common/            # Shared Utilities & Types

ml-service/            # 🐍 Python CatBoost Service
```

---

## ⚠️ Critical Rules (DO NOT IGNORE)

### 1. Park Timezone (ALWAYS)

**⚠️ NEVER use `new Date()` directly.** Parks are global.

- **Detailed Guide**: [Date & Time Handling](docs/development/datetime-handling.md)
- Always use `park.timezone`.
- **Utils**: `src/common/utils/date.util.ts` (`getCurrentDateInTimezone`).

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
