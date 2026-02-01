# Park Fan API - AI Knowledge Bootstrap

> **Purpose**: This document bootstraps AI knowledge for the Park Fan API codebase.
> **Detailed Documentation**: See the `docs/` directory for in-depth guides.


> **Documentation Strategy**: This file is an **Index**. When adding new knowledge:
> 1. Create a detailed markdown file in the `docs/` directory (e.g., `docs/troubleshooting/my-issue.md`).
> 2. Link it in the "Documentation Index" below.
> 3. Keep this file (`claude.md`) concise.

---

## 📚 Documentation Index

- **Architecture**: [System Overview](docs/architecture/system-overview.md)
- **Database**: [Schema & Entities](docs/database/schema.md)
- **Analytics**: [P50 Crowd Levels](docs/analytics/p50-crowd-levels.md)
- **Machine Learning**: [Model & Training](docs/ml/model-overview.md)
- **Development**: [Setup & Guidelines](docs/development/setup.md) | [Date & Time Handling](docs/development/datetime-handling.md)
- **Deployment**: [Coolify Guide](docs/deployment/coolify.md)

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

- **Baseline**: Static P50 (Median) of **Headliner Attractions** (548-day window).
- **Current**: Median wait time of operating attractions.
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
