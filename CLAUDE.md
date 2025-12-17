# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.

---

## 🤖 Claude Workflow - READ THIS FIRST

**BEFORE implementing any feature, you MUST:**

1. **Propose a Plan**: Outline approach, affected files, architecture decisions
2. **Verify Assumptions**: Ask clarifying questions if unclear
3. **Discuss Trade-offs**: Present options when multiple solutions exist
4. **Get Approval**: Wait for user confirmation before writing code
5. **Propose Changes**: If you see improvements, propose them first

**DO NOT** immediately start coding without proposing a plan first.

---

# park.fan API

**Tech Stack**: NestJS + PostgreSQL (TimescaleDB) + Redis + Bull Queue + TypeScript + Docker
**Status**: ✅ Phase 6.6 Complete (Multi-Source Integration) | 🔄 Phase 6.7 Planned (Processor Integration)

Real-time theme park data aggregation API with multi-source integration, ML predictions, and optimized data collection.

**Current Data**: **138 parks** (95 multi-source, 7 wiki-only, 36 qt-only), 135 geocoded (97.8%), **6,122 entities** (4,013 attractions + 807 shows + 1,302 restaurants), all data streams operational.

---

## Core Architecture

### 1. Database & Storage
- **PostgreSQL + TimescaleDB**: TypeORM with `synchronize: true` (migrations in production)
- **Time-Series**: 5 hypertables (`queue_data`, `forecast_data`, `weather_data`, `show_live_data`, `restaurant_live_data`) with compression
- **Delta Tracking**: Only save when Δ > 5 min or status changes (attractions, restaurants), or showtimes/status change (shows)
- **Composite PKs**: Required for TimescaleDB partitioning (e.g., `queue_data`: id + timestamp)

### 2. Module Structure

```
src/
├── analytics/              # Occupancy, percentiles & statistics
├── attractions/            # Attractions logic & endpoints
├── common/                 # Shared utils, interceptors, constants, redis
├── config/                 # Application configuration
├── database/               # TimescaleDB migrations & seeding
├── date-features/          # Date feature extraction (weekends, holidays)
├── destinations/           # Destinations (resort-level)
├── external-apis/          # Data Sources Integration
│   ├── data-sources/       # Orchestrator, Matcher, Interfaces
│   ├── themeparks/         # ThemeParks.wiki Client
│   ├── queue-times/        # Queue-Times.com Client
│   ├── geocoding/          # Google Geocoding
│   ├── open-meteo/         # Weather
│   └── nager-date/         # Holidays
├── health/                 # Health checks
├── holidays/               # Holiday data logic
├── ml/                     # Machine Learning module
│   ├── controllers/        # Dashboard, Accuracy, Health endpoints
│   ├── dto/                # ML-specific DTOs
│   └── services/           # Prediction & model services
├── parks/                  # Parks + weather + schedules
├── queue-data/             # Queue/wait times + forecasts entities
├── queues/                 # BullMQ background jobs
│   ├── processors/         # Job processors (sync, train, forecast)
│   └── schedulers/         # Cron job scheduling
├── restaurants/            # Restaurants logic
├── search/                 # Full-text search
└── shows/                  # Shows logic
```

**Entity Hierarchy**: `Destination → Parks → Attractions/Shows/Restaurants`

### 3. Multi-Source Architecture (New in Phase 6.6)
- **Orchestrator**: `MultiSourceOrchestrator` manages data fetching from multiple sources.
- **Matching**: `EntityMatcherService` matches parks/attractions using fuzzy string matching + geolocation (75-80% threshold).
- **Conflict Resolution**: `ConflictResolverService` merges data (Wiki = rich metadata, Queue-Times = lands).
- **Explicit IDs**: `Park` entity stores `wikiEntityId` and `queueTimesEntityId` for O(1) lookups.

### 4. Queue Management & Scheduling ✅

**Three-Layer Architecture**:

1. **Seeder** (`db-seed.service.ts`) - Runs ONCE on empty DB:
   - Parks metadata (Multi-Source discovery)
   - Children metadata (attractions + shows + restaurants)
   - Weather (initial sync)
   - Holidays

2. **Bootstrap** (`queue-bootstrap.service.ts`) - Runs on EVERY API restart:
   - **Only triggers**: Wait-times update (for fresh data on startup)
   - **Does NOT trigger**: Weather, metadata (handled by seeder + cron)

3. **Scheduler** (`queue-scheduler.service.ts`) - Cron jobs:
   - `wait-times`: Every 5 minutes
   - `weather`: Every 12 hours (0:00, 12:00)
   - `park-metadata`: Daily at 3am
   - `children-metadata`: Daily at 4am
   - `weather-historical`: Daily at 5am
   - `holidays`: Monthly on 1st at 2am
   - `ml-training`: Daily at 6am
   - `prediction-accuracy`: Every hour

**Job Names** (consistent across seeder/scheduler/processors):
- Weather: `fetch-weather` ✅
- Holidays: `fetch-holidays` ✅
- Parks: `sync-all-parks` ✅
- Children: `fetch-all-children` ✅
- Wait Times: `fetch-wait-times` ✅

### 5. API Structure - Integrated Endpoints ✅

**Core Endpoints** (Integrated Data):
```
GET  /v1/parks                              # List all parks
GET  /v1/parks/{slug}                       # Park + weather + schedule + attractions + shows + restaurants
GET  /v1/parks/{slug}/wait-times            # Current wait times for all attractions in park
GET  /v1/parks/{slug}/schedule              # 30-day schedule
GET  /v1/parks/{continent}/{country}/{city} # Geographic filtering
GET  /v1/parks/{slug}/attractions/{slug}    # Specific attraction in park context

GET  /v1/attractions                        # List all attractions
GET  /v1/attractions/{slug}                 # Attraction + queue + forecasts + predictions
GET  /v1/attractions/{slug}/wait-times      # Historical wait times
GET  /v1/attractions/{slug}/forecasts       # Future wait time predictions

GET  /v1/shows                              # List all shows
GET  /v1/shows/{slug}                       # Show + live status + showtimes
GET  /v1/shows/{slug}/showtimes             # Upcoming showtimes

GET  /v1/restaurants                        # List all restaurants
GET  /v1/restaurants/{slug}                 # Restaurant + live status + availability
GET  /v1/restaurants/{slug}/availability    # Current dining availability

GET  /v1/destinations                       # List all destinations
GET  /v1/destinations/{slug}                # Destination + child parks

GET  /v1/ml/dashboard                       # ML System Dashboard
GET  /v1/ml/models/active                   # Active model metadata
GET  /v1/ml/accuracy/system                 # System-wide accuracy stats

GET  /v1/search?q=space                     # Full-text search
GET  /v1/holidays?country=US                # Holiday data
GET  /health                                # System health status
```

### 6. External APIs

**ThemeParks.wiki**: https://api.themeparks.wiki/docs/v1/
- Entity Types: DESTINATION, PARK, ATTRACTION, SHOW, RESTAURANT
- Queue Types: STANDBY, SINGLE_RIDER, RETURN_TIME, PAID_RETURN_TIME, BOARDING_GROUP, PAID_STANDBY
- Status: OPERATING, DOWN, CLOSED, REFURBISHMENT
- Rate Limit: 60 req/min (reactive backoff: 1s → 2s → 4s → 8s → 16s, max 5 retries)

**Queue-Times.com**:
- Entity Types: PARK, LAND, RIDE
- Unique Data: LANDS (Themed Areas)

**Open-Meteo**: Free weather API (16-day forecast + historical)
**Google Geocoding**: Reverse `lat/lng → continent/country/city` (99/105 parks geocoded)
**Nager.Date**: Holiday data (100+ countries, 2-year rolling window)

### 7. Slug Strategy
- Auto-generate via `@BeforeInsert()` hook (transliteration + slugify)
- **NEVER use API slugs** (lack hyphens, bad for SEO)
- Conflict resolution: append `-2`, `-3`, etc.

---

## Coding Standards

**Language**: All code, comments, docs, commits in **English** (mandatory)

**Strong Typing**:
- **NEVER use `any` type** - use specific types, interfaces, or `unknown` (then narrow with type guards)
- Enable `strict: true` in tsconfig.json

**Naming**:
- Classes/Interfaces: `PascalCase`
- Functions/variables: `camelCase`
- Constants: `UPPER_SNAKE_CASE`
- Files: `kebab-case.ts`
- Booleans: `isOpen`, `hasWaitTime`
- **Objects/Fields**: Use descriptive, semantic names (e.g., `crowdLevel` instead of `rating`, `currentWaitTime` instead of `current`). Avoid generic names.

**Documentation**:
- **Swagger**: ALL new endpoints must have `@ApiTags`, `@ApiOperation`, and `@ApiResponse` decorators.
- **Return Codes**: Explicitly document success (200) and error (404, 400) responses.


---

## Commands

```bash
# Development
npm install
npm run dev                    # Start with hot-reload
docker-compose up -d           # Start DB + Redis + Bull Board

# Database
npm run db:reset               # Wipe & reseed (dev only)

# Jobs
npm run job:ml-train           # Trigger ML training manually

# Testing
npm test                       # Unit tests
npm run test:watch             # Watch mode unit tests
npm run test:cov               # Application coverage
npm run test:e2e               # End-to-end tests

# Production
npm run build
```

---

## Implementation Status

### ✅ Phases 1-6.6: Complete
- **Foundation**: NestJS + Docker + TypeORM + Bull Queue
- **Data Collection**: ThemeParks.wiki, Open-Meteo, Geocoding, Holidays
- **Multi-Source Integration**: 
    - **Orchestrator**: Unified interface for ThemeParks.wiki & Queue-Times.com
    - **Seeder**: Automatic matching & deduplication
    - **Live Data**: Hybrid fetching strategy
- **Entities**: 138 parks (98% geocoded), 6k+ entities
- **TimescaleDB**: 5 hypertables with 30-day compression (hourly resolution)
- **Analytics**: Occupancy (95th percentile), statistics, crowd levels
- **ML Predictions**: CatBoost 1.2.8 (MAE 10.98 min), daily retraining
- **Search**: Full-text search with fuzzy matching
- **Live Data**: All entity types (attractions/shows/restaurants) with delta strategies
- **API Optimization**: 98% reduction (10 req/min vs 803 req/min before)

### ✅ Recent Optimizations (Dec 2024)

**Multi-Source ID Architecture**:
- **Problem**: Inefficient DB lookups and rate-limiting issues when syncing multiple sources.
- **Solution**: Added explicit `wikiEntityId` and `queueTimesEntityId` columns to `Park` entity.
- **Benefit**: Zero-lookup synchronization, eliminates 404s for single-source parks.

**Queue Skipping Bug Fix**:
- **Problem**: Attractions in open parks were skipped when API returned no live data
- **Solution**: Fallback logic marks all entities as OPERATING when park is open but API fails
- **Files**: `wait-times.processor.ts`, `parks.service.ts` (closing time boundary fix)

**Logging & ML Optimizations**:
- **Delta Statistics**: Logs now show "Updated" (actually saved) vs "Processed" counts
- **ML Resampling**: Fixed feature engineering (`features.py`) to handle delta-compressed data gaps (5-min ffill)
- **ML Timezone Awareness**: Model now trains on **Park-Local Time** (converted from UTC) for accurate daily patterns
- **Manual Trigger**: Added `npm run job:ml-train` to manually trigger ML training

---

## Key Entities

**Core**: `Destination`, `Park`, `Attraction`, `Show`, `Restaurant`
**Data**: `QueueData`, `ForecastData`, `ShowLiveData`, `RestaurantLiveData`, `ScheduleEntry`, `WeatherData`, `Holiday`
**ML**: `WaitTimePrediction`, `MLModel`, `ParkOccupancy`, `PredictionAccuracy`

---

## Key Decisions

**Architecture**:
- Strong typing: NO `any` - use interfaces or `unknown`
- Slugs: Always generate our own (never use API slugs)
- TypeORM: `synchronize: true` until production
- **Explicit Source IDs**: Store source IDs directly on Park entity to prevent expensive lookups.

**Queue Management**:
- **Seeder**: Initial setup only (parks, children, weather, holidays)
- **Bootstrap**: Minimal (wait-times only for fresh data on startup)
- **Scheduler**: Regular updates (cron jobs)

**Data Collection**:
- Delta storage: waitTime Δ > 5 min or status changes
- Entity-type routing: Single processor for all types
- Park-level API: ONE call per park (not per attraction)
- Closed parks: Mark as CLOSED (NO API call)
- Open parks without API data: Mark as OPERATING (fallback logic)

**Timezone Handling** (CRITICAL):
- **ALWAYS use park timezone** for status calculations
- Database stores UTC timestamps (openingTime/closingTime)
- Use `date-fns-tz.toZonedTime()` to convert current time to park timezone
- **NEVER compare server local time** with UTC schedule times directly
- Park status: `nowInParkTz >= openingTime && nowInParkTz < closingTime`
- Queue data freshness: Only show data when park is OPERATING
- ML predictions: Already timezone-aware (daily patterns based on park local time)

**Time-Series**:
- TimescaleDB: Composite PKs, compression preserves hourly data
- Hourly resolution: CRITICAL for ML/occupancy (never aggregate away)

---

## 🎯 Current Status

**Operational**:
- Data collection: All streams working (wait times every 5 min, weather every 12h)
- Live data: All entity types with delta strategies
- ML: CatBoost v1.1.0 (MAE 10.98 min), daily retraining
- Analytics: Occupancy, statistics integrated in park endpoints
- Search: Full-text with fuzzy matching
- API optimization: 10 req/min vs 803 req/min before

**Bull Board**: http://localhost:3001

---

## 🚀 Next Priority Tasks

### ⭐ IMPLEMENT NEXT: Real-Time Prediction Updates

**Plan Location**: `/Users/patrick/.gemini/antigravity/brain/4c07901a-b450-46d6-9b4f-2beb13e0146f/real-time-updates-plan.md`

**Goal**: Automatically regenerate predictions when actual wait times deviate significantly from forecasts.

**Triggers**:
- Absolute deviation > 10 minutes
- Percentage deviation > 20%
- Rate limited: Max 1 update per attraction per 15 min

**Implementation**:
- New Service: `PredictionDeviationService` - Detect deviations
- New Processor: `PredictionUpdateProcessor` - Handle regeneration
- Modify: `wait-times.processor.ts` - Trigger checks on new data

**Debug Points** (Record These Deviations):
```typescript
// In PredictionDeviationService
logger.debug({
  event: 'deviation_detected',
  attractionId,
  predicted: predictedWaitTime,
  actual: actualWaitTime,
  absoluteDeviation,
  percentageDeviation,
  timestamp: new Date().toISOString(),
  threshold: { absolute: 10, percentage: 20 }
});

// In PredictionUpdateProcessor  
logger.log({
  event: 'prediction_regenerated',
  attractionId,
  parkId,
  oldPredictions: previousPredictions.length,
  newPredictions: updatedPredictions.length,
  duration: regenerationTimeMs,
  timestamp: new Date().toISOString()
});

// Rate limiting
logger.warn({
  event: 'update_rate_limited',
  attractionId,
  lastUpdate: lastUpdateTimestamp,
  cooldownRemaining: remainingSeconds,
  timestamp: new Date().toISOString()
});
```

**Estimated Effort**: 6-8 hours
**Impact**: 30-40% fresher predictions during volatility

---

**Update this file whenever**: Data models change, queue configs tuned, architectural decisions made, or new learnings discovered.
