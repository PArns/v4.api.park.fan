# System Architecture

## Overview

The Park Fan API v4 is a microservices-based architecture designed for theme park wait time tracking, predictions, and analytics. It consists of a primary NestJS backend and a specialized Python machine learning service.

![Architecture Diagram](https://mermaid.ink/img/pako:eNptkU9rwjAQxb9KyGkL9aDQU8FDbz30VmSPSQxjN5GkSyrWgfjdTezP4sE3LMz7zZt5M0KnNIKQ_D0pWqGtyY5K6z8lF5y_FpyjE4K_w5gLyeE1Xy6W8_liOcdLzM_Y4g0Wj9jiHh6u4R2Wd1jgEZb3eHyAF7g_wRusHuD1E7zD27eE0bZBa8oOjeaM0XpDObdG7dEazg2lDdpQzqyppNG7RiuD3pP2Bq0ZoxWlFdpQ9midQW9J24M2S_aonUHvSHtDqRmlDdoQdqjd_x1oDeuU0TqD3rP2Bm04e7TOoHekvaHUgtIGbYh7tM6gd6S9odSM0oboR-sMekfa27-xL62VHCW7J8flsN_vD4fTYTfoD_rddjMaRj38M4y67VbUa_fbTfxz1OniH91OqxkN43b02_8B9oyw7w)

## Components

### 1. API Service (NestJS)
The core backend service responsible for:
- REST API endpoints (`/v1/...`)
- Business logic and orchestration
- Data ingestion and normalization
- Background job processing (BullMQ)
- Serving static data (parks, attractions)

**Key Modules:**
- `Analytics`: P50 baseline calculations, crowd levels
- `Queue Data`: Raw wait time ingestion
- `ML`: Client for Python ML service
- `Enrichment`: Aggregating data for responses

### 2. ML Service (Python)
A lightweight FastAPI microservice dedicated to machine learning tasks.
- **Framework**: FastAPI, CatBoost
- **Responsibility**: Predict wait times based on historical data, weather, and schedules.
- **Communication**: Internal HTTP calls from NestJS service.

### 3. Database (PostgreSQL + TimescaleDB)
Primary data store.
- **Extensions**: TimescaleDB (implied for time-series optimization).
- **Entities**: Parks, Attractions, QueueData, Weather, Schedules.
- **ORM**: TypeORM (with `synchronize: true` in dev).

### 4. Redis
High-performance data structure store used for:
- **Caching**: API responses, calculated stats.
- **Queues**: BullMQ backing for background jobs.
- **Pub/Sub**: Real-time event propagation.

## Data Flow

1. **Ingestion**: Scrapers/Clients send raw wait times to `QueueDataService`.
2. **Processing**: 
   - Data is stored in Postgres.
   - `AnalyticsService` calculates current stats.
   - `MLService` is queried for future predictions.
3. **Serving**: 
   - API builds rich responses using `ParkIntegrationService`.
   - Responses are cached in Redis.

## Infrastructure

- **Docker Compose**: Orchestrates all services for dev and prod.
- **Networks**: Internal `parkfan-network` isolates database and ML service.
- **Volumes**: Persistent storage for DB, Redis, and ML models.
