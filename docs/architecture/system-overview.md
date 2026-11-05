# System overview

The parts of the park.fan API and how a wait time travels through them. Open this first when you are
new to the repo; each section links the page that owns the detail.

## Components

| Component | What it is | Owner page |
| --- | --- | --- |
| **API** (`src/`) | NestJS: the `/v1/...` REST API, ingestion, analytics, background jobs on Bull (`@nestjs/bull`, `bull` 4) | this page |
| **ml-service** | Python FastAPI + CatBoost: intraday 15-min slots and the daily forecast beyond TFT's horizon; trains daily 06:00 UTC | [CatBoost](../ml/catboost.md); what serves what: [model overview](../ml/model-overview.md) |
| **nf-service** | Python: TFT near-term daily forecast (`tft_forecasts`) and the Chronos-2 shadow | [TFT](../ml/tft.md) |
| **pcn-service** | Python: PCN intraday shadow forecast (`pcn_forecasts`) | [PCN](../ml/pcn.md) |
| **ml-bench** | offline benchmark, not deployed | [ml-bench](../ml/ml-bench/README.md) |
| **PostgreSQL + TimescaleDB** | all data; `queue_data` and the other time series are hypertables | [Schema](../database/schema.md) |
| **Redis** | response and analytics caches, Bull queues (prefix `BULL_PREFIX`, default `parkfan`) | [Caching](caching-strategy.md), [Job queues](job-queues.md) |
| **bull-board** | queue dashboard, its own container (port 3001) | [Job queues](job-queues.md) |

The Compose files (`docker-compose.yml`, `docker-compose.production.yml`) run all of them on the
internal `parkfan-network`; the API reaches the Python services over HTTP (`ML_SERVICE_URL` …).
Deployment: [Coolify](../deployment/coolify.md).

Main API modules: `parks` (park payload, calendar, `/plan/day`, schedules, weather), `attractions`,
`queue-data` (ingestion and live status), `analytics` (P50/P90 baselines, crowd levels, statistics),
`ml` (client for the Python services, prediction storage), `holidays`, `search`, `discovery` /
`location` / `favorites`, `push` / `trips` / `ride-alerts` / `show-follows`, `admin`, `queues`
(processors and the scheduler).

## How a wait time flows

1. **Ingest** — `WaitTimesProcessor` (every 5 min) fetches every source a park has
   ([data ingestion](data-ingestion.md)) and writes changed readings to `queue_data`; parks whose feeds
   stopped are handled by [reverse reconciliation](reverse-reconciliation.md).
2. **Derive** — nightly jobs build baselines, rollups and seasonality
   ([job queues](job-queues.md)); crowd levels follow [one rule](../rules/crowd-levels.md).
3. **Predict** — the API asks the Python services and stores the answers
   ([ML overview](../ml/ml-overview.md)).
4. **Serve** — `ParkIntegrationService` / `AttractionIntegrationService` build the payloads, cached
   in Redis and warmed after every sync; a write is published
   [outside-in](../rules/publish-outside-in.md) to the frontend.

Status rules for what a ride or day reads: [attraction status](attraction-status/README.md),
[calendar rules](calendar-schedule-and-ml-rules.md). Time is always park time
([rule](../rules/park-timezone.md)).
