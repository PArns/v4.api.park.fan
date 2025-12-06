# park.fan API v4

Real-time theme park data aggregation API providing wait times, occupancy insights, and ML-ready analytics for optimizing park visits.

## Tech Stack

- **Framework**: NestJS 10
- **Database**: PostgreSQL 16 + TimescaleDB
- **Cache/Queue**: Redis 7 + Bull Queue
- **Language**: TypeScript (strict mode)
- **Container**: Docker Compose

## Quick Start

### Prerequisites

- Node.js 20+
- Docker & Docker Compose
- Git

### Installation

```bash
# 1. Install dependencies
npm install

# 2. Copy environment file
cp .env.example .env

# 3. Start infrastructure (PostgreSQL + Redis)
npm run docker:up

# 4. Start development server
npm run dev
```

The API will be available at:
- **API**: http://localhost:3000/v1
- **Bull Board**: http://localhost:3001 (queue monitoring)

## API Documentation

### Parks (`/v1/parks`)

Core endpoints for park data, weather, schedules, and live wait times.

- **List All Parks**: `GET /v1/parks`
  - Query params: `continent`, `country`, `city`, `sort`
- **Park Details**: `GET /v1/parks/:slugOrContinent` (e.g., `europa-park`)
- **Park by Location**: `GET /v1/parks/:continent/:country/:city/:parkSlug`
- **Wait Times**: `GET /v1/parks/:slug/wait-times`
  - Returns current wait times for all attractions.
- **Weather**:
  - Current & Range: `GET /v1/parks/:slug/weather`
  - 16-Day Forecast: `GET /v1/parks/:slug/weather/forecast`
- **Schedule**:
  - Range: `GET /v1/parks/:slug/schedule`
  - Specific Date: `GET /v1/parks/:slug/schedule/:date`

### Attractions (`/v1/attractions`)

Detailed attraction data including ML predictions and forecasts.

- **List All**: `GET /v1/attractions`
- **Details**: `GET /v1/attractions/:slug`
  - Includes: Live Queue Data, Forecasts (24h), ML Predictions (Daily), Statistics.

### Geographic Discovery

- **Destinations**: `GET /v1/destinations` (Resorts like Disney World)
- **Parks by City**: `GET /v1/parks/:continent/:country/:city`
- **Parks by Country**: `GET /v1/parks/:continent/:country`

### Shows & Restaurants

- **Shows**:
  - List: `GET /v1/shows`
  - Details: `GET /v1/shows/:slug`
- **Restaurants**:
  - List: `GET /v1/restaurants`
  - Details: `GET /v1/restaurants/:slug`

## Project Structure

```
src/
├── config/                 # Configuration
├── common/                 # Shared utilities (filters, interceptors)
├── queues/                 # Bull queue setup & processors
├── destinations/           # Destinations module
├── parks/                  # Parks module (weather, schedules)
├── attractions/            # Attractions module
├── shows/                  # Shows module
├── restaurants/            # Restaurants module
├── queue-data/             # Wait times & forecasting
└── ml/                     # Machine Learning integration
```

## Docker Commands

```bash
# Start all services
npm run docker:up

# Stop all services
npm run docker:down

# View logs
npm run docker:logs
```

## Environment Variables

See `.env.example` for all available configuration options.

Key variables:
- `DB_SYNCHRONIZE=true`: Auto-sync schema (dev only!)
- `DB_LOGGING=true`: Show SQL queries
- `NODE_ENV=development`: Environment mode

## Documentation References

- [CLAUDE.md](./CLAUDE.md): Development guidelines & Implementation Status
- [ML_TRAINING.md](./ML_TRAINING.md): ML Training guide & commands
- [ThemeParks.wiki API Docs](https://api.themeparks.wiki/docs/v1/)

## License

UNLICENSED - Private project
