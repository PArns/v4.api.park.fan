<div align="center">

# 🎢 park.fan API v4

**Real-time theme park intelligence powered by machine learning**

[![NestJS](https://img.shields.io/badge/NestJS-11.0-E0234E?logo=nestjs&logoColor=white)](https://nestjs.com/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.7-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

*Aggregating wait times, weather forecasts, park schedules, and ML-powered predictions for optimal theme park experiences worldwide.*

</div>

---

## 📖 Quick Navigation

- [✨ Features](#-features)
- [🚀 Quick Start](#-quick-start)
- [📚 API Documentation](#-api-documentation)
- [🛠️ Tech Stack](#️-tech-stack)
- [📁 Project Structure](#-project-structure)
- [🐳 Docker Commands](#-docker-commands)
- [🧪 Testing](#-testing)
- [🔧 Environment Variables](#-environment-variables)

---

## ✨ Features

- **🚀 Real-time Wait Times** — Live queue data for attractions, shows, and restaurants
- **🤖 ML Predictions** — Machine learning forecasts for wait times and crowd levels
- **🌤️ Weather Integration** — Current conditions and 16-day forecasts for all parks
- **📅 Park Schedules** — Opening hours, special events, and operating calendars
- **🌍 Multi-Source Data** — Aggregated from multiple providers for maximum coverage
- **📊 Analytics Ready** — TimescaleDB-powered time-series data for insights
- **⚡ High Performance** — Redis caching and Bull queue processing
- **🎯 RESTful API** — Clean endpoints with full Swagger/OpenAPI documentation

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|-----------|
| **Backend** | [NestJS 11](https://nestjs.com/) · TypeScript (strict mode) |
| **Database** | [PostgreSQL 16](https://www.postgresql.org/) · [TimescaleDB](https://www.timescale.com/) |
| **Cache & Queue** | [Redis 7](https://redis.io/) · [Bull Queue](https://github.com/OptimalBits/bull) |
| **ML Service** | Python 3.11 · CatBoost · FastAPI |
| **DevOps** | Docker Compose · GitHub Actions |
| **Testing** | Jest · Supertest · Testcontainers |

---

## 🚀 Quick Start

### Prerequisites

- Node.js 20+
- Docker & Docker Compose
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/PArns/v4.api.park.fan.git
cd v4.api.park.fan

# Install dependencies
npm install

# Copy environment configuration
cp .env.example .env

# Start infrastructure (PostgreSQL + Redis)
npm run docker:up

# Start development server
npm run dev
```

### Access Points

Once running, you can access:

- **API Root**: http://localhost:3000/ (this README)
- **API Base**: http://localhost:3000/v1
- **API Docs**: http://localhost:3000/api (Swagger)
- **Bull Board**: http://localhost:3001 (queue monitoring)

---

## 📚 API Documentation

### 🏥 Health & Monitoring

System health checks and monitoring endpoints.

```http
GET /v1/health              # System health status
GET /v1/health/db           # Database connectivity
```

**Response includes:**
- System uptime
- Database connection status
- Last sync timestamps (wait times, park metadata)
- Active jobs and queue status

---

### 🎡 Parks

Core endpoints for park information, weather, schedules, and wait times.

```http
GET /v1/parks                                      # List all parks
GET /v1/parks/:slug                                # Get park by slug
GET /v1/parks/:continent/:country/:city/:parkSlug  # Get park by location
GET /v1/parks/:slug/wait-times                     # Live wait times
GET /v1/parks/:slug/weather                        # Current weather
GET /v1/parks/:slug/weather/forecast               # 16-day forecast
GET /v1/parks/:slug/schedule                       # Operating hours
GET /v1/parks/:slug/schedule/:date                 # Schedule for specific date
```

**Query Parameters:**
- `continent`, `country`, `city` — Filter by location
- `sort` — Sort order (name, popularity, etc.)
- `include` — Include related data (attractions, schedule, weather)

**Example Response:**
```json
{
  "id": 1,
  "name": "Magic Kingdom",
  "slug": "magic-kingdom",
  "continent": "North America",
  "country": "United States",
  "city": "Orlando",
  "timezone": "America/New_York",
  "currentStatus": "OPERATING",
  "currentCrowdLevel": "MODERATE",
  "coordinates": { "lat": 28.3772, "lng": -81.5707 }
}
```

---

### 🎢 Attractions

Detailed attraction data with ML predictions and historical analytics.

```http
GET /v1/attractions         # List all attractions
GET /v1/attractions/:slug   # Get attraction details
```

**Response includes:**
- Live wait times and status (OPERATING, CLOSED, DOWN, REFURBISHMENT)
- 24-hour ML-powered wait time forecasts
- Daily predictions with confidence scores
- Historical statistics (average, percentiles)
- Downtime tracking and reliability metrics

**Example Response:**
```json
{
  "id": 123,
  "name": "Space Mountain",
  "slug": "space-mountain",
  "parkSlug": "magic-kingdom",
  "category": "RIDE",
  "currentWaitTime": 45,
  "status": "OPERATING",
  "lastUpdate": "2025-12-23T19:30:00Z",
  "forecast": [
    { "hour": "20:00", "predictedWaitTime": 50, "confidence": 0.87 },
    { "hour": "21:00", "predictedWaitTime": 35, "confidence": 0.82 }
  ],
  "stats": {
    "averageWaitTime": 42,
    "p50": 40,
    "p75": 55,
    "p90": 70
  }
}
```

---

### 🌍 Geographic Discovery

Navigate parks by geographic hierarchy for route generation and exploration.

```http
GET /v1/discovery/geo                        # Full geo hierarchy
GET /v1/discovery/continents                 # List continents
GET /v1/discovery/continents/:continent      # Countries in continent
```

**Response Structure:**
```json
{
  "continents": [
    {
      "continent": "North America",
      "countries": [
        {
          "country": "United States",
          "cities": [
            {
              "city": "Orlando",
              "parks": [
                {
                  "name": "Magic Kingdom",
                  "url": "/north-america/united-states/orlando/magic-kingdom",
                  "attractions": [...]
                }
              ]
            }
          ]
        }
      ]
    }
  ]
}
```

---

### 🏰 Destinations

Resort-level aggregation grouping multiple parks.

```http
GET /v1/destinations        # List all destinations
GET /v1/destinations/:slug  # Get destination details
```

**Examples:**
- Walt Disney World (Magic Kingdom, EPCOT, Hollywood Studios, Animal Kingdom)
- Disneyland Paris (Disneyland Park, Walt Disney Studios Park)

---

### 🎭 Shows & Dining

Entertainment and dining options across all parks.

```http
GET /v1/shows               # List all shows
GET /v1/shows/:slug         # Get show details
GET /v1/restaurants         # List all restaurants
GET /v1/restaurants/:slug   # Get restaurant details
```

**Response includes:**
- Showtimes and schedules
- Operating hours
- Wait times (for restaurants)
- Location and park information

---

### 🔍 Search

Global search across parks, attractions, shows, and destinations.

```http
GET /v1/search?q=space+mountain
```

**Search supports:**
- Parks by name or location
- Attractions by name
- Shows and restaurants
- Fuzzy matching for typos

---

### 🎉 Holidays

Public holiday data affecting park crowds and operating hours.

```http
GET /v1/holidays/:isoCountryCode              # Holidays by country
GET /v1/holidays/:isoCountryCode/:year        # Holidays for specific year
```

---

## 📁 Project Structure

```
v4.api.park.fan/
├── src/
│   ├── config/                    # App configuration & environment
│   ├── common/                    # Shared utilities, filters, interceptors
│   ├── database/                  # Database utilities & migrations
│   ├── queues/                    # Bull queue setup & processors
│   ├── health/                    # Health check endpoints
│   ├── destinations/              # Resort/destination grouping
│   ├── parks/                     # Parks, weather, schedules
│   ├── attractions/               # Attractions & data sources
│   ├── shows/                     # Entertainment shows
│   ├── restaurants/               # Dining locations
│   ├── queue-data/                # Wait time data & history
│   ├── ml/                        # ML prediction integration
│   ├── analytics/                 # Statistics & analytics
│   ├── holidays/                  # Public holiday data
│   ├── date-features/             # Date-based features for ML
│   ├── discovery/                 # Geographic discovery endpoints
│   └── search/                    # Global search functionality
├── ml-service/                    # Python ML service (CatBoost)
│   ├── train.py                   # Model training script
│   ├── inference.py               # FastAPI prediction service
│   ├── features.py                # Feature engineering
│   └── db.py                      # Database connection
├── docker/                        # Docker configurations
├── scripts/                       # Utility & debug scripts
├── migrations/                    # Database migrations
└── test/                          # E2E tests
```

---

## 🐳 Docker Commands

```bash
# Start all services (PostgreSQL + Redis)
npm run docker:up

# Stop all services
npm run docker:down

# View logs
npm run docker:logs

# Restart services
npm run docker:restart

# Reset database (WARNING: deletes all data)
npm run db:reset
```

**Production Deployment:**
```bash
docker-compose -f docker-compose.production.yml up -d
```

---

## 🧪 Testing

```bash
# Run unit tests
npm run test

# Run e2e tests
npm run test:e2e

# Run all tests with coverage
npm run test:all:cov

# Watch mode for development
npm run test:watch

# Specific test file
npm run test -- wait-times.processor.spec.ts
```

**Code Quality:**
```bash
# Lint code
npm run lint

# Format code
npm run format

# Type check
npm run build
```

---

## 🔧 Environment Variables

### Application Settings

```env
NODE_ENV=development              # development | production | test
PORT=3000                         # API server port
API_PREFIX=v1                     # API version prefix
```

### Database Configuration

```env
# PostgreSQL with TimescaleDB
DB_HOST=localhost
DB_PORT=5432
DB_USERNAME=parkfan
DB_PASSWORD=your_secure_password
DB_DATABASE=parkfan
DB_SYNCHRONIZE=true               # ⚠️ Set to false in production!
DB_LOGGING=false                  # Enable for debugging
DB_SSL_ENABLED=false              # Enable for production
```

### Caching & Queue

```env
# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=                   # Optional, recommended for production

# Bull Queue
BULL_PREFIX=parkfan
BULL_BOARD_PORT=3001              # Queue monitoring dashboard
```

### External APIs

```env
# Google APIs (Geocoding, Places)
GOOGLE_API_KEY=your_google_api_key

# Weather Data
OPEN_WEATHER_API_KEY=your_openweather_key

# Data Sources (optional, for enhanced coverage)
QUEUE_TIMES_API_KEY=              # Queue-Times.com
THEMEPARKS_API_KEY=               # ThemeParks.wiki
```

### ML Service

```env
# ML Service Configuration
ML_SERVICE_URL=http://localhost:8000        # Development
# ML_SERVICE_URL=http://ml-service:8000     # Production (Docker)
MODEL_DIR=/app/models                       # Model storage directory
MODEL_VERSION=v1.1.0                        # Current model version
```

### Sync & Processing

```env
# Data Sync Intervals (cron expressions)
SYNC_WAIT_TIMES_CRON=*/5 * * * *           # Every 5 minutes
SYNC_PARK_METADATA_CRON=0 6 * * *          # Daily at 6 AM
SYNC_WEATHER_CRON=0 * * * *                # Hourly
```

---

## 🤝 Contributing

This is a private project. For questions or collaboration inquiries, please contact the maintainer.

---

## 📄 License

**UNLICENSED** — Private project by [Patrick Arns](https://arns.dev/)

---

## 🙏 Powered By

This project aggregates data from multiple sources:

- **[Queue-Times.com](https://queue-times.com/)** — Real-time wait time data
- **[ThemeParks.wiki](https://themeparks.wiki/)** — Comprehensive park information and live data
- **[Wartezeiten.app](https://www.wartezeiten.app/)** — Wait times, crowd levels, and opening hours enrichment

Special thanks to these services for making real-time theme park data accessible.

---

<div align="center">

Made with ❤️ for theme park enthusiasts worldwide

**[View API Documentation](http://localhost:3000/api)** · **[Monitor Queues](http://localhost:3001)**

</div>
