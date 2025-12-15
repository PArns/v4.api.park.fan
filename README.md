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

## ✨ Features

- **🚀 Real-time Wait Times** — Live queue data for attractions, shows, and restaurants
- **🤖 ML Predictions** — Machine learning forecasts for wait times and crowd levels
- **🌤️ Weather Integration** — Current conditions and 16-day forecasts for all parks
- **📅 Park Schedules** — Opening hours, special events, and operating calendars
- **🌍 Multi-Source Data** — Aggregated from multiple providers for maximum coverage
- **📊 Analytics Ready** — TimescaleDB-powered time-series data for insights
- **⚡ High Performance** — Redis caching and Bull queue processing
- **🎯 RESTful API** — Clean, intuitive endpoints with geographic routing

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

- **API**: http://localhost:3000/v1
- **Bull Board**: http://localhost:3001 (queue monitoring dashboard)
- **API Docs**: http://localhost:3000/api (Swagger)

---

## 📚 API Documentation

### 🎡 Parks

Core endpoints for park information, weather, schedules, and wait times.

```http
GET /v1/parks
GET /v1/parks/:slug
GET /v1/parks/:continent/:country/:city/:parkSlug
GET /v1/parks/:slug/wait-times
GET /v1/parks/:slug/weather
GET /v1/parks/:slug/weather/forecast
GET /v1/parks/:slug/schedule
GET /v1/parks/:slug/schedule/:date
```

**Query Parameters:**
- `continent`, `country`, `city` — Filter by location
- `sort` — Sort order (name, popularity, etc.)

### 🎢 Attractions

Detailed attraction data with ML predictions and historical analytics.

```http
GET /v1/attractions
GET /v1/attractions/:slug
```

**Response includes:**
- Live wait times and status
- 24-hour forecasts
- Daily ML predictions
- Historical statistics
- Percentile data (P50, P75, P90, P95)

### 🌍 Geographic Discovery

Navigate parks by geographic hierarchy.

```http
GET /v1/destinations              # Resorts (e.g., Disney World, Disneyland Paris)
GET /v1/parks/:continent
GET /v1/parks/:continent/:country
GET /v1/parks/:continent/:country/:city
```

### 🎭 Shows & Dining

```http
GET /v1/shows
GET /v1/shows/:slug
GET /v1/restaurants
GET /v1/restaurants/:slug
```

---

## 📁 Project Structure

```
v4.api.park.fan/
├── src/
│   ├── config/                    # App configuration
│   ├── common/                    # Shared utilities, filters, interceptors
│   ├── queues/                    # Bull queue setup & processors
│   ├── destinations/              # Destinations module
│   ├── parks/                     # Parks, weather, schedules
│   ├── attractions/               # Attractions & integration services
│   ├── shows/                     # Shows module
│   ├── restaurants/               # Restaurants module
│   ├── queue-data/                # Wait times & forecasting
│   ├── ml/                        # ML integration
│   └── analytics/                 # Analytics & statistics
├── ml-service/                    # Python ML service
├── docker/                        # Docker configurations
└── scripts/                       # Utility scripts
```

---

## 🚀 Deployment

### Coolify (Docker Compose)

This project is optimized for deployment on [Coolify](https://coolify.io/):

```bash
# All services are defined in docker-compose.production.yml
# with persistent volumes for data retention across deployments
```

**Quick Setup:**
1. Connect GitHub repository to Coolify  
2. Set `docker-compose.production.yml` as compose file
3. Upload `.env` file with your configuration
4. Deploy!

📖 **[Full Deployment Guide →](./DEPLOYMENT.md)**

### Persistent Volumes

Your data is automatically preserved across redeployments:
- `pgdata` → PostgreSQL database
- `redisdata` → Redis cache
- `ml-models` → Trained ML models

---

---

## 🐳 Docker Commands

```bash
# Start all services
npm run docker:up

# Stop all services
npm run docker:down

# View logs
npm run docker:logs

# Reset database (WARNING: deletes all data)
npm run db:reset
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

# Watch mode
npm run test:watch
```

---

## 🔧 Environment Variables

Key configuration options (see `.env.example` for complete list):

```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_USERNAME=parkfan
DB_PASSWORD=parkfan
DB_DATABASE=parkfan
DB_SYNCHRONIZE=true              # ⚠️ Development only!
DB_LOGGING=false

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# Application
NODE_ENV=development
PORT=3000
API_PREFIX=v1

# External APIs
OPEN_WEATHER_API_KEY=your_key_here
```

---

## 🤝 Contributing

This is a private project. For questions or collaboration inquiries, please contact the maintainer.

---

## 📄 License

**UNLICENSED** — Private project by Patrick Arns [arns.dev](https://arns.dev/)

---

## 🙏 Powered By

This project aggregates data from multiple sources:

- **[Queue-Times.com](https://queue-times.com/)** — Real-time wait time data
- **[ThemeParks.wiki](https://themeparks.wiki/)** — Comprehensive park information and live data

Special thanks to these services for making real-time theme park data accessible.

---

<div align="center">

Made with ❤️ for theme park enthusiasts worldwide

</div>
