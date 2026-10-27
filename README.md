<div align="center">

# 🎢 park.fan API

**Live wait times, crowd levels, opening hours and forecasts for theme parks worldwide.**
The data service behind [park.fan](https://park.fan).

[![NestJS 12](https://img.shields.io/badge/NestJS-12-E0234E?logo=nestjs&logoColor=white)](https://nestjs.com/)
[![TypeScript 6](https://img.shields.io/badge/TypeScript-6-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![PostgreSQL 18 + TimescaleDB](https://img.shields.io/badge/PostgreSQL_18-TimescaleDB-4169E1?logo=postgresql&logoColor=white)](https://www.timescale.com/)
[![Redis 8](https://img.shields.io/badge/Redis-8-DC382D?logo=redis&logoColor=white)](https://redis.io/)
[![Python ML](https://img.shields.io/badge/ML-CatBoost_·_TFT_·_PCN-3776AB?logo=python&logoColor=white)](https://github.com/PArns/v4.api.park.fan/blob/main/docs/ml/model-overview.md)

**[OpenAPI / Swagger](https://api.park.fan/api)** · **[park.fan](https://park.fan)** · **[Developer docs](https://github.com/PArns/v4.api.park.fan/blob/main/docs/README.md)**

</div>

---

## What it serves

All routes live under **`/v1`**. Parks are addressed by their geographic path,
`/v1/parks/{continent}/{country}/{city}/{park}`, e.g.
`/v1/parks/europe/germany/rust/europa-park`. Every parameter and response shape is in the
[Swagger UI](https://api.park.fan/api).

| Area | Routes | What you get |
| --- | --- | --- |
| 🎡 **Parks** | `GET /parks` · `/parks/{continent}[/{country}[/{city}]]` · `/parks/popular` | Park lists by region, most-requested parks |
| 🏰 **One park** | `GET /parks/…/{park}` | Status, opening hours, live crowd level, weather, every attraction, show and restaurant |
| ⏱️ **Live** | `…/{park}/wait-times` | Current queues per attraction |
| 📅 **Calendar** | `…/{park}/calendar` · `/best-days` · `/schedule` · `/seasons` · `/predictions/yearly` | Opening hours, day status, crowd forecast per day, holidays, named seasons |
| 🗓️ **Trip planning** | `…/{park}/plan/day` | One day, ride by ride: hourly waits with source and expected error |
| 📊 **Statistics** | `…/{park}/stats` · `/stats/hourly` · `/stats/day` | Historical park stats, the park's hourly profile, one ride's day curve |
| 🎢 **Attractions** | `…/{park}/attractions[/{attraction}]` | Catalog list; detail with live state, forecast, typical/busy waits, ride profile |
| 🌤️ **Weather** | `…/{park}/weather` · `/weather/forecast` · `/weather/nowcast` | Observed weather, 16-day forecast, short-term nowcast, severe-weather warnings |
| 🌍 **Discovery** | `GET /discovery/geo` · `/discovery/continents[…]` · `/discovery/nearby` | Geo hierarchy for navigation, parks or rides near a position |
| 🔍 **Search** | `GET /search?q=…` | Parks and attractions by name or place |
| 📈 **Analytics** | `GET /analytics/realtime` · `/geo-live` · `/ticker` · `/best-times` | Platform-wide live figures and best times to visit |
| 📖 **Glossary** | `GET /glossary/terms/counts` · `/glossary/terms/{termId}/attractions` | Rides that feature a glossary term |
| ⭐ **Personal** | `/favorites` · `/trips` · `/push` · `/push/ride-alerts` · `/push/show-follows` · `/page-views` | Favorites, stored day plans, web push, page-view counts |
| 🩺 **Health** | `GET /health` · `/health/ready` · `/health/ping` · `/ml/health` | Liveness, readiness, ML service state |

### Conventions worth knowing

- **Park-local time.** Dates and "today" are in the park's own timezone (`timezone` on every park), never the caller's.
- **Unknown is not closed.** A ride no source reports reads `UNKNOWN`; a crowd level without a baseline reads `unknown`. Neither means "closed" or "quiet".
- **Nulls are omitted.** Fields without a value are left out of the response; add `?debug=true` to see them.
- **Cached at the edge.** Responses carry `Cache-Control` headers for Cloudflare; live endpoints refresh within minutes.

---

## 🚀 Quick start

Requires **Node.js 26+**, **pnpm 11** (`corepack enable`) and **Docker**.

```bash
git clone https://github.com/PArns/v4.api.park.fan.git
cd v4.api.park.fan
pnpm install
cp .env.example .env

docker compose up -d postgres redis bull-board   # add ml-service nf-service pcn-service for forecasts
pnpm dev                              # http://localhost:3000
```

| URL | |
| --- | --- |
| http://localhost:3000/ | This page |
| http://localhost:3000/v1 | The API |
| http://localhost:3000/api | Swagger UI |
| http://localhost:3001/ | Bull Board (job queues) |

`pnpm docker:up` starts the **whole** stack including the API container on port 3000 — use it
instead of `pnpm dev`, not alongside it.

---

## 🧱 Architecture

```
            ┌──────────────── NestJS API (src/) ────────────────┐
 park.fan ─▶│ REST /v1 · Redis cache · Bull queues · TypeORM    │
            └───┬───────────────┬──────────────────┬────────────┘
                │               │                  │
        PostgreSQL 18     Redis 8          Python services
        + TimescaleDB     cache + queues   ml-service  (CatBoost)
                                           nf-service  (TFT, Chronos)
                                           pcn-service (intraday PCN)
```

| Path | Contents |
| --- | --- |
| `src/` | NestJS modules: `parks`, `attractions`, `queue-data`, `analytics`, `ml`, `holidays`, `discovery`, `search`, `trips`, `push`, `admin`, … |
| `ml-service/` | CatBoost training and serving (FastAPI) |
| `nf-service/` | NeuralForecast TFT and Chronos daily forecasts |
| `pcn-service/` | Intraday forecast model |
| `ml-bench/` | Offline forecasting benchmark (not deployed) |
| `scripts/` | Operational and one-off scripts |
| `test/` | E2E suite (Testcontainers) and fixtures |
| `docs/` | Developer documentation — start at [`docs/README.md`](https://github.com/PArns/v4.api.park.fan/blob/main/docs/README.md) |

---

## 🧪 Development

```bash
pnpm test           # unit tests
pnpm test:e2e       # end-to-end tests against a TimescaleDB container
pnpm lint           # ESLint (fixes in place)
pnpm format         # Prettier
pnpm build          # type check + build
```

Configuration lives in [`.env.example`](https://github.com/PArns/v4.api.park.fan/blob/main/.env.example)
(development) and `.env.production.example`. Deployment runs on
Coolify with `docker-compose.production.yml` — see
[docs/deployment](https://github.com/PArns/v4.api.park.fan/tree/main/docs/deployment).

---

## 🙏 Data sources

park.fan combines these sources. Thank you for making theme park data accessible.

| Source | Used for |
| --- | --- |
| **[Queue-Times.com](https://queue-times.com/)** | Live wait times — *Powered by Queue-Times.com* |
| **[ThemeParks.wiki](https://themeparks.wiki/)** | Parks, attractions, live data, opening hours |
| **[Wartezeiten.app](https://www.wartezeiten.app/)** | Wait times, crowd levels and opening hours |
| **[Open-Meteo.com](https://open-meteo.com/)** | Weather forecasts, current conditions and history ([CC BY 4.0](https://creativecommons.org/licenses/by/4.0/); built on data from DWD, ECMWF, NOAA, Météo-France and other national weather services) |
| **[Bright Sky](https://brightsky.dev/)** | Severe-weather warnings for Germany from the Deutscher Wetterdienst (DWD) |
| **[MeteoGate](https://meteogate.eu/)** | Severe-weather warnings for Europe (EUMETNET / MeteoAlarm) |
| **[Nager.Date](https://date.nager.at/)** · **[OpenHolidays API](https://www.openholidaysapi.org/)** | Public and school holidays |
| **[NEIS](https://open.neis.go.kr/)** · **[KASI](https://www.data.go.kr/)** | School and public holidays in South Korea |
| **[Wikidata](https://www.wikidata.org/)** | Park and ride facts |
| **[Google Geocoding](https://developers.google.com/maps/documentation/geocoding)** | Park locations |
| **[MaxMind GeoLite2](https://www.maxmind.com/)** | Approximate visitor location for "nearby" — includes GeoLite2 data created by MaxMind |

---

## 📄 License

**UNLICENSED** — private project by [Patrick Arns](https://arns.dev/). For questions or
collaboration, contact the maintainer.

<div align="center">

Made with ❤️ for theme park fans · **[api.park.fan/api](https://api.park.fan/api)** · **[park.fan](https://park.fan)**

</div>
