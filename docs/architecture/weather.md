# Weather Architecture

## Overview

Weather data is fetched from **Open-Meteo** (free, no API key, global coverage including US/worldwide).

**Key fact**: Open-Meteo works for all parks globally — no US-specific API is needed. Empty weather on a park means the park has no `latitude`/`longitude` in the DB.

---

## Data Flow

```
Open-Meteo API (/forecast, /archive)
        ↓
WeatherProcessor (BullMQ queue: "weather")
  — every 12h (00:00 + 12:00 UTC)
  — filters parks where latitude IS NOT NULL AND longitude IS NOT NULL
  — fetches 16 days (today + 15 forecast days)
        ↓
weather_data table  (composite PK: parkId + date)
  — dataType: "current" (today) | "forecast" (future) | "historical" (past)
        ↓
WeatherService.getCurrentAndForecast(parkId)
  — returns: { current: today, forecast: next 15 days }
        ↓
ParkIntegrationService.buildIntegratedResponse()
  — dto.weather = { current, forecast: next 6 days }  ← API response
```

---

## API Response Shape

The integrated park endpoint (`GET /v1/parks/:continent/:country/:city/:slug`) returns:

```json
{
  "weather": {
    "current": {
      "date": "2026-03-31",
      "dataType": "current",
      "temperatureMax": 12.2,
      "temperatureMin": 0.3,
      "precipitationSum": 0.0,
      "rainSum": 0.0,
      "snowfallSum": 0.0,
      "weatherCode": 45,
      "weatherDescription": "Fog",
      "windSpeedMax": 8.3
    },
    "forecast": [
      { "date": "2026-04-01", "dataType": "forecast", ... },
      { "date": "2026-04-02", ... },
      ...  // up to 6 days (today + 6 = 7 total)
    ]
  }
}
```

---

## Storage

**Entity**: `src/parks/entities/weather-data.entity.ts`
**Composite PK**: `(parkId, date)`

| Column | Type | Notes |
|--------|------|-------|
| parkId | UUID | FK → parks |
| date | DATE (timestamptz) | Stored as midnight in park timezone |
| dataType | enum | `historical` \| `current` \| `forecast` |
| temperatureMax / Min | decimal(5,2) | °C |
| precipitationSum | decimal(6,2) | mm |
| rainSum | decimal(6,2) | mm |
| snowfallSum | decimal(6,2) | cm |
| weatherCode | int | WMO code |
| windSpeedMax | decimal(5,2) | km/h |

**Timezone note**: Dates are always interpreted in park local timezone. `saveWeatherData()` converts `"2026-03-31"` → `fromZonedTime("2026-03-31T00:00:00", park.timezone)` before storing.

---

## Sync Jobs (BullMQ)

| Queue | Job name | Schedule | Action |
|-------|----------|----------|--------|
| `weather` | `fetch-weather` | Every 12h (00:00 + 12:00 UTC) | Fetch 16-day forecast for all parks with coordinates |
| `weather-historical` | `mark-historical` | Daily 05:00 UTC | Mark past records as `dataType = historical` |

**Rate limiting**: 1 second delay between parks (Open-Meteo is free, be polite).

---

## Why a Park Has Empty Weather

A park shows `weather: { current: null, forecast: [] }` when **it has no coordinates** (`latitude IS NULL OR longitude IS NULL`).

Coordinates come from the ThemeParks.wiki API sync (park-metadata processor, Step 1). For newer parks or parks where the wiki hasn't provided coordinates, the geocoding step (Step 8 of metadata sync, using Google Maps) attempts to fill them in.

**Diagnosis**: Check `parks` table — if `latitude` and `longitude` are NULL, weather sync skips the park.

**Fix**: Trigger a park-metadata sync (`sync-parks` job) to re-attempt geocoding. Once coordinates are populated, the next weather sync (runs every 12h) will populate weather data.

---

## Key Files

| File | Purpose |
|------|---------|
| `src/external-apis/weather/open-meteo.client.ts` | Open-Meteo API client (daily + hourly forecast, historical) |
| `src/parks/weather.service.ts` | Save/query weather data, `getCurrentAndForecast()`, hourly forecast |
| `src/parks/entities/weather-data.entity.ts` | TypeORM entity |
| `src/parks/dto/weather-item.dto.ts` | Response DTO with `fromEntity()` and WMO description |
| `src/queues/processors/weather.processor.ts` | BullMQ processor for daily sync |
| `src/queues/processors/weather-historical.processor.ts` | Marks past data as historical |
| `src/common/constants/wmo-weather-codes.constant.ts` | WMO code → human-readable description |

---

## Open-Meteo Limits

- Free tier: no API key, no hard rate limit
- Max forecast days: **16** (current implementation uses all 16)
- Historical: 1940–present via `/archive` endpoint
- Redis rate-limit guard: `ratelimit:openmeteo:blocked` key (60s block on 429)
