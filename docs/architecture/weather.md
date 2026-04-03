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
  — two jobs: "weather-full-cron" (12h) and "weather-current-cron" (1h)
  — filters parks where latitude IS NOT NULL AND longitude IS NOT NULL

  Hourly (currentOnly=true):
    — forecast_days=1, fetches current{} variables from Open-Meteo
    — saves today's record + live fields (temperatureCurrent, apparentTemperature, humidity, isDay)
    — 300ms delay between parks (~45s for 150 parks)

  Every 12h (currentOnly=false):
    — forecast_days=16, saves today + 15 forecast days
    — 1000ms delay between parks

        ↓
weather_data table  (composite PK: parkId + date)
  — dataType: "current" (today) | "forecast" (future) | "historical" (past)
  — live columns (nullable): temperatureCurrent, apparentTemperature, humidity, isDay
        ↓
WeatherService.getCurrentAndForecast(parkId)
  — returns: { current: today, forecast: next 15 days }
        ↓
ParkIntegrationService.buildIntegratedResponse()
  — dto.weather = { current, now, forecast: next 6 days }  ← API response
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
    "now": {
      "temperature": 9.4,
      "apparentTemperature": 7.1,
      "humidity": 82,
      "weatherCode": 45,
      "weatherDescription": "Fog",
      "isDay": true
    },
    "forecast": [
      { "date": "2026-04-01", "dataType": "forecast", ... },
      { "date": "2026-04-02", ... },
      ...  // up to 6 days
    ]
  }
}
```

`weather.now` is `null` until the first hourly sync has run for a park.
`weather.current` remains the daily summary (max/min temp, precipitation totals).
`weather.now` is the live snapshot updated every hour.

---

## Storage

**Entity**: `src/parks/entities/weather-data.entity.ts`
**Composite PK**: `(parkId, date)`

| Column | Type | Notes |
|--------|------|-------|
| parkId | UUID | FK → parks |
| date | DATE (timestamptz) | Stored as noon UTC (see timezone note) |
| dataType | enum | `historical` \| `current` \| `forecast` |
| temperatureMax / Min | decimal(5,2) | °C — daily high/low |
| precipitationSum | decimal(6,2) | mm |
| rainSum | decimal(6,2) | mm |
| snowfallSum | decimal(6,2) | cm |
| weatherCode | int | WMO code (daily) |
| windSpeedMax | decimal(5,2) | km/h |
| temperatureCurrent | decimal(5,2) | °C — live, updated hourly (nullable) |
| apparentTemperature | decimal(5,2) | °C — feels-like, updated hourly (nullable) |
| humidity | int | % relative humidity, updated hourly (nullable) |
| isDay | boolean | daytime at park location, updated hourly (nullable) |

**Timezone note**: Dates are always interpreted in park local timezone. `saveWeatherData()` converts `"2026-03-31"` → `fromZonedTime("2026-03-31T00:00:00", park.timezone)` before storing.

---

## Sync Jobs (BullMQ)

| Queue | Job ID | Schedule | Action |
|-------|--------|----------|--------|
| `weather` | `weather-current-cron` | Every hour | `forecast_days=1` — saves today + live fields; 300ms delay between parks |
| `weather` | `weather-full-cron` | Every 12h (00:00 + 12:00 UTC) | `forecast_days=16` — saves today + 15 forecast days; 1s delay between parks |
| `weather-historical` | `weather-historical-cron` | Daily 05:00 UTC | Mark past records as `dataType = historical` |

**Rate limiting**: 300ms (hourly/current) or 1s (full) delay between parks.
**Cache invalidation**: `weather:forecast:{parkId}` Redis key is deleted after each park update so the next request sees fresh data.

---

## Why a Park Has Empty Weather

A park shows `weather: { current: null, forecast: [] }` when **it has no coordinates** (`latitude IS NULL OR longitude IS NULL`).

Coordinates come from the ThemeParks.wiki API sync (park-metadata processor, Step 1). For newer parks or parks where the wiki hasn't provided coordinates, the geocoding step (Step 8 of metadata sync, using Google Maps) attempts to fill them in.

**Diagnosis**: Check `parks` table — if `latitude` and `longitude` are NULL, weather sync skips the park.

**Fix**: Trigger a park-metadata sync (`sync-parks` job) to re-attempt geocoding. Once coordinates are populated, the next weather sync (runs every 12h) will populate weather data.

---

## Known Bug Patterns (Fixed)

### DATE + AT TIME ZONE = off-by-one for non-UTC parks

**Bug**: Queries using `DATE(weather.date AT TIME ZONE :tz) BETWEEN ...` on a PostgreSQL `DATE` column shift dates by ±1 day for non-UTC parks.

**Why it happens**: PostgreSQL implicitly casts a `DATE` to `timestamp with time zone` (midnight UTC) before applying `AT TIME ZONE`. For parks west of UTC (e.g. `America/New_York` = UTC-4), midnight UTC becomes 8 PM the previous day in local time → the stored date shifts back by 1 day → today's record is excluded from the query → `current` is always `null`.

For parks east of UTC (e.g. `Europe/Berlin` = UTC+2), the save bug and the query bug happen to cancel out — they appear to work but actually show the wrong day's data (April 1 data shown as "today" on March 31).

**Fix**:
- **Save**: Use noon-UTC instead of `fromZonedTime(midnight, tz)`. Noon UTC is never ambiguous for any timezone: `new Date(\`${day.date}T12:00:00Z\`)` → PostgreSQL DATE always truncates to the correct calendar date.
- **Query**: Use direct date-string comparison (`weather.date >= :start`) instead of `DATE(weather.date AT TIME ZONE :tz)`. PostgreSQL auto-casts `'YYYY-MM-DD'` strings to DATE.

**Affected**: All non-UTC parks. US parks showed `current: null`; European parks showed tomorrow's data as today.

### save via `fromZonedTime(midnight, timezone)` stores wrong date for east-of-UTC parks

**Bug**: `fromZonedTime("2026-03-31T00:00:00", "Europe/Berlin")` = `2026-03-30T22:00:00Z`. PostgreSQL DATE = `2026-03-30` (off by -1 day).

**Fix**: Use `new Date(\`${day.date}T12:00:00Z\`)` — noon UTC is unambiguous for all timezones (±12h = always same calendar day).

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
