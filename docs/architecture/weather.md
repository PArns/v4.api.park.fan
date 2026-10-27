# Weather

How park weather is fetched from Open-Meteo, stored in `weather_data`, served on the park payload, and
extended past the 16-day forecast with climate normals that the wait-time models never see. Open this
when a park's weather is empty or wrong, or before adding a weather call. Severe-weather warnings
come from two other sources ([below](#severe-weather-warnings)).

Open-Meteo is free, keyless and global; no US-specific source is needed. **A park with empty weather
(`current: null, forecast: []`) has no `latitude`/`longitude`.** The park metadata sync
(`sync-all-parks`) geocodes parks without coordinates (Google Maps); the next full weather sync then
fills them in.

## Sync (queue `weather`, `WeatherProcessor`)

| Job | Cron (UTC) | What |
| --- | --- | --- |
| `weather-full-cron` | 00:00, 12:00 | `forecast_days=16`: today plus 15 forecast days, and the forecast issue log |
| `weather-current-cron` | 07:00, 19:00 | `forecast_days=1`: today and the live fields (`temperatureCurrent`, `apparentTemperature`, `humidity`, `isDay`) |
| `weather-historical` / `mark-historical` | 05:00 | Relabels past rows `dataType = 'historical'` |

Parks are deduplicated to unique coordinates; those are fetched in batches of 5 with 1 s (current) or
2 s (full) between batches, then every park is saved from the client's cache in batches of 10. After
each park `weather:forecast:{parkId}` is deleted. A done-marker (5 h current, 11 h full) lets a restart
skip a redundant re-sync.

**Forecast issue log** (PAR-1061): the full sync also appends every daily forecast to
`weather_forecast_issues` (`issued_at` = the response's `fetchedAt`, `lead_days` from the park-local
date), from the same response, no extra call. Write-only ([covariate source](covariate-source.md),
[schema](../database/schema.md)).

## Storage: `weather_data`

Entity `src/parks/entities/weather-data.entity.ts`, primary key `(parkId, date)`. `dataType` is
`current` (today), `forecast` (future) or `historical` (past). Daily columns: `temperatureMax`/`Min`
(°C), `precipitationSum`, `rainSum` (mm), `snowfallSum` (cm), `weatherCode` (WMO), `windSpeedMax`
(km/h); live columns, nullable: `temperatureCurrent`, `apparentTemperature`, `humidity`, `isDay`.

**Dates are stored as noon UTC** (`new Date(`${day.date}T12:00:00Z`)`) and queried by plain
date-string comparison (`weather.date >= :start`). Two bugs taught this
([park timezone](../rules/park-timezone.md)):

- `DATE(weather.date AT TIME ZONE :tz)` on a `DATE` column casts it to midnight UTC first, so west of
  UTC the stored date moves back a day and today's row is never found (`current` was always `null` for
  US parks).
- Saving `fromZonedTime("…T00:00:00", tz)` stores the previous date east of UTC
  (`Europe/Berlin` → `2026-03-30T22:00Z`); European parks showed tomorrow's data as today.

## On the park payload

`ParkIntegrationService.buildIntegratedResponse` serves
`weather = { current, now, forecast, warnings }`: `current` is today's daily summary, `now` the live
snapshot (`null` until the first current sync), `forecast` up to 15 following days from
`WeatherService.getCurrentAndForecast`. DTO and WMO descriptions: `src/parks/dto/weather-item.dto.ts`,
`src/common/constants/wmo-weather-codes.constant.ts`.

## Severe-weather warnings

`WeatherWarningsService.syncWarnings` (`src/parks/weather-warnings.service.ts`, `weather-warnings-cron`
every 15 min) picks the source per country:

| Parks | Source | Matching |
| --- | --- | --- |
| Germany | Bright Sky (`api.brightsky.dev/alerts`, DWD's CAP warnings, no key), `BrightSkyWarningsClient` | point-based: one query per park returns its DWD warncell's warnings |
| the other MeteoAlarm countries | MeteoGate (EUMETNET's MeteoAlarm feed), `MeteoGateWarningsClient` | one country index, matched to parks by bbox and point-in-polygon |

Germany goes to DWD directly because MeteoGate's German feed lags and drops the lower awareness
levels (it once served only expired alerts while DWD had an active extreme-heat warning for Brühl).
The country gate is MeteoGate's list, so without `METEOALARM_API_TOKEN` no country syncs, Germany
included. Each sync replaces a country's rows in one transaction, hourly segments of one warning
merged into a single row. What a client reads: [weather warnings](../frontend/weather-warnings.md).

## Climate normals beyond the forecast

For a calendar day past the forecast the calendar serves the park's **climate normal**, marked so nobody
takes it for a forecast.

| | |
| --- | --- |
| Source | Open-Meteo archive (`archive-api.open-meteo.com/v1/archive`, ERA5), same provider and attribution |
| Period | 2015–2024 (`CLIMATE_NORMAL_FIRST_YEAR` / `_LAST_YEAR`, `src/external-apis/weather/climate-normals.ts`) |
| Shape | per park coordinate (0.01°), each `MM-DD` averages ±3 days over all years (`CLIMATE_NORMAL_WINDOW_DAYS`), 366 keys; max/min temperature, precipitation, rain, snowfall, wind, most frequent WMO code |
| Fetching | background, after a calendar window reaches past the forecast; at most `CLIMATE_WARM_BUDGET_PER_DAY` parks a day, a failed park not retried for an hour, own block key `ratelimit:openmeteo-archive:blocked`; ~180 units of the daily allowance per park. Cached 30 days; the calendar reads the cache only, so the first request after a miss has no normals |

Every calendar weather block carries `basis: "forecast" | "climate_normal"`, a normal also
`normalPeriod: "2015-2024"`. A normal never fills a day that has a forecast row, never feeds the day's
`recommendation`, and `/plan/day` does not use it.

**Walled off from the wait-time model**, which cannot tell "the mean of this date" from "what to expect
on this day": a normal is never written to `weather_data` (what `ml-service/db.py` trains on), never
returned by `getCurrentAndForecast` or `getHourlyForecast` (what `MLService` sends), and is cached under
its own key (`weather:climate-normals:<parkId>`). `weather.service.climate-normals.spec.ts` pins all of
it, including that neither `ml-service/*.py` nor `ml.service.ts` mentions normals.

## Open-Meteo limits

Free, keyless, at most 16 forecast days. The daily allowance is weighted per call (variables / 10 ×
days / 14). A 429 blocks every forecast call for 60 s (`ratelimit:openmeteo:blocked`); 5xx or network
failures that exhaust their retries open a 30 s circuit (`ratelimit:openmeteo:circuit`); the archive has
its own 15-min block (`ratelimit:openmeteo-archive:blocked`). Do not add calls to fill a gap — archive
what the crons already fetch.

## Call cadence per surface

| Surface | Cache | Refresh |
| --- | --- | --- |
| Daily, `weather:daily:{lat}:{lon}:{days}` | 2 h per ~1 km coordinate | the crons only |
| Hourly for ML, `weather:hourly:v2:park:{id}` (client: `weather:hourly:utc:…`) | 6 h + ≤ 1 h jitter | on demand |
| Nowcast, `weather:nowcast:raw:{parkId}` | park-hours aware (`nowcastCacheTtlSeconds`) | on demand (park page, `rain-soon` push) |

The nowcast is the only surface refreshed every 30 min, and on demand, so crawlers kept closed parks on
that cadence (2026-10-20 19:34 UTC: 20 of 29 warm nowcast keys belonged to closed parks). Its TTL is now
chosen on each fetch from the earliest OPERATING / EXTRA_HOURS / TICKETED_EVENT window not yet closed:
open or opening within the hour → 30 min; closed → 3 h, but never past one hour before the next opening;
no window known → 30 min (a missing schedule is not a closed park). Only the per-park key follows the
park; the client's per-coordinate key (`weather:nowcast:{lat}:{lon}:{steps}`) stays at 30 min so an
open park sharing coordinates with a closed one is never served stale.

## Hourly observed weather — designed, not built (PAR-1045)

CatBoost trains on the **daily** rows while serving has an **hourly** forecast; PAR-1031/1032 collapse the
forecast to daily units, so a 15:00 shower and a dry morning read the same in training. The design:

| | |
| --- | --- |
| Table | `weather_hourly_observed`: `parkId`, `ts` (UTC hour), `temperature`, `apparentTemperature`, `precipitation`, `rain`, `snowfall`, `weatherCode`, `windSpeed`, `windGusts`, `cloudCover`; PK `(parkId, ts)`; a hypertable compressed after 7 days, registered in `HYPERTABLES`. Observed values only; a missing hour stays absent |
| Source | Open-Meteo archive, `hourly=` those variables, `timezone=GMT`; the archive runs ~5 days behind, so the last training days keep the daily row |
| Size | ~211 parks × 8,760 h ≈ 1.85 M rows/yr, ≈ 230 MB uncompressed, ~25 MB compressed; two years (`TRAIN_LOOKBACK_YEARS`) ≈ 50 MB |
| Calls | backfill of 730 days for ~185 locations ≈ 8,700 units (spread over 3–4 days); then ~85 a day |
| Consumers | `ml-service/db.fetch_training_data` joins it per (park, UTC hour); serving keeps the hourly forecast; normals stay walled off |

Not built: it needs a table, a backfill, a sync job and a measured retrain.

## Key files

`src/external-apis/weather/open-meteo.client.ts` (client: daily, hourly, nowcast, archive),
`src/parks/weather.service.ts` (save, query, nowcast, normals), `src/queues/processors/weather.processor.ts`,
`src/queues/processors/weather-historical.processor.ts`.
