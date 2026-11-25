# Park weather endpoints (`/weather`, `/weather/forecast`, `/weather/nowcast`, `/weather/hourly`)

The contract for the four weather endpoints of a park: the daily rows, the forecast, the
15-minute nowcast with its timestamps, and the hour-by-hour days for a day curve. Open it when rendering weather outside the park payload or
building a rain alert. The `weather` block on the park payload and how the data is fetched and stored:
[weather](../architecture/weather.md). Severe-weather warnings, also carried by the nowcast:
[weather warnings](./weather-warnings.md).

| Endpoint (under `/v1/parks/:continent/:country/:city/:park`) | Returns | HTTP cache |
| --- | --- | --- |
| `GET …/weather?from=&to=` | `{ park, weather[], attribution }`: daily rows | 1 h |
| `GET …/weather/forecast` | the same shape, forecast rows only | 1 h |
| `GET …/weather/nowcast` | `WeatherNowcastDto`: the next ~6 h in 15-minute steps | until `nextUpdateAt` |
| `GET …/weather/hourly` | `WeatherHourlyDto`: today + up to 14 days, hour by hour | until `nextUpdateAt` |

Show `attribution` (`url`, `license`, `attribution`: Open-Meteo, CC BY 4.0) wherever the data appears;
the licence requires it.

## Daily rows

```jsonc
{ "date": "2026-10-27", "dataType": "current", "temperatureMax": 14.2, "temperatureMin": 6.1,
  "precipitationSum": 0.4, "rainSum": 0.4, "snowfallSum": 0, "weatherCode": 61,
  "weatherDescription": "Rain, slight intensity", "windSpeedMax": 18.0,
  "windGustsMax": 41.4, "precipitationProbabilityMax": 65, "precipitationHours": 3,
  "uvIndexMax": 1.2, "sunshineHours": 2.4 }
```

- `precipitationProbabilityMax` is the highest chance of precipitation in any hour of the day (%),
  `precipitationHours` how many hours have some, `windGustsMax` the strongest gust (km/h),
  `sunshineHours` the sunshine over the day. All five are `null` on a row stored before they were
  fetched (PAR-1393).

- `dataType`: `historical` for past days, `current` for the park's today, `forecast` after it. Units:
  °C, mm, cm, km/h; `weatherCode` is WMO.
- `/weather`: `from` and `to` are park-local `YYYY-MM-DD`. `from` defaults to 30 days ago and `to`
  to the park's today, so a bare request returns the last month. More than 90 days, `to` before
  `from` (also a future `from` without `to`), or a day the month does not have answer 400.
- `/weather/forecast`: only `forecast` rows, so **today is not in it**; up to 15 days after today.
- A day without a row is simply missing, and a park without coordinates answers `weather: []`.
- The live fields (current temperature, humidity, `isDay`) are not on these endpoints: they are on the
  park payload's `weather.now` and on the nowcast. Climate normals past the forecast are served by the
  calendar (with their spread: `tempMaxP25`…`tempMinP75`, `wetDayPercent`), the day planner's
  `climateNormal` and the trip planner's `climate` blocks
  ([the spread](../architecture/weather.md#the-spread-typically-1618-c)).

## The nowcast

- **`observedAt` is the time of the request**, not when the forecast was fetched. The upstream window behind it is refetched every 30 minutes while the park is open or
  opens within the hour, and up to every 3 hours while it is closed
  ([call cadence](../architecture/weather.md#call-cadence-per-surface)); each read re-derives the
  snapshot against now.
- **`nextUpdateAt` is the next quarter-hour mark** (:00, :15, :30, :45). The response's `max-age` runs
  exactly to it, with no stale window, so poll then.
- `currentTemperatureC`, `currentApparentTemperatureC`, `currentHumidity` and `isDay` are Open-Meteo's
  current block as of the fetch; the precipitation, code, wind, snowfall and visibility fields are the
  15-minute slot containing `observedAt`, or the next slot with a value.
- **Event times** (`rainStartsAt`, `rainEndsAt`, `thunderstormStartsAt`, …, `stormEndsAt`) are UTC ISO
  timestamps. A `*StartsAt` is null both when nothing is coming and when it is already happening, so
  read `currentlyRaining` first; a null `*EndsAt` means nothing in the window or that it lasts past it.
  Rain starts at 0.1 mm per slot, a storm at gusts of 75 km/h, a thunderstorm at WMO 95/96/99, hail at
  96/99.
- **`steps[].time` is park-local without an offset** (`"2026-10-27T14:15"`), as Open-Meteo sends it,
  and the list includes the slots of the cached window that are already past. Read it in the park's
  `timezone`.
- No coordinates, or an upstream failure, answer **404**.

## The hourly days

One curve per park-local day, midnight to midnight on the park's clock: the park's **today first**,
then up to 14 following days (the forecast strip's), **15 at most**. A tap on a forecast day reads
`days[i]`; nothing past today + 14 is served.

```jsonc
{ "park": { "id": "…", "name": "Phantasialand", "slug": "phantasialand", "timezone": "Europe/Berlin" },
  "generatedAt": "2026-11-25T09:10:00.000Z", "nextUpdateAt": "2026-11-25T11:10:00.000Z",
  "days": [
    { "date": "2026-11-25", "sunrise": "2026-11-25T08:01+01:00", "sunset": "2026-11-25T16:38+01:00",
      "hours": [
        { "time": "2026-11-25T00:00+01:00", "temperatureC": -1.2, "apparentTemperatureC": -5.0,
          "precipitationMm": 0, "precipitationProbability": 5, "weatherCode": 0,
          "windSpeedKmh": 12.4, "windGustsKmh": 30.2, "windDirectionDeg": 250, "isDay": false }
        // … 24 hours
      ] }
    // … up to 14 more days
  ],
  "attribution": { "url": "https://open-meteo.com/", "license": "CC-BY-4.0", "attribution": "Weather data by Open-Meteo.com" } }
```

- **`time`, `sunrise`, `sunset` are on the park's clock with their offset** (`…T14:00+01:00`): one string
  is both the instant and the wall time, so a client needs no time-zone database to place an hour.
  Read `date` as the park-local day.
- **A DST day has 23 or 25 hours.** Spring forward skips 02:00 (23 entries); fall back lists 02:00
  twice, `+02:00` then `+01:00` (25 entries). Draw the axis by instant, not by index × 1 h.
- A day the upstream window cuts short is left out, so every listed day is whole; `sunrise` / `sunset`
  are null where none falls on that date.
- Units: °C, mm per hour, % (0–100), km/h, degrees the wind comes **from**. `weatherCode` is WMO, as on
  the other routes; `isDay` is Open-Meteo's daylight flag for the hour. Any value can be null.
- **`nextUpdateAt`** is when the forecast behind the answer is refetched (`generatedAt` + 2 h), or the
  park's next midnight if that comes first (today changes then); never less than a minute away. The
  response's `max-age` runs exactly to it, with no stale window, as on the nowcast.
- Cost: one Open-Meteo request per ~1 km coordinate per 2 h (`weather:hourly-detail:{lat}:{lon}`),
  shared by every park and request on it; the park's days are cut from it at read time.
  [Call cadence](../architecture/weather.md#call-cadence-per-surface).
- No coordinates, or an upstream failure, answer **404**. Key permission: `live.weather`.
