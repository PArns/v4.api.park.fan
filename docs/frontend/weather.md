# Park weather endpoints (`/weather`, `/weather/forecast`, `/weather/nowcast`)

The contract for the three weather endpoints of a park: the daily rows, the forecast, and the
15-minute nowcast with its timestamps. Open it when rendering weather outside the park payload or
building a rain alert. The `weather` block on the park payload and how the data is fetched and stored:
[weather](../architecture/weather.md). Severe-weather warnings, also carried by the nowcast:
[weather warnings](./weather-warnings.md).

| Endpoint (under `/v1/parks/:continent/:country/:city/:park`) | Returns | HTTP cache |
| --- | --- | --- |
| `GET …/weather?from=&to=` | `{ park, weather[], attribution }`: daily rows | 1 h |
| `GET …/weather/forecast` | the same shape, forecast rows only | 1 h |
| `GET …/weather/nowcast` | `WeatherNowcastDto`: the next ~6 h in 15-minute steps | until `nextUpdateAt` |

Show `attribution` (`url`, `license`, `attribution`: Open-Meteo, CC BY 4.0) wherever the data appears;
the licence requires it.

## Daily rows

```jsonc
{ "date": "2026-10-27", "dataType": "current", "temperatureMax": 14.2, "temperatureMin": 6.1,
  "precipitationSum": 0.4, "rainSum": 0.4, "snowfallSum": 0, "weatherCode": 61,
  "weatherDescription": "Rain, slight intensity", "windSpeedMax": 18.0 }
```

- `dataType`: `historical` for past days, `current` for the park's today, `forecast` after it. Units:
  °C, mm, cm, km/h; `weatherCode` is WMO.
- `/weather`: `from` and `to` are park-local `YYYY-MM-DD`; **send both**. `from` defaults to 30 days
  ago, but a missing `to` becomes `from` itself (`parseDateRange` adds the endpoint's 0 days to
  `from`), so a bare request returns one day a month back, not the month Swagger describes. More than
  90 days, `to` before `from`, or a day the month does not have answer 400.
- `/weather/forecast`: only `forecast` rows, so **today is not in it**; up to 15 days after today.
- A day without a row is simply missing, and a park without coordinates answers `weather: []`.
- The live fields (current temperature, humidity, `isDay`) are not on these endpoints: they are on the
  park payload's `weather.now` and on the nowcast. Climate normals past the forecast are served by the
  calendar only.

## The nowcast

- **`observedAt` is the time of the request**, not when the forecast was fetched; Swagger says
  otherwise. The upstream window behind it is refetched every 30 minutes while the park is open or
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
- No coordinates, or an upstream failure, answer **404**, not the 503 Swagger names.
