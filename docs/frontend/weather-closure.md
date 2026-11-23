# Shut by the cold or the snow (`weatherClosure`)

The contract for the line that says why a ride is shut during opening hours when the weather is
the reason, and when today's temperature curve lets it open (PAR-1414). On the park payload's
attractions and on the attraction detail response:

```json
"weatherClosure": {
  "cause": "cold",
  "probability": 0.91,
  "temperatureC": 1.2,
  "thresholdC": 4.5,
  "outlook": "opens_when_warmer",
  "expectedOpenAt": "2026-11-23T12:00:00.000Z"
}
```

## When it is present

All of these, decided by `decideWeatherClosure` (`src/parks/utils/weather-closure.util.ts`):

- the ride carries an `outage` or a [`notRunToday`](./not-run-today.md), so it is shut while
  the park is open, in its season and outside a works period; those two rules carry the gates;
- the park is inside an `OPERATING` window now;
- the ride's learned profile for the cause clears **0.9**;
- for cold: it is colder than the ride's `thresholdC` this hour, and no snow fell in the last
  24 hours (snow would make "cold" the wrong word for the rides that close for snow and not for
  cold); for snow: snow fell in the last 24 hours.

Absent means nothing may be said. A ride shut at 15 °C is shut for something else.

## What the fields say

| Field            | Meaning                                                                                            |
| ---------------- | -------------------------------------------------------------------------------------------------- |
| `cause`          | `cold` or `snow`. Render nothing for a value you do not know.                                      |
| `probability`    | How likely the weather is the reason for this closure, 0.9–1.                                      |
| `temperatureC`   | The park's temperature this hour.                                                                  |
| `thresholdC`     | Cold only: the temperature from which the ride usually runs, learned from its own hours.           |
| `outlook`        | Cold only: `opens_when_warmer` or `likely_closed_today`. Null for snow.                            |
| `expectedOpenAt` | With `opens_when_warmer`: the first forecast hour at or above `thresholdC` before the park closes. |

`expectedOpenAt` is read off the forecast, not announced by the park: write it as "probably
around 13:00", never "opens at 13:00". Snow has no time because the clearing of a track is not
forecast.

## How the probability is made

A weekly job (`downtime` / `rebuild-weather-closure-profiles`, Mondays 05:45) reads a year of each
park's whole open hours: a ride is shut in an hour that read CLOSED or DOWN and never
OPERATING. Out-of-season hours and hours in which over 30 % of the park's other rides were shut
are dropped. The cold profile is learned on dry hours (no snow in the last 24), the snow profile
on the snowy ones, each against the ride's ordinary rate at 12–25 °C.

The probability is the attributable fraction `(p_condition − p_ordinary) / p_condition`, taken
from the pessimistic end of both rates: the Wilson lower bound of the first against the upper
bound of the second, with **one day counted as one observation**. Counting hours, one snowy
afternoon with a long fault vouched for itself ten times; Europa-Park and Phantasialand had eight
rides over 0.9 that way and one (Poseidon) with days.

Measured there (December 2025 to November 2026): Poseidon is shut in 66 % of dry hours under
4 °C and 0.2 % at 12–25 °C. Taron is shut in 0 % of dry cold hours and in 29 % after snow, which
is why the two causes are kept apart. No park was open below −3 °C, so the claims that Taron runs
to −10 °C and Black Mamba to −5 °C are neither confirmed nor refuted. The profiles table is
`attraction_weather_closure_profiles`; it keeps every ride with a profile, also those below the
bar, so a ride approaching it can be watched.

## Who reads it

The park page's ride cards and the ride page (frontend `WeatherClosureNote`), Parky's `get_ride`
as `reopening.reason: "weather"`, and the MCP `get_park` (on `down` and `notRunToday` entries) and
`get_attraction`.

## Pinned by

`weather-closure.util.spec.ts` (the threshold, the day-counted bound, snow kept apart from cold,
the park-wide and season filters, the outlook against the curve and the close, the 0.9 bar) and
the `reopeningOf` cases in `assistant-tools.spec.ts`.
