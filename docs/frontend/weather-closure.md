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

- the ride carries an `outage` or a [`notRunToday`](./not-run-today.md) and reads `CLOSED` or
  `DOWN`, so it is shut while the park is open, in its season and outside a works period; a ride
  the park lists in maintenance never gets the field;
- the park is inside an `OPERATING` window now;
- the ride's learned profile for the cause clears the bar: probability **≥ 0.9** and the cause
  shuts the ride in at least 15 % of its hours (lower bound);
- for cold: it is colder than the ride's `thresholdC` this hour. Cold is asked first, because its
  profile is learned on dry hours and holds with or without snow;
- for snow, when the cold does not answer: at least 0.1 cm fell in the last 24 hours and it is
  below 6 °C now.

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
forecast. The forecast is Open-Meteo's own; when it cannot be read there is no verdict, never one
from hours synthesised out of the daily minimum and maximum.

## How the probability is made

A weekly job (`weather-closure` / `rebuild-weather-closure-profiles`, Mondays 05:45, its own
queue) reads a year of each park's whole open hours: a ride is shut in an hour that read CLOSED or
DOWN and never OPERATING. Our own bookkeeping rows (reconciliation and the heartbeat's invented
CLOSED) are not readings, and an hour counts only when the park's feed delivered a real
observation in it.

Dropped before learning, because each would pass for weather:

- a ride out of season: it must have run within a week **before and after** the day, so the days
  after its last run of the year do not count;
- a streak of four or more consecutive open days shut from first hour to last: a rehab, not a cold
  morning;
- hours in which over 30 % of the park's other in-season rides were shut.

Days are the park's own calendar days. The cold profile is learned on dry hours (under 0.1 cm of
snow in the last 24), the snow profile on the snowy ones at or above a publishable cold threshold,
each against the ride's ordinary rate at 12–25 °C, and each needs 30 hours over eight days.

The probability is the attributable fraction `(p_condition − p_ordinary) / p_condition`, taken
from the pessimistic end of both rates: the Wilson lower bound of the first against the upper
bound of the second, with **one day counted as one observation**. The cold threshold is chosen and
judged on the same hours, which flatters it; the day-counted bound and the 15 % floor are there to
absorb that.

Measured on Europa-Park and Phantasialand (December 2025 to November 2026): Poseidon is shut in
about half of the dry hours under 3 °C and in 0.2 % at 12–25 °C. Taron is shut in 0 % of dry cold
hours and in about a quarter of the hours after snow, which is why the two causes are kept apart.
With every rule above, no ride of the two parks clears the bar after this one winter: Poseidon's
cold profile stands at 0.85. The bar is meant to be reached by more winter days, not by a looser
rule. No park was open below −3 °C, so the claims that Taron runs to −10 °C and Black Mamba to
−5 °C are neither confirmed nor refuted. The profiles table is
`attraction_weather_closure_profiles`; it keeps every ride with a profile, also those below the
bar, so a ride approaching it can be watched.

## In the admin

`GET /v1/admin/weather-closure` lists every stored profile with its ride and park, best probability
first, with `publishable` and `totals.near` (not shown, best probability at least 0.75), so a ride
approaching the bar can be watched. `POST /v1/admin/weather-closure/rebuild` (editor) runs the job
now instead of on Monday. The MCP has the same list as `weather_closure_profiles` and the rebuild
as `run_job` `rebuild-weather-closure`.

## Who reads it

The park page's ride cards and the ride page (frontend `WeatherClosureNote`), Parky's `get_ride`
as `reopening.reason: "weather"`, and the MCP `get_park` (on `down` and `notRunToday` entries) and
`get_attraction`.

## Pinned by

`weather-closure.util.spec.ts` (the threshold, the day-counted bound, the eight-day and 15 %
floors, snow kept apart from cold both ways, the park-wide, season-end and rehab filters, park-local
days, the outlook against the curve and the close, the 0.9 bar) and the `reopeningOf` cases in
`assistant-tools.spec.ts`.
