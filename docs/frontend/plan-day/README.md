# `/plan/day` — one day, ride by ride

The contract of the trip planner's series: the expected wait for each open hour
(and, behind a flag, each quarter-hour) of one date, for every ride the day can
be answered for, plus that day's context. Start here; each part below covers one
question about the response.

```
GET /v1/parks/:continent/:country/:city/:parkSlug/plan/day?date=YYYY-MM-DD
```

- `date` is park-local and defaults to today in the park's timezone. A malformed
  or impossible date (`2026-02-31`) is a **400**, never a day of nulls that reads
  like a closed park. Unknown park: **404**.
- **Cached 15 minutes** at the edge (`HttpCacheInterceptor`), except a response
  whose `ridesUnavailable.reason` is `data_unavailable`: that one is sent
  `no-store`, so a transient fault does not outlive itself at the edge. There is
  no origin-side response cache; the pieces underneath (calendar month, hourly
  profile, ML predictions, H5 profiles) each have their own.

Code: `src/parks/services/plan-day.service.ts`, DTO `src/parks/dto/plan-day.dto.ts`.

## The response at a glance

| Field              | What it is                                                                                   | Part                                    |
| ------------------ | -------------------------------------------------------------------------------------------- | --------------------------------------- |
| `tier`             | how the numbers were produced: `observed` / `measured` / `composed` / `climatology` / `long_range` | [tiers](./tiers.md)                     |
| `leadDays`         | whole days from today to `date`, park timezone                                               |                                         |
| `context`          | the day: hours, status, crowd level, weather, holidays (below)                               | [tiers](./tiers.md#opening-hours-can-come-from-the-data), [midnight](./midnight.md) |
| `rides[]`          | one entry per ride the day can be answered for                                               | [rides](./rides.md)                     |
| `rides[].hours[]` / `slots[]` | the hourly curve / the 15-minute curve                                            | [tiers](./tiers.md), [slots and H5](./slots-and-h5.md) |
| `ridesUnavailable` | why `rides` is empty; present exactly when it is                                             | [availability](./availability.md)       |
| `accuracy`, `leadTimeMae`, `rides[].expectedError` | how wrong the day typically is                               | [accuracy](./accuracy.md)               |
| `climatology`      | the reference days behind a `climatology` tier                                               | [tiers](./tiers.md#climatology-how-it-was-last-year) |
| `shows[]`          | the day's programme, `scheduled` or `projected`                                              | [shows](./shows.md)                     |

### `context`

| Field                                  | Notes                                                                                                                                                       |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `date`, `status`                       | the calendar's park status for the day                                                                                                                      |
| `openHour`, `closeHour`                | the operator's wall-clock pair; `closeHour < openHour` on a day past midnight — see [midnight](./midnight.md)                                                |
| `hoursSource`                          | `schedule` (published) or `observed` (measured window past the publishing horizon) — see [tiers](./tiers.md#opening-hours-can-come-from-the-data)           |
| `crowdLevel`                           | the calendar's level for the day                                                                                                                            |
| `weather`                              | absent past the forecast's reach (~14 days); no climate normal is substituted                                                                               |
| `isHoliday`, `isBridgeDay`, `isSchoolVacation` | as on the calendar                                                                                                                                  |
| `schoolVacationUnknown`                | `true` when the park's school-holiday data ends before this day, so `isSchoolVacation: false` means unknown; absent otherwise                                |
| `isWeekend`                            | Saturday or Sunday in the park's timezone                                                                                                                   |
| `neighborHolidays`                     | holidays in neighbouring regions whose day-trippers raise local crowds                                                                                      |
| `liveWaitTimes`                        | the curated readability flag, repeated here because a planner never fetches the park payload — see [live-wait-times-availability](../live-wait-times-availability.md) |
| `hasEarlyEntry`                        | only ever `true`: a human confirmed hotel guests get in early. Absent means "no" and "nobody checked" alike. It covers the park's headliners                  |
| `earlyEntryMinutesPeak` / `…OffPeak`   | minutes before `openHour` the early-entry rides open; only with `hasEarlyEntry`. No calendar behind peak/off-peak — the editor swaps the values               |

## Who reads it

- park.fan's trip planner (`lib/planner/*`; the frontend side is
  [its trip-planner docs](https://github.com/PArns/park.fan/blob/main/docs/features/trip-planner/README.md)).
- The forward archive captures d0–d7 plus d10/14/21/30/45/60/90 daily via
  `buildPlanDayWithShadow` — see [forward archive](../../ml/forward-archive/capture.md).
- The `plan-day-coverage` job — see [availability](./availability.md#the-number-is-watched).

## Parts

- [tiers.md](./tiers.md) — `tier`, `climatology`, the per-hour `source`, `dayPeak`, observed opening hours
- [slots-and-h5.md](./slots-and-h5.md) — 15-minute `slots[]` and the H5 composer behind `PLAN_DAY_H5_COMPOSER`
- [midnight.md](./midnight.md) — a day that runs past midnight
- [rides.md](./rides.md) — the ride object, which rides a day carries, `minimumHeight`/`mayGetWet`
- [availability.md](./availability.md) — rides out of season or in a works period, and `ridesUnavailable`
- [opens-at.md](./opens-at.md) — `opensAt`, `opensAtConfidence`, `runsOnWeekdays`
- [accuracy.md](./accuracy.md) — `accuracy`, `expectedError`, `leadTimeMae`
- [shows.md](./shows.md) — `shows[]`: scheduled vs projected, how far ahead, positions

## Related

- [The park's day shape (`/stats/hourly`)](../park-hourly-profile.md) — the historical hour profile the composer scales
- [Calendar: status](../calendar-schedule-status.md) — where the day's context comes from
- [Quantile serving & calibration](../../ml/quantile-serving-and-calibration.md) — where `uncertaintyMinutes` comes from
- [What we can forecast, and how far](../../ml/long-range-forecasting.md)
