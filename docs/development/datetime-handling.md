# Date and time handling

The mechanism behind the [park-timezone rule](../rules/park-timezone.md): which helper answers which
question, how to query a park-local day, how date-only values shift, the two anti-patterns that broke US
parks repeatedly, and the pandas trap in the Python services. Open it before writing code that turns a
time into a date or a date into a range.

## 1. Park time

Timestamps are stored in UTC (`TIMESTAMPTZ`) and the container runs in UTC. Every decision about a day
("is the park open today", "is this a holiday", "yesterday's stats") is made in the park's
`parks.timezone`. When it is 10:00 in Berlin it is 01:00 in Los Angeles, and `new Date()` answers for
neither.

`new Date()` is fine where no park day is involved:

- absolute metadata (`lastUpdated`, `generatedAt`, `geocodingAttemptedAt`);
- relative windows (`Date.now() - 2 * 60 * 60 * 1000` for "the last two hours", retry delays);
- comparing instants (`now >= schedule.openingTime`: both are UTC instants);
- parsing a `TIMESTAMPTZ` from the database (`new Date(row.timestamp)`).

## 2. The helpers (`src/common/utils/date.util.ts`)

| Need                                         | Helper                                                                                         |
| -------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Today / tomorrow / yesterday as `YYYY-MM-DD` | `getCurrentDateInTimezone`, `getTomorrowDateInTimezone`, `getYesterdayDateInTimezone` (`…At(now)` variants for tests) |
| An instant's park-local date                 | `formatInParkTimezone(date, tz)`                                                               |
| Park-local midnight as a UTC instant         | `getStartOfDayInTimezone(tz)` (`getStartOfDayInTimezoneAt`)                                    |
| Hour or weekday of "now" in the park         | `getCurrentTimeInTimezone(tz)`. Its result carries the park's wall clock in a `Date`; read `getHours()`/`getDay()` from it and never use it as an instant. |
| Same park day?                               | `isSameDayInTimezone(a, b, tz)`                                                                |
| Iterate days in a range                      | `eachDayAtNoonInTimezone(from, to, tz)`: one noon instant per day. Stepping `setDate(+1)` from local midnight skipped Berlin's 2027-03-28 (23-hour day). |
| Date arithmetic on `YYYY-MM-DD`              | `addIsoDays(iso, n)`; `isRealIsoDate(value)` validates one                                     |
| TTL of a cached "today" answer               | `secondsUntilEndOfDayInTimezone(tz)`: a copy must not outlive the day it describes            |

## 3. Querying a park-local day

"All wait times on 2026-05-20" is not `00:00–23:59 UTC`. Compute the park's boundaries
(`getStartOfDayInTimezone`, or `fromZonedTime('2026-05-20T00:00:00', tz)`), and prefer date-string
equality (`schedule.date = :todayStr`) for a single day so the result does not depend on the database
session's timezone. Two parks are compared only after both are normalised to their own days.

## 4. Tests

Mock the system time or pass fixed instants (the `…At(now)` helpers); never depend on the runner's local
time.

## 5. Caching an answer about today

A TTL is blind to the day boundary: a cached `isToday`, a today-only crowd reading or "the current day's
cell" is wrong the moment the copy outlives its day. The origin can re-derive those on a cache read; a CDN
copy cannot, and a browser copy cannot even be purged. Cap such a TTL with
`secondsUntilEndOfDayInTimezone(park.timezone)`.

## 6. Date-only values (holidays, schedules, events)

Holidays, school holidays and `schedule_entries.date` are `DATE` columns. A date means **that whole
calendar day in the park's timezone**. Keep it a `YYYY-MM-DD` string end to end: `new Date("2025-12-25")`
is midnight UTC, which is 19:00 on 24 December in New York. If an instant is unavoidable, use noon UTC
(`new Date(`${dateStr}T12:00:00Z`)`), which has the same date in every offset from −12 h to +12 h.

```typescript
const todayStr = getCurrentDateInTimezone(park.timezone);
const isHoliday = holiday.date === todayStr;                       // string equality
const schedule = await this.getScheduleForDate(parkId, todayStr);  // DATE = :todayStr
```

## 7. Two anti-patterns that shift US parks by a day

Each earlier fix broke either UTC parks or US parks; these two patterns were the cause every time.

**A. External date-only string → `new Date()` → `formatInParkTimezone()`.** ThemeParks.wiki returns
`{ date: "2026-03-02", openingTime: "2026-03-02T10:00:00-05:00" }`, where `date` is already the park's
local day. `new Date("2026-03-02")` is midnight UTC, and formatted in `America/New_York` it is
`2026-03-01`. Parks east of UTC are unaffected, which is why it hides. `saveScheduleData` therefore uses a
date-only string as it is:

```typescript
const raw = String(entry.date);
const dateStr = /^\d{4}-\d{2}-\d{2}$/.test(raw)
  ? raw                                          // already the park's day
  : formatInParkTimezone(new Date(raw), park.timezone);
```

**B. TypeORM `DATE` column → `formatInParkTimezone()`.** TypeORM returns a `DATE` as a `Date` at midnight
UTC; formatting it in a US timezone gives the previous day, so `formatInParkTimezone(new Date(w.date), tz)
=== todayStr` silently fails there. Read the stored date with `.toISOString().split("T")[0]` (or keep the
string).

| Source                                           | Runtime type           | Correct handling                              |
| ------------------------------------------------ | ---------------------- | --------------------------------------------- |
| External date-only string (`"2026-03-02"`)       | `string`               | Use as is; no `new Date()`                     |
| External ISO datetime (`"2026-03-02T10:00-05:00"`) | `string`             | `formatInParkTimezone(new Date(raw), tz)`     |
| TypeORM `DATE` column                            | `Date` (midnight UTC)  | `.toISOString().split("T")[0]`                |
| TypeORM `TIMESTAMPTZ` column                     | `Date` (real instant)  | `formatInParkTimezone(date, tz)`              |

## 8. Python and pandas: one column, one timezone

**A pandas datetime column carries exactly one timezone.** Writing per-park tz-aware values into one
column converts each write back to the column's timezone, silently (PAR-452).

```python
# Wrong: looks local, lands as UTC
df["local_timestamp"] = df["timestamp"]                     # datetime64[ns, UTC]
for park_id, idx in df.groupby("parkId").groups.items():
    df.loc[idx, "local_timestamp"] = df.loc[idx, "timestamp"].dt.tz_convert(tz_map[park_id])

# Right: convert once per timezone, store the wall clock naive
tz_per_row = df["parkId"].map(tz_map).fillna("UTC")
local = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
for tz_name, idx in df.groupby(tz_per_row, sort=False).groups.items():
    local.loc[idx] = df.loc[idx, "timestamp"].dt.tz_convert(tz_name).dt.tz_localize(None)
df["local_timestamp"] = local                               # datetime64[ns]
```

**`local_timestamp` is a reading, not an instant.**

| Need                                                                                  | Column                                      |
| ------------------------------------------------------------------------------------- | ------------------------------------------- |
| Wall-clock features: `hour`, `day_of_week`, `month`, `season`, `date_local`           | `local_timestamp` (naive)                   |
| Join key for holidays, school holidays, schedules, `downtime_minutes_today`           | `date_local`                                |
| Any comparison with a `TIMESTAMPTZ` (`openingTime`, `closingTime`) or an API instant (`parkOpeningTimes`) | `timestamp` (UTC), never `local_timestamp` |

The schedule joins in `features.py` and `predict.py` are the reference: join on the local date, compare
the instants. While this was broken, `hour` was wrong on 99.8 % of production training rows and
`date_local` on 3.1 %, and nothing failed, because training and serving were wrong the same way
(`ml-service/test_local_time_features.py`).

**Values crossing the API ↔ ML boundary (PAR-818):**

| Value                                      | Rule                                                                                                                                                                                                         |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `weatherForecast[].time` sent to `/predict` | A UTC instant. The hourly Open-Meteo call asks for `timezone: "GMT"`: with `"auto"` it applies the offset in force at request time to every hour, which is wrong across a DST change. The DB fallback (`synthesizeHourlyFromDaily`) steps real UTC hours between local midnights. |
| Historical occupancy profile lookup        | Keyed on park-local (Postgres DOW, hour), read from `local_timestamp` with `pg_dow = (pandas_dow + 1) % 7`.                                                                                                 |
| Daily peak hours (`DAILY_PEAK_HOURS`, `12,14,16`) | Hours on the park's clock, collapsed per park-local date; the daily row is published at park-local noon (`daily_anchor_time`).                                                                        |
| An API-side date key for a prediction      | `MLService.localDateOf(predictedTime, tz)`, never `predictedTime.slice(0, 10)`.                                                                                                                              |

Tests: `ml-service/test_park_local_daily_and_occupancy.py`.
