# One covariate source, read as of an origin (PAR-1061)

Where each model's schedule, holiday, weather and event inputs come from today, and the two
append-only logs that record **when** a schedule or weather fact became known. Open this before adding
a covariate to any model, or before reading `schedule_day_versions` / `weather_forecast_issues`.

The two logs are written and **nothing reads them yet**; every service still has its own reader (§2).
The plan built on them — holiday fact columns, one shared `covariates/` derivation read "as of an
origin", the migration one service at a time and its owner decisions — is PAR-1147.

## 1. Why

Most covariate defects of 2026 were one defect: two code paths that should mean the same thing and do
not. The neighbour-holiday cap (#98) was fixed twice (`features.py`, `predict.py`); PAR-816 found training
and inference reading different holiday types (hence `holiday_features.py`, now vendored into nf-service);
PAR-1030/1031/1032 found CatBoost serving building weather, lags and climatology differently from
training; PAR-1039 found heartbeat rows treated differently by different readers; PAR-1049 had to land in
four places (ml-service, TFT, Chronos, NestJS). Under all of them: **no reader can ask what was known at an
origin**, because the tables are overwritten in place. ml-bench therefore labels its schedules
`published_final` and its weather `ORACLE` ([level drivers](../ml/level-drivers.md)), and every offline
covariate comparison is an upper bound.

## 2. What exists today

### 2.1 What the tables can say

| Table | Written by | History? |
| --- | --- | --- |
| `schedule_entries` | `saveScheduleData`, `fillScheduleGaps`, the silent-feed guard, Wartezeiten, the live fallback, hand-written CLOSED rows ([schedule sync](schedule-sync-and-calendar.md)) | No: upserted, retracted days deleted, gap-filled days promoted/demoted in place; `updatedAt` is the last change; no provenance column. Now logged by `schedule_day_versions` |
| `holidays` | Nager public + long weekends, OpenHolidays school, curated files, `synth-summer:` projections | Upsert by `externalId` (contains the date); a moved holiday becomes a new row and the old one **stays**; `createdAt` ≈ first seen; rows older than 3 years are deleted |
| `weather_data` | `weather-full-cron`, `weather-current-cron`, `weather-historical` ([weather](weather.md)) | No: a forecast row is overwritten every 12 h and later becomes the observed row of the same key. Now logged by `weather_forecast_issues` |
| hourly Open-Meteo forecast | `WeatherService.getHourlyForecast`, Redis only | not stored |
| `parks` geography | park sync, curation | no (`influencingRegions` corrections apply retroactively) |
| `park_day_operations` | measured verdict for past days ([calendar rules §1.5](calendar-schedule-and-ml-rules.md#15-a-past-closed-day-the-measurement-overrules-par-697)) | an outcome, not a forecast input |

### 2.2 Covariate × service (as designed against, 2026-10)

| Covariate | ml-service (CatBoost) | nf-service TFT | Chronos-2 | pcn-service | ml-bench |
| --- | --- | --- | --- | --- | --- |
| Public holiday, own park | `holiday_features.py`: public ∪ bank ∪ bridge, regional or national, Easter Sunday for 10 countries | `db.add_calendar_covariates`: public ∪ bank, bridge separate, no Easter | vendored `holiday_features.py` | none | imports ml-service's |
| Neighbour holidays | per influencing region: 3 public slots + `neighbor_school_holiday_count` | one flag, public **or school** | counts | — | as ml-service |
| School holiday | flag + coverage NaN | flag, distance, `school_holiday_known` (grid) | flag; `holidays_through` | — | as ml-service |
| Bridge day | `holiday_utils.calculate_holiday_info` (weekday next to a public holiday) | Nager `bridge` rows | Mon before a Tue / Fri after a Thu holiday | — | `schedule_entries.isBridgeDay` |
| Opening hours | `add_park_schedule_features`, `is_park_open` with a wait override | grid only: `sched_open` / `sched_closed` | `fetch_windows` | `fetch_operating_windows` (same SQL, own copy) | `windows`, masked by `updated_us` |
| Events | `has_special_event`, `has_extra_hours` | — | — | — | drivers `ev` |
| Weather, training | `weather_data` historical (observed) | all types, median fill | none | none | observed, flagged `ORACLE` |
| Weather, serving | live hourly from NestJS, daily equivalents (PAR-1032), climatology beyond (PAR-1031) | forecast rows ≤ 16 days, median fill | — | — | — |

No column is as-of anywhere. Three things only the table shows: **the same name means different things**
(`is_holiday_primary` includes bridge days in CatBoost and excludes them in TFT; "neighbour holiday" is
public-only in one and public-or-school in the other; bridge day has four definitions); **NestJS computes
covariates nobody reads** (`MLService.enrichForecastWithHolidays` adds holiday flags to every
`weatherForecast[]` item, 3 queries per day per park; ml-service derives its own); and **no reader can be
right about the past** while the data is overwritten.

## 3. The two logs

Facts are recorded once, where they arrive (NestJS), append-only with `known_from`: a change closes the
open version and writes a new one, and a retraction is a version too.

**`schedule_day_versions`.** The park-level day as published, one row per version;
entity `src/parks/entities/schedule-day-version.entity.ts`, writer
`recordScheduleDayVersions` (`src/parks/utils/schedule-version-log.ts`) via
`ParksService.recordScheduleVersions(parkId, source)`.

| Column | Notes |
| --- | --- |
| `park_id`, `day` (park-local date), `known_from` | PK; `known_to` null = current, one open version per `(park_id, day)` (`uq_schedule_day_versions_open`, partial unique) |
| `state` | `OPERATING` / `CLOSED` / `UNKNOWN` / `NONE` (no park-level row: never published, or retracted) |
| `open_utc`, `close_utc`, `n_windows` | earliest OPERATING opening, latest closing — the `fetch_windows` rule |
| `ticketed_event`, `private_event`, `extra_hours`, `extra_open_utc`, `extra_close_utc` | from the event rows of that day |
| `source` | `themeparks` / `wartezeiten` / `live_fallback` / `gap_fill` / `backfill` |
| `asof_exact` | false for a park's first versions (`known_from = max(updatedAt)`) |

It is called **after** the last writer of a sync has finished with a park, derives the day state from
`schedule_entries` (park-level rows), and writes only changed days, under a per-park advisory lock. No FK
(a park merge re-points it through `PARK_DEPENDENCIES`), no hypertable.

**`weather_forecast_issues`.** Every daily forecast `weather-full-cron` already fetches, kept:
PK `(park_id, target_date, issued_at)`, `lead_days` (`target_date` − park-local date of `issued_at`),
`temp_max`, `temp_min`, `precip_sum`, `rain_sum`, `snow_sum`, `wind_max` (real), `weather_code`. Written
by `WeatherService.saveForecastIssue` from the same response, insert-or-ignore (a re-run inside the
client's 2 h cache adds nothing), so it costs no extra Open-Meteo call. Hourly forecasts are not
archived.

Rules for both logs:

- Record after the last writer of a sync, never an interim state, or the log fills with versions no
  reader saw.
- Never delete from a log or update it in place, beyond closing `known_to`.
- A backfill is not exact: `updatedAt` is the last change, not the first publication (`asof_exact`).
- Unknown is `NULL` in the source, never 0 ([absent facts](../rules/absent-facts.md)).
- Never fetch Open-Meteo more often to fill the archive ([limits](weather.md#open-meteo-limits)).
- Backfills and size checks go through `scripts/prod-psql.sh`
  ([DB health §0](../troubleshooting/db-health-runbook.md)).

**Size.** Estimated, not yet measured: `weather_forecast_issues` ~150 parks × 16 days × 2 issues ≈
4,800 rows a day, ≈ 230 MB a year; `schedule_day_versions` a backfill of ≈ 83 k rows, then ≈ 160 MB a
year if ≤ 5 % of the 365 future days per park change daily (lower bound:
`SELECT date_trunc('day', "updatedAt"), count(*) FROM schedule_entries WHERE "attractionId" IS NULL GROUP BY 1`).
Both are plain tables, no hypertable: below a gigabyte they are not worth a TimescaleDB job queueing for
an ACCESS EXCLUSIVE lock ([TimescaleDB lock queues](../troubleshooting/timescaledb-lock-queues.md)).
