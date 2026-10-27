# One covariate source, read as of an origin (PAR-1061)

A design for recording **when** each schedule, holiday and weather fact became known, and for deriving
every model's covariates from those facts in one place. Open this before adding a covariate to any model,
or before reading `schedule_day_versions` / `weather_forecast_issues`.

**Status (2026-10-27): PRs 1 and 2 of §4 are built** — the two write-only logs
`weather_forecast_issues` and `schedule_day_versions`. Nothing reads them yet. PR 3 (holiday fact
columns) and everything after it are not built; the code still has the duplicated readers §2 describes.

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

## 3. Proposed model

### 3.1 Principles

1. Facts are recorded once, where they arrive (NestJS), **append-only with `known_from`**; a change closes
   the open version and writes a new one; a retraction is a version too.
2. **One derivation** turns facts into covariate semantics (holiday sets, coverage, bridge days,
   distances, windows, weather basis).
3. **Each service keeps its own encoding** (NaN for CatBoost, a `*_known` flag for TFT, channels for PCN).
   What a value *means* is not the service's decision.
4. **Unknown is a value:** `NULL` in the source, never 0 ([absent facts](../rules/absent-facts.md)).
5. **Every read names its origin**, or asks for `final` (latest facts, today's behaviour), labelled as an
   oracle in its output.

### 3.2 Layer 1: fact tables (Postgres, written by NestJS)

**`schedule_day_versions`** — built (PR 2). The park-level day as published, one row per version;
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

**`weather_forecast_issues`** — built (PR 1). Every daily forecast `weather-full-cron` already fetches,
kept: PK `(park_id, target_date, issued_at)`, `lead_days` (`target_date` − park-local date of
`issued_at`), `temp_max`, `temp_min`, `precip_sum`, `rain_sum`, `snow_sum`, `wind_max` (real),
`weather_code`. Written by `WeatherService.saveForecastIssue` from the same response, insert-or-ignore
(a re-run inside the client's 2 h cache adds nothing). Hourly forecasts are not archived (D5).

**`holidays`, three columns** — not built (PR 3): `first_seen_at` (backfill `createdAt`),
`withdrawn_at` (set when a source answer **with entries** for that country, year and type no longer
contains the row — the schedule-retraction rule), `source` (`nager` / `nager_long_weekend` /
`openholidays` / `synth_summer`).

**`holiday_coverage_versions`** — not built: `country`, `region` (`''` = national), `holiday_type`,
`covered_through` (what the source has answered for), `known_from`, `known_to`. Makes the PAR-1049
coverage as-of.

**Events** stay columns of `schedule_day_versions` until a second event source exists. **Park geography**
is read current and stated as timeless; `influencingRegions` corrections are meant to rewrite the past.

### 3.3 Layer 2: one derivation

A Python package `covariates/` (where it lives: D1) replacing `holiday_features.py`, `holiday_utils.py`,
nf-service's covariate code, `chronos_shadow.day_covariates`, `fetch_windows` /
`fetch_operating_windows` and the SQL of `add_park_schedule_features`:

```python
park_day_covariates(conn, park_ids, d0, d1, *, origin: datetime | None, final: bool = False) -> DataFrame
park_hour_covariates(conn, park_ids, t0_utc, t1_utc, *, origin, final=False) -> DataFrame
schedule_windows(conn, park_id, d0, d1, *, origin, final=False) -> DataFrame   # PCN, Chronos
```

It reads Postgres only (with each service's bounded-session settings), no HTTP hop. Output: one row per
`(park_id, local_date)`, `NULL` = unknown, in groups — own holidays (`public_holiday`, `school_holiday`,
`bridge_day`, `easter_sunday`; unknown past `covered_through` as of origin), neighbours (counts plus
`neighbor_public_1..3` for CatBoost, and `neighbor_unknown_count`), distances (capped at 30), schedule
(`sched_state` NULL = no version known, distinct from `UNKNOWN`; hours; events), weather (basis always
stated), and provenance (`sched_known_from`, `sched_asof_exact`, coverage dates, `weather_basis` =
`observed` / `forecast` / `climatology` / `oracle_observed`, issue time, lead, `origin`, `final`).
Calendar encodings (dow, doy, peak season) stay in the services.

### 3.4 As-of semantics

`origin` is a UTC instant; its local date is the park-local date of that instant.

| Covariate | At `origin` | Before the log existed |
| --- | --- | --- |
| Schedule | the version with `known_from ≤ origin < coalesce(known_to, ∞)` — a past day reads what was **published**, not what happened | the backfilled version, `asof_exact = false`: it can hide a window that was known, never show one that was not |
| Public holidays | `first_seen_at ≤ origin`, not withdrawn by then, dates ≤ `covered_through`; `synth_summer` excluded unless asked | `first_seen_at = createdAt`; `final` is a mild, documented oracle (laws fix them years ahead) |
| School holidays | the same per region; past coverage → `NULL` | coverage backfilled from today's rows |
| Bridge day | derived from the neighbouring days' public flags (D4) | — |
| Weather, past target | observed row with `updatedAt ≤ origin`, else the latest issue | `oracle_observed` |
| Weather, lead 0–15 | latest issue with `issued_at ≤ origin` → `forecast` | `oracle_observed` only with `allow_oracle=True`, else climatology |
| Weather, lead > 15 | climatology per park and month from observed rows before origin (PAR-1031); ERA5 normals stay out ([weather](weather.md#climate-normals-beyond-the-forecast)) | — |

`final=True` reads the latest facts for every date (today's behaviour); parity tests run in it.

### 3.5 How each service reads it

CatBoost serving: `origin = now` (daily weather from the latest issue, hourly unchanged until D5).
CatBoost training: `final=True` (as-of training is D2). TFT training: history `final`, future frame
`origin = now`; TFT serving `origin = now`. Chronos-2 shadow: the capture origin — the first reader whose
"as of the origin" claim becomes true. PCN: `schedule_windows(origin = tick)`. ml-bench: exports the fact
tables and builds per origin; `published_final` / `ORACLE` remain only before the logs start.

### 3.6 Size and cost

| Table | Rows | Per year |
| --- | --- | --- |
| `weather_forecast_issues` | ~150 parks × 16 days × 2 issues ≈ 4,800/day | ≈ 230 MB (≈ 80 B heap + 50 B PK per row) |
| `schedule_day_versions` | backfill ≈ 83 k; churn **assumed** ≤ 5 % of 365 future days per park per day ≈ 1 M/yr | ≈ 160 MB at the assumed churn — measure before relying on it (lower bound: `SELECT date_trunc('day', "updatedAt"), count(*) FROM schedule_entries WHERE "attractionId" IS NULL GROUP BY 1`) |
| `holiday_coverage_versions` | ~40 countries × regions × a few changes | < 1 MB |

The read side gets cheaper (~150 × 548 park-days for a full training window). Plain tables, no hypertable:
below a gigabyte they are not worth a TimescaleDB job queueing for an ACCESS EXCLUSIVE lock
([TimescaleDB lock queues](../troubleshooting/timescaledb-lock-queues.md)).

## 4. Migration, one small PR at a time

Start the clocks first — an unlogged day cannot be recovered (the same reason the
[forward archive](../ml/forward-archive/README.md) shipped before anything scored it). Each reader PR changes one
service, in `final` mode first, behind a parity test that fails if the output moves.

| # | PR | Moves a served number? | Parity / test |
| --- | --- | --- | --- |
| 1 | **built:** `weather_forecast_issues` from `weather-full-cron` | no | one issue per park per run, `lead_days` from the park-local date, no extra call |
| 2 | **built:** `schedule_day_versions` + backfill on a park's first call | no | unchanged → no row; change → close + insert; retraction → `NONE`; one open version per day |
| 3 | holiday facts: `first_seen_at`, `withdrawn_at`, `source`, `holiday_coverage_versions` | no | withdrawn only inside an answered (country, year, type) |
| 4 | `covariates/` in `final` mode, holidays only; ml-service, Chronos and ml-bench import it | no | `assign_holiday_features` before == after for every park × 548 days, dtype-exact; vendored copies byte-identical |
| 5 | `schedule_windows` replaces `fetch_windows`, `fetch_operating_windows`, `fetch_park_schedules` SQL | no | windows identical for all parks × 90 days; `is_park_open` per training row identical |
| 6 | origin mode; Chronos switches to it | shadow only | as-of at `origin = now` equals `final` for future dates; a two-version fixture returns the older one |
| 7 | ml-bench exports versions / issues / coverage | offline | a published bench cell reproduces in `final`; the as-of cell is reported beside it |
| 8 | weather reader: CatBoost serving daily fields and the TFT future frame from the latest issue | no | latest issue == the `weather_data` forecast row on deploy day |
| 9 | TFT semantics onto the package | **yes** | through the `tft_grid` shadow under the model switch rule; every renamed column's diff listed |
| 10 | as-of training (D2) | **yes** | shadow, once the issue archive is ≥ 60 days old |
| 11 | delete the old paths, including `MLService.enrichForecastWithHolidays` | no | grep proves no caller |

## 5. What not to do

- Do not materialise a wide feature table per origin (≈ 20 M rows/yr of near-identical values).
- Do not log interim states: record after the last writer, or the log fills with versions no reader saw.
- Do not delete from a fact log or update it in place (beyond closing `known_to`).
- Do not call a backfill exact: `updatedAt` is the last change, not the first publication.
- Do not encode unknown as 0 in the source; NaN, a `*_known` flag or a mask is each service's encoding.
- Do not change a model's inputs inside a parity PR; semantic changes go through a shadow.
- Do not add a covariate microservice, and do not reimplement the derivation in SQL or TypeScript (the
  NestJS calendar keeps its own `schoolCoverageThrough`, with a shared fixture test against the package).
- Do not fetch Open-Meteo more often to fill the archive ([limits](weather.md#open-meteo-limits)).
- Do not version park geography.
- Run backfills and size checks through `scripts/prod-psql.sh` ([DB health §0](../troubleshooting/db-health-runbook.md)).

## 6. Owner decisions

| # | Question | Recommendation |
| --- | --- | --- |
| D1 | Where does the derivation live? | (a) a `covariates/` package vendored into each service's build context with a byte-identical CI test, as `holiday_features.py` travels today; building from the repo root could follow; not SQL functions |
| D2 | What do the trainings learn from? | `final` facts now; as-of at the served lead as PR 10 behind a shadow once the archive covers d60 — observed-weather training vs forecast serving is the largest remaining gap |
| D3 | Weather past 16 days | climatology from our observed rows before the origin; ERA5 normals stay walled off |
| D4 | One bridge-day definition | Mon before a Tue / Fri after a Thu holiday (Chronos): derived from public flags, narrowest claim; CatBoost moves only through a retrain |
| D5 | Archive hourly weather? | no, until an intraday model shows it needs it (Chronos measured weather at < 0.12 min) |
| D6 | Retention of `weather_forecast_issues` | none for now; revisit at 1 GB |
