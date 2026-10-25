# One covariate source, read as of an origin (PAR-1061)

**Status: design. PRs 1 and 2 of §4 are built (the two write-only logs, PAR-1061); nothing reads them yet.** Every model reads schedules, holidays, weather
and events through its own SQL and its own derivation. This page proposes one set
of fact tables that record **when** each fact became known, one derivation that
turns those facts into covariates, and a migration that moves one service at a
time with a parity test in front of each move.

---

## 1. Why

Most of the covariate defects of the last months were the same defect: two code
paths that should mean the same thing and do not.

| Defect | What diverged |
| --- | --- |
| Neighbour-holiday cap (#98) | `check_neighbor_holidays` counted only the first 3 influencing regions; BE at country level never matched. Fixed twice, in `features.py` and `predict.py`. |
| PAR-816 | Training merged holidays by index label and read another row's holiday; inference kept the last row; the two counted different holiday types. Result: `holiday_features.py`, now copied into nf-service as well. |
| PAR-1030 / 1031 / 1032 | CatBoost serving built weather, lags and climatology differently from training (first row's month for all rows, 20 °C past the forecast, hourly vs daily units). |
| PAR-1039 | Heartbeat rows were targets and context in some readers and not in others. |
| PAR-1049 (#557) | A school break that has not been published yet read as "no school holiday" in every consumer. The fix had to land in four places (ml-service, nf TFT, nf Chronos, NestJS). |

Each fix was right, and each had to be made once per reader. Another problem sits
under all of them: **no reader can ask what was known at an origin**, because the
tables overwrite in place. ml-bench therefore labels its schedules
`published_final` and its weather `ORACLE`
([level drivers](../ml/level-drivers.md), `ml-bench/mlbench/export.py`), and every
offline comparison of a covariate is an upper bound.

---

## 2. What exists today

### 2.1 Storage: what the tables can and cannot say

| Table | Key | Written by | History? | When-known signal |
| --- | --- | --- | --- | --- |
| `schedule_entries` | uuid; logical `(parkId, date, scheduleType)` + `attractionId` | `saveScheduleData` (Wiki 03:00 + 15:00, on-demand), `fillScheduleGaps`, the silent-feed guard, Wartezeiten, the live fallback in `WaitTimesProcessor`, hand-written CLOSED rows | **No.** Upserted in place. A withdrawn day is **deleted** (retraction, PAR-538), and a gap-filled day is promoted or demoted in place | `updatedAt` = last change only. There is no provenance column ([schedule sync](schedule-sync-and-calendar.md)) |
| `holidays` | `externalId` (unique; contains the date) | Nager public + long weekends, OpenHolidays school, `synth-summer:` projections | Upsert only; a moved holiday becomes a new row and the old row **stays**. Rows older than 3 years are deleted | `createdAt` ≈ first seen (the upsert keeps it) |
| `weather_data` | `(parkId, date)` | `weather-full-cron` 00:00 + 12:00 (16 days), `weather-current-cron` 07:00 + 19:00, `weather-historical-cron` 05:00 relabels the past | **No.** A forecast row is overwritten every 12 h and becomes the observed row of the same key | `updatedAt`, `lastUpdated`; `dataType` says which of the three a row is **now** |
| Hourly Open-Meteo forecast | — | `WeatherService.getHourlyForecast`, Redis only (30 min) | **Not stored** | — |
| `parks` geography | `id` | park sync, curated | No | — (`influencingRegions` corrections apply retroactively) |
| `park_day_operations` | `(park_id, day)` | measured verdict for past days (calendar §1.5) | No | `computed_at` (an outcome, not a forecast input) |

### 2.2 Covariate × service

"As-of?" asks whether the reader can restrict the value to what was known at its
origin. ✗ = it reads the latest value, whatever the origin.

| Covariate | ml-service (CatBoost) | nf-service TFT | nf-service Chronos-2 | pcn-service | ml-bench | As-of? |
| --- | --- | --- | --- | --- | --- | --- |
| Public holiday, own park | `holiday_features.py`: public ∪ bank ∪ bridge, regional OR national, Easter Sunday added for 10 countries | own code in `db.add_calendar_covariates`: public ∪ bank, **bridge separate**, no Easter rule | vendored `holiday_features.py` (byte-identical test in `tests/test_chronos_shadow.py`) | none (`is_holiday` channel rejected) | imports ml-service's `holiday_features.py` | ✗ everywhere |
| Neighbour holidays | per influencing region: 3 public slots + `neighbor_school_holiday_count`, uncapped totals | **one** flag, public **or school** of any neighbour | `holiday_count_total`, `school_holiday_count_total` | — | as ml-service | ✗ |
| School holiday | flag + coverage NaN (#557) | flag, distance, `school_holiday_known` (#557, grid only) | flag; `holidays_through` per type (#557) | — | as ml-service | ✗ — #557 coverage is the **current** last date, applied to every origin |
| Bridge day | `holiday_utils.calculate_holiday_info` heuristic (weekday next to a public holiday) | Nager `bridge` rows | Mon before a Tue holiday / Fri after a Thu holiday | — | `schedule_entries.isBridgeDay` | ✗ — **four definitions** |
| Days until / since holiday | `add_holiday_distance_features` (public ∪ bridge) | `_holiday_distance` (public), school distance in grid | — | — | drivers: own lags | ✗ |
| Opening hours / is open | `add_park_schedule_features` (`fetch_park_schedules`, 5-min cache), `is_park_open` with a wait-time override; `park_has_schedule` | grid only: `sched_open` / `sched_closed` (0/0 = unknown) | `fetch_windows`: min open / max close of OPERATING | `fetch_operating_windows` (same SQL, separate copy) | `windows` table; drivers mask by `updated_us` | ✗ (ml-bench approximates with `updatedAt`) |
| Events | `has_special_event` (TICKETED/PRIVATE), `has_extra_hours` | — | — | — | drivers `ev` (ticketed, extra) | ✗ |
| Weather, training | `weather_data` `historical` (observed) joined on park-local date | `weather_data` all types; per-park median fill, then constants | none | none | observed only, flagged `ORACLE` | ✗ — the training sees observed weather |
| Weather, serving | live hourly Open-Meteo from NestJS (`weatherForecast[]`), daily equivalents (PAR-1032), climatology from 3 years of `weather_data` beyond it (PAR-1031) | `weather_data` forecast rows (≤ 16 days), median fill beyond | — | — | — | ✗ — no issue time is kept anywhere |
| Calendar (dow, doy, season, peak season) | `add_time_features` | own; southern-hemisphere peak (PAR-1033) | — | `_slot_time_features` | own | n/a (deterministic) |

Three things the table shows that no single file does:

1. **The same name means different things.** `is_holiday_primary` includes bridge
   days in CatBoost and excludes them in TFT. "Neighbour holiday" is public-only in
   CatBoost and public-or-school in TFT. Bridge day has four definitions.
2. **NestJS computes covariates nobody reads.** `MLService.enrichForecastWithHolidays`
   attaches `isHoliday` / `isSchoolHoliday` / `isBridgeDay` to every
   `weatherForecast[]` item (3 queries per day per park). `ml-service` derives its
   own and never reads those fields.
3. **No reader can be correct about the past**, because no table keeps it.
   Getting the code right does not help while the data is overwritten.

---

## 3. Proposed model

### 3.1 Principles

1. **Facts are recorded once, where they arrive** (NestJS, which already syncs
   them), **append-only, with `known_from`**. A change closes the open version and
   writes a new one. A retraction is a version too, and nothing is ever deleted
   from the log.
2. **One derivation.** Holiday sets, coverage, bridge days, distances, windows,
   weather basis: one implementation turns facts into covariate *semantics*.
3. **Each service keeps its own encoding.** CatBoost takes NaN, TFT needs a
   `*_known` 0/1 beside a placeholder, PCN needs channels. That is model input
   format and stays in the service. What a value *means* is not the service's
   decision.
4. **Unknown is a value.** `NULL` in the source, never 0
   ([absent facts](../rules/absent-facts.md)). Before a version log has an
   entry, the answer is unknown, not "closed" and not "no holiday".
5. **Every read names its origin**, or explicitly asks for `final` (latest
   facts, today's behaviour). `final` is labelled as an oracle in its output.

### 3.2 Layer 1: fact tables (Postgres, written by NestJS)

**`schedule_day_versions`**: the park-level day as published, one row per version.

| Column | Type | Notes |
| --- | --- | --- |
| `park_id` | uuid | |
| `day` | date | park-local service date |
| `known_from` | timestamptz | when this version was written |
| `known_to` | timestamptz null | null = current. One open version per `(park_id, day)` (partial unique index `WHERE known_to IS NULL`) |
| `state` | text | `OPERATING` / `CLOSED` / `UNKNOWN` / `NONE` (no park-level row: never published or retracted) |
| `open_utc`, `close_utc` | timestamptz null | earliest OPERATING opening, latest closing, the same rule as `fetch_windows` / `fetch_operating_windows` |
| `n_windows` | smallint | |
| `ticketed_event`, `private_event`, `extra_hours` | boolean | from TICKETED_EVENT / PRIVATE_EVENT / EXTRA_HOURS rows of that day |
| `extra_open_utc`, `extra_close_utc` | timestamptz null | |
| `source` | text | `themeparks` / `wartezeiten` / `live_fallback` / `gap_fill` / `silent_guard` / `curated` / `backfill` |
| `asof_exact` | boolean | false for the backfilled first version (§3.4) |

PK `(park_id, day, known_from)`. Writer: one `ScheduleVersionLog.record(parkId,
days)` called **after** every writer in §2.1 has finished with a park (so the end
state after `saveScheduleData` + `fillScheduleGaps` is logged, not the interim).
It derives the day state from `schedule_entries`, compares it with the open
version, and writes only on change. `schedule_entries` stays as it is: it is the
serving table, and the log is the record of how it changed.

**`holidays`**: existing table, three columns added.

| Column | Notes |
| --- | --- |
| `first_seen_at` timestamptz | backfill = `createdAt` |
| `withdrawn_at` timestamptz null | set when a source answer for that (country, year, type) **with entries** no longer contains the row. This is the schedule-retraction rule (only inside answered coverage). Today such rows stay true forever |
| `source` text | `nager` / `nager_long_weekend` / `openholidays` / `synth_summer`. A projection is a fact about our projection, and readers can exclude it |

**`holiday_coverage_versions`**: how far each source has published, per type.

| Column | Notes |
| --- | --- |
| `country`, `region` (`''` = national) | |
| `holiday_type` | `public` / `school` |
| `covered_through` date | last date the source has answered for (#557 derives it as the last stored date; the sync knows it better, because it asked) |
| `known_from`, `known_to` | as above |

This makes #557's coverage as-of: a 2025 training row sees the coverage that held
in 2025, not today's.

**`weather_forecast_issues`**: what `weather-full-cron` already fetches, kept.

| Column | Notes |
| --- | --- |
| `park_id` uuid, `target_date` date (park-local), `issued_at` timestamptz | PK `(park_id, target_date, issued_at)` |
| `lead_days` smallint | `target_date` − park-local date of `issued_at` |
| `temp_max`, `temp_min`, `precip_sum`, `rain_sum`, `snow_sum`, `wind_max` real; `weather_code` smallint | the daily fields `weather_data` holds |

Written in the same loop that upserts `weather_data`, from the same response, with
no extra Open-Meteo call ([sequential upstream is intentional](weather.md#open-meteo-limits)).
Observed weather stays in `weather_data` (`dataType = 'historical'`). Hourly
forecasts are not archived in step one (decision D5).

**Events**: the only event source is `schedule_entries` (TICKETED_EVENT,
PRIVATE_EVENT, EXTRA_HOURS), so events are columns of `schedule_day_versions`.
No events table until a second source exists.

**Park geography** (`timezone`, `countryCode`, `regionCode`, `influencingRegions`,
latitude) is read current and treated as timeless. A correction to
`influencingRegions` is meant to apply to the past (the #98 fix did exactly that).
This is stated in the output, not versioned.

### 3.3 Layer 2: one derivation

A Python package `covariates/` (decision D1 covers where it lives) with three
entry points. They replace `holiday_features.py`, `holiday_utils.py`,
nf-service's `add_calendar_covariates` holiday/weather/schedule part,
`chronos_shadow.day_covariates`, `fetch_windows` / `fetch_operating_windows`, and
`add_park_schedule_features`' SQL:

```python
park_day_covariates(conn, park_ids, d0, d1, *, origin: datetime | None, final: bool = False) -> DataFrame
park_hour_covariates(conn, park_ids, t0_utc, t1_utc, *, origin, final=False) -> DataFrame   # open/close per hour, from the same windows
schedule_windows(conn, park_id, d0, d1, *, origin, final=False) -> DataFrame              # what PCN and Chronos need
```

It reads only Postgres, with the bounded-session settings each service already
uses (`SET LOCAL statement_timeout` / `lock_timeout`). There is no HTTP hop.

**Output contract, one row per `(park_id, local_date)`.** Columns are semantics.
`NULL` means unknown.

| Group | Columns | Unknown when |
| --- | --- | --- |
| holiday, own | `public_holiday`, `school_holiday`, `bridge_day`, `easter_sunday` | the date is past `covered_through` as of origin (per type) |
| holiday, neighbours | `neighbor_public_count`, `neighbor_school_count`, `neighbor_public_1..3` (CatBoost compatibility), uncapped | the neighbour's coverage ends before the date (counted separately: `neighbor_unknown_count`) |
| distances | `days_until_public`, `days_since_public`, `days_until_school`, `days_since_school` (cap 30) | the cap window reaches past coverage without a hit |
| schedule | `sched_state`, `open_utc`, `close_utc`, `open_hours`, `n_windows`, `ticketed_event`, `private_event`, `extra_hours` | no version known at origin (`sched_state = NULL`, distinct from `UNKNOWN` = the park said so / gap-fill could not decide) |
| weather | `temp_max`, `temp_min`, `precip_sum`, `snow_sum`, `wind_max`, `weather_code` | none (climatology is the fallback), but the basis is always stated |
| provenance | `sched_known_from`, `sched_asof_exact`, `school_covered_through`, `public_covered_through`, `weather_basis` (`observed` / `forecast` / `climatology` / `oracle_observed`), `weather_issued_at`, `weather_lead_days`, `origin`, `final` | — |

Calendar encodings (dow one-hot, doy sin/cos, peak season) stay in the services.
They are deterministic, and moving them buys nothing.

### 3.4 As-of semantics

`origin` is a UTC instant. "The origin's local date" is the park-local date of
that instant.

| Covariate | Rule at `origin` | Before the log existed |
| --- | --- | --- |
| Schedule (future and past days) | the version with `known_from ≤ origin < coalesce(known_to, ∞)`. A past day reads what was **published**, not what happened (that is `park_day_operations`, a past-only input) | backfill one version per day with `known_from = max(updatedAt)` of its rows and `asof_exact = false`. This is the conservative proxy ml-bench's drivers already use: it can hide a window that was known, and it never shows one that was not. Days with no row → no version → unknown |
| Public holidays | rows with `first_seen_at ≤ origin` and (`withdrawn_at` null or `> origin`), dates `≤ covered_through` as of origin; `synth_summer` excluded unless asked | `first_seen_at = createdAt`. Public holidays are fixed by law years ahead, so for public holidays reading `final` before the log start is a mild, documented oracle |
| School holidays | the same, per region; past `covered_through` → `NULL` (#557, now as-of) | coverage backfilled from today's rows with `known_from = max(createdAt)` of the rows behind it |
| Bridge day | derived from the public-holiday flags of the neighbouring calendar days, so its as-of follows the public flags (decision D4) | — |
| Weather, target < origin's local date | observed (`weather_data` historical with `updatedAt ≤ origin`), else the latest issue | before the archive: observed, `basis = oracle_observed` |
| Weather, 0 ≤ lead ≤ 15 | latest issue with `issued_at ≤ origin` → `basis = forecast` | no issue → `oracle_observed` **only** with `allow_oracle=True`, else climatology |
| Weather, lead > 15 | climatology per (park, park-local month) from observed rows **before** origin (the PAR-1031 rule); ERA5 climate normals stay walled off ([weather](weather.md#climate-normals-beyond-the-forecast)) | — |
| Events | inside the schedule version | as schedule |
| Geography | current | — |

`final=True` reads the latest facts for every date, which is what every reader
does today. Parity tests run in this mode, and ml-bench labels such runs as oracle.

### 3.5 How each service reads "as of origin"

| Service | Origin | Step-one mode |
| --- | --- | --- |
| CatBoost serving (`predict.py`) | request time | `origin = now`. The daily weather fields come from the latest issue, the same values the hourly fetch reduces to (PAR-1032). Hourly temperature stays on the live hourly forecast until D5 |
| CatBoost training (`features.py` via `db.fetch_training_data`) | the row's own day | `final=True` (today's behaviour, the parity target). As-of training (the row's covariates at the lead it will be served at) is a model change, decision D2 |
| TFT training (`train_runner`, 03:00 UTC) | the training night | history panel `final=True`; future frame `origin = now` |
| TFT serving / `build_future_frame` | the forecast night | `origin = now` |
| Chronos-2 shadow (`chronos_data.run`) | capture origin | `origin = origin`. It already says "read as of the origin" and is the first reader whose claim becomes true |
| PCN (`fetch_operating_windows`) | forecast tick | `schedule_windows(origin = tick)` |
| ml-bench | each rolling origin | exports the fact tables (versions, issues, coverage) instead of final tables; `build` calls the package per origin. `published_final` / `ORACLE` remain only for dates before the logs start |

### 3.6 Size and cost

Assumptions are marked. Each one is replaced by the measurement given before the
PR that depends on it ships. ~150 parks have coordinates ([weather](weather.md)).

| Table | Rows | Bytes/row (heap + PK) | Per year |
| --- | --- | --- | --- |
| `weather_forecast_issues` | 150 parks × 16 days × 2 issues/day = **4,800/day**, 1.75 M/yr (arithmetic, not an assumption) | 7 × real + smallint + keys + 24 B header ≈ 80 B heap; PK (16 + 4 + 8 B) ≈ 50 B index | **≈ 230 MB/yr** |
| `schedule_day_versions` | backfill ≈ 150 × ~550 days ≈ 83 k; churn **assumed ≤ 5 % of 365 future days per park per day** ≈ 2.7 k/day ≈ 1 M/yr | ≈ 110 B heap, ≈ 50 B PK | **≈ 160 MB/yr** at the assumed churn. Measure first: `SELECT date_trunc('day', "updatedAt"), count(*) FROM schedule_entries WHERE "attractionId" IS NULL GROUP BY 1` (a lower bound: deletions do not show) |
| `holiday_coverage_versions` | ~40 countries × a few hundred regions × a handful of changes/yr | — | < 1 MB |
| `holidays` + 3 columns | existing rows | 2 × 8 B + short text | negligible |

The read side gets cheaper. The package evaluates per `(park, date)`, ~150 × 548
= 82 k rows for a full training window, and the services join that onto their
rows. Most of today's readers already work this way (`assign_holiday_features`),
but nf-service's per-park `isin` loop and ml-service's 1-hour holiday cache fall
away. Plain tables, no hypertable and no compression policy: below a gigabyte
they are not worth a TimescaleDB job queueing for an ACCESS EXCLUSIVE lock
([DB health §0b](../troubleshooting/db-health-runbook.md)).

---

## 4. Migration, one small PR at a time

Order rule: **start the clocks first.** A day that is not logged cannot be
recovered later, the same reason the [forward archive](../ml/forward-archive.md)
shipped before anything scored it. Every reader PR after that changes one
service, in `final` mode first, with a parity test that fails if the output
moves.

| # | PR | Changes behaviour? | Parity / test |
| --- | --- | --- | --- |
| 1 | **`weather_forecast_issues`**, written by `weather-full-cron` from the response it already has. Nobody reads it | no | unit: one issue per park per run, `lead_days` from the park-local date (a park west of UTC at 00:00 UTC), no extra HTTP call. E2E schema list reads the DDL from the entity |
| 2 | **`schedule_day_versions`** + `ScheduleVersionLog.record`, called after save + gap-fill + guards; one-off backfill with `asof_exact = false` | no | unit: unchanged → no row; change → close + insert; retraction → `NONE` version; gap-fill promotion → new version; one open version per day (constraint). Backfill count = distinct park-level days |
| 3 | **holiday facts**: `first_seen_at`, `withdrawn_at`, `source`, `holiday_coverage_versions` written by the holiday sync | no (nothing reads them) | unit: withdrawn only inside an answered (country, year, type); a moved holiday = one withdrawn + one new row |
| 4 | **`covariates/` package, `final` mode**, holidays only: `holiday_features.py` + `holiday_utils.py` move into it, and ml-service, nf Chronos and ml-bench import it (vendored per D1). nf TFT untouched | no | golden test: for every park × 548 days, `assign_holiday_features` before == after (`assert_frame_equal`, dtype-exact), including #557's coverage NaNs. Vendored copies byte-identical (the existing test, generalised) |
| 5 | **schedule reader**: `schedule_windows` replaces `fetch_windows`, `fetch_operating_windows` and the SQL in `fetch_park_schedules`, read from `schedule_entries` in `final` mode | no | windows identical for all parks × 90 days (open/close to the second); `is_park_open` per CatBoost training row identical |
| 6 | **origin mode** in the package; Chronos switches to `origin = origin` (the shadow, so no served number moves) | shadow only | the as-of read at `origin = now` equals `final` for all future dates (both read the open version); a fixture with two versions returns the older one for an origin between them |
| 7 | **ml-bench** exports versions / issues / coverage and builds per origin | offline only | rerun of one published bench cell in `final` mode reproduces its number; the same cell as-of is reported beside it, which is the size of the oracle we have been grading with |
| 8 | **weather reader**: CatBoost serving daily fields and the TFT future frame from the latest issue | no (same values) | on deploy day: latest issue == the `weather_data` forecast row for every park × lead ≤ 15 (they come from one response) |
| 9 | **TFT semantics** onto the package (bridge, neighbour, Easter, coverage), so `is_holiday_primary` means one thing | **yes: a model change** | through the `tft_grid` shadow (#554) under the model switch rule (offline full year + live ≥ 14 d / ≥ 10 parks, owner decides). The diff of every renamed column is counted and listed in the PR |
| 10 | **as-of training** (D2): CatBoost / TFT train on covariates at the lead they are served at | **yes** | shadow, once the issue archive is ≥ 60 days old (lead d60) |
| 11 | **delete** the old paths: nf `add_calendar_covariates` holiday/weather/schedule code, ml-service holiday/schedule caches, `MLService.enrichForecastWithHolidays` (read by nobody, 3 queries per forecast day) | no | grep proves no caller; contract test that `/predict` ignores the removed fields |

PRs 1–3 are independent of each other and of everything else. PR 4 can start in
parallel. PRs 9 and 10 are the only ones that move a served number.

---

## 5. What not to do

- **Do not materialise a wide feature table per origin.** 150 parks × 366 future
  days × 365 origins ≈ 20 M rows/yr of mostly identical values. Store facts with
  versions and derive on read.
- **Do not log the interim states.** `saveScheduleData` deletes and re-inserts
  inside one park's sync, and gap-fill then rewrites. Record after the last
  writer, or the log fills with versions no reader ever saw.
- **Do not delete from a fact log or update it in place.** A retraction and a
  correction are both new versions, otherwise the log becomes a second
  `schedule_entries`.
- **Do not call a backfill exact.** `updatedAt` is the last change, not the first
  publication. Backfilled versions carry `asof_exact = false`, and backtests over
  that period are labelled.
- **Do not encode unknown as 0 in the source.** The package returns `NULL`, and
  turning it into NaN, a `*_known` flag or a mask is each service's encoding
  ([absent facts](../rules/absent-facts.md), #557).
- **Do not change a model's inputs inside a refactor PR.** Parity PRs have to
  reproduce today's frames exactly. A semantic change (TFT's bridge and neighbour
  definitions) goes through a shadow.
- **Do not add a covariate microservice.** Another HTTP hop and failure mode in
  front of the 03:00 / 06:00 trainings buys nothing over a package that reads
  Postgres.
- **Do not reimplement the derivation in SQL or TypeScript as well.** NestJS's
  calendar keeps its own UI reading (`schoolCoverageThrough`, #557), but it gets
  a shared fixture test against the package so the two cannot drift silently.
- **Do not fetch Open-Meteo more often to fill the archive.** Archive the 12-hour
  run we already make. The limits are real, and climate normals stay out of
  model inputs.
- **Do not version park geography.** `influencingRegions` corrections are meant to
  rewrite the past.
- **Do not run the backfills or size checks by hand in `psql`.** Use
  `scripts/prod-psql.sh` ([DB health §0](../troubleshooting/db-health-runbook.md)).

---

## 6. Owner decisions

| # | Question | Options | Recommendation |
| --- | --- | --- | --- |
| D1 | Where does the derivation live? | (a) `covariates/` package, vendored into each service's build context, byte-identical test in CI. This is how `holiday_features.py` already travels, and Compose builds each service from its own directory. (b) Build each service from the repo root. (c) SQL functions in Postgres. (d) NestJS materialises a table | **(a)**: no build or deploy change. (b) is cleaner and could follow later. (c) re-creates the duplication in a second language |
| D2 | What do the trainings learn from? | (a) `final` facts (today). (b) As-of at the served lead | **(a) now, (b) as PR 10** behind a shadow once the archive covers d60. Training on observed weather and serving forecasts is the largest remaining train/serve gap, but closing it changes the model |
| D3 | Weather past 16 days | (a) climatology from our observed rows before the origin (CatBoost today). (b) ERA5 normals | **(a)**: normals are walled off from the model on purpose |
| D4 | One bridge-day definition | (a) weekday next to a public holiday (ml-service heuristic). (b) Mon before Tue / Fri after Thu (Chronos). (c) Nager long-weekend rows (TFT). (d) `schedule_entries.isBridgeDay` | **(b)**. It is derived from the public flags, so its as-of and coverage come free, and it is the narrowest claim. CatBoost moves to it only through a retrain |
| D5 | Archive hourly weather? | (a) no. (b) d0–d2 hourly from the existing hourly fetch (~150 × 72 × 2 ≈ 22 k rows/day, ≈ 0.6 GB/yr) | **(a)** until an intraday model shows it needs it. Chronos measured weather at < 0.12 min |
| D6 | Retention of `weather_forecast_issues` | (a) none. (b) 2 years | **(a) for now**, revisit at 1 GB. A full seasonal backtest needs more than a year of issues |

---

## Related

- School-holiday coverage (`docs/architecture/school-holiday-coverage.md`, added by #557, PAR-1049): the coverage rule this design makes as-of.
- [Forward archive](../ml/forward-archive.md): the same "start the clock first" pattern for served curves.
- [What explains the daily level](../ml/level-drivers.md): why schedules are as-of `updated_us` and weather is `ORACLE` today.
- [Schedule sync](schedule-sync-and-calendar.md), [Weather](weather.md), [Date & time §8](../development/datetime-handling.md).
