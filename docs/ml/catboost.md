# CatBoost (ml-service)

The CatBoost wait-time model in `ml-service/`: what it predicts, its 75 features, the
`/predict` contract the API calls, and the rules that keep training and serving computing the
same feature. How it is trained, gated and activated is on [CatBoost training](catboost-training.md);
which of its quantiles reaches a reader is on [Quantile serving](quantile-serving-and-calibration.md).

## What it predicts

- **One global model** over all parks and rides (`parkId`, `attractionId` and `weatherCode` are
  categorical), CatBoost with `MultiQuantile:alpha=0.5,0.8,0.95`, depth 7, 254 borders,
  `l2_leaf_reg` 3, `rsm` 0.8, `min_data_in_leaf` 20, learning rate 0.1, 1500 trees
  (`ml-service/config.py`). Trained on CPU.
- **Target:** the hourly median of a ride's OPERATING STANDBY readings ≥ 5 min. Serving predicts
  15-minute slots with the same model.
- **Hourly:** 48 hours of 15-minute slots (`HOURLY_PREDICTIONS` = 48, 192 slots; 24 until
  2026-09-23), regenerated for every park every 15 minutes by `predictions/generate-hourly`.
  Three places in the API break silently if this number grows past them: the dedup window in
  `MLService.deduplicatePredictions` (48 h), the cutoff of the `cleanup-old` job, and
  `PlanDayService.HOURLY_HORIZON_DAYS`.
- **Daily:** 365 days (`DAILY_PREDICTIONS`), written once a night by `generate-daily` (01:00 UTC).
  Each day is the **maximum** of the predictions at park-local 12:00, 14:00 and 16:00
  (`DAILY_PEAK_HOURS`), a proxy for the day's peak — one 14:00 value under-read the peak by
  13–45 min on busy headliner days. Days the schedule marks CLOSED get no row, so the reach is
  the operator's published calendar ([long-range-forecasting.md §1](long-range-forecasting.md)).
- **Output:** q0.5 is the shown wait, rounded half-up to 5 minutes and floored at 5 when
  positive (10 until PAR-1027); q0.8 drives the crowd level; q0.95 − q0.5 is the band.
  The crowd level is `crowd_level_for`: the q0.8 wait ÷ the park's typical-day-peak baseline,
  the P50 baseline when that is missing, `unknown` (baseline 0) when both are.

What CatBoost serves today: days 61–365 of the daily level, the hourly slots PCN does not cover
and every slot when `SERVE_PCN_INTRADAY` is off ([model overview](model-overview.md)). The
dropouts described on the training page exist because of the far-daily rows; they cannot go
while CatBoost is the only model past day 60.

## The `/predict` contract

The API calls `POST /predict` (bulk) from `MLService.buildRawParkPredictions`; the body is
`PredictionRequestDto` / pydantic `PredictionRequest`:

| Field | Meaning |
|---|---|
| `attractionIds`, `parkIds` | parallel lists, one park id per ride |
| `predictionType` | `hourly` or `daily` |
| `baseTime` | ISO instant, default now |
| `weatherForecast` | hourly Open-Meteo rows for the park |
| `currentWaitTimes`, `recentWaitTimes` | live wait and the one ~30 min earlier, per ride |
| `featureContext` | `parkOccupancy`, `parkOpeningTimes`, `downtimeCache`, `queueData`, `parkHasSchedule`, `isSchoolHoliday`, `parkLiveStatus` (`src/common/types/feature-context.type.ts`) |
| `typicalDayPeakBaseline`, `p50Baseline` | the park's crowd baselines; the first rates the crowd level, the second is its fallback |

The answer is `BulkPredictionResponse` (`predictions`, `count`, `modelVersion`). Each
`PredictionResponse` carries `predictedWaitTime`, `confidence`, `crowdLevel`, `baseline`,
`uncertaintyMinutes`, and the raw `predictedQ80` / `predictedQ95` that the API stores for
coverage checks and strips from every served read. A field `predict.py` sets but
`PredictionResponse` does not declare is dropped by pydantic.

Other endpoints: `GET /predict/park/{park_id}`, `GET /model/info` (the model the answering
worker has loaded), `GET /model/info/{version}` (that version's own metadata — the one the
training processor reads), `POST /model/reload`, `POST /train`, `GET /train/status`,
`GET /models/files`, `DELETE /models/files/{version}`, `POST /driver-level/run`
([level-drivers.md §8](level-drivers.md#8-the-production-shadow-par-1058)).

On the API side the Redis-cached answer is `MLService.getRawParkPredictions` (CatBoost-pure);
what readers get goes through the overrides on [Intraday serving](intraday-serving.md).

## Features

`features.get_feature_columns()` lists 75; `predict.create_prediction_features` builds the
same columns for serving.

| Group | Features |
|---|---|
| ids | `parkId`, `attractionId` (categorical) |
| time (14) | `hour`, `day_of_week`, `month`, sin/cos of hour, weekday, month and day of year, `season`, `is_weekend` (region-aware: Fri/Sat in the Middle East), `is_peak_season` |
| weather (12) | `temperature_avg`, `temperature_deviation` (vs the monthly mean), `precipitation`, `precipitation_last_3h`, `windSpeedMax`, `snowfallSum`, `weatherCode` (categorical), `is_raining`, `is_rain_starting`, `is_rain_stopping`, `is_temp_extreme`, `is_wind_extreme` (> P90 of the last 14 days) |
| holidays (13) | `is_holiday_primary`, `is_school_holiday_primary`, `is_holiday_neighbor_1/2/3`, `holiday_count_total`, `school_holiday_count_total`, `neighbor_school_holiday_count`, `is_school_holiday_any`, `is_bridge_day`, `days_until_next_holiday`, `days_since_last_holiday`, `is_long_weekend` |
| schedule (5) | `is_park_open`, `time_since_park_open_mins`, `park_has_schedule`, `has_special_event` (ticketed or private event), `has_extra_hours` |
| ride (6) | `park_attraction_count`, `is_coaster`, `is_water_ride`, `is_indoor`, `is_wind_sensitive`, `has_virtual_queue` |
| history (15) | `avg_wait_last_1h`, `avg_wait_last_24h`, `avg_wait_same_hour_last_week`, `avg_wait_same_hour_last_month`, `avg_wait_same_dow_4w`, `rolling_avg_7d/_weekday/_weekend/_28d/_90d`, `wait_time_velocity`, `trend_7d`, `volatility_7d/_weekday/_weekend` |
| live context (3) | `park_occupancy_pct`, `had_downtime_today`, `downtime_minutes_today` |
| interactions (5) | `hour_weekend_interaction`, `hour_is_weekend`, `temp_precip_interaction`, `holiday_occupancy_interaction`, `hour_occupancy_interaction` |

Definitions that are not obvious from the name:

- **`trend_7d`** = `avg_wait_last_24h − rolling_avg_7d` (momentum), not a regression slope.
- **`volatility_*`** = `log1p(std)` of the window, capped at `log1p(VOLATILITY_CAP_STD_MINUTES)` =
  `log1p(30)`. The weekday/weekend split exists because a mixed 7-day std told the model a ride
  was volatile but not when (2026-03); at a cap of 15 the signal vanished, so it is back at 30.
- **`avg_wait_same_dow_4w`**: the mean of the same weekday and hour 1–4 weeks back. More than
  28 days ahead none of those lags exists yet — see the parity table below.
- **`is_long_weekend`, `days_until_next_holiday`, `days_since_last_holiday`**: a bridge day is
  part of a 3–4-day block and often busier than the holiday itself; arrivals climb before a long
  weekend and the last day empties early. The distances are clipped and default to 7.
- **`is_rain_starting` / `is_rain_stopping`**: guests react to the change (indoor rides fill when
  rain starts) more than to the amount.
- **Weather units (PAR-1031, PAR-1032).** Training reads `weather_data`, one row per park-local
  day, so the model knows daily precipitation and snowfall sums, the daily wind maximum and the
  daily weather code, plus a temperature on a sinusoid between the day's min and max (trough
  02:00, peak 14:00 local). Serving reduces the hourly forecast to the same daily values per
  park-local day (`predict.apply_weather_features`; a day needs ≥ 12 forecast hours,
  `MIN_FORECAST_HOURS_PER_DAY`, and sums are scaled to 24 h) and keeps the hourly temperature.
  Every row past the forecast reads the climatology of its own park-local month
  (`fetch_weather_climatology`, 3-year averages) — never 20 °C / 0 mm / code 0. Rain timing
  within a day is not a feature: there is no hourly observed weather to learn it from.
- **Downtime:** training counts DOWN readings over the earlier hours of the local day
  (`downtime_count_before`), so the model can learn the queue spike after a reopening.

### One definition per feature on both paths (PAR-1030, PAR-1032, PAR-1039, PAR-1045, PAR-1052)

Training (`features.py`) is the definition; serving (`predict.create_prediction_features`)
reproduces it. `tests/test_train_serve_feature_parity.py` computes each of these through both
paths on one fixture.

| Feature | Definition (both paths) | Serving before |
|---|---|---|
| `avg_wait_last_1h` | mean of [t − 1 h, t). Serving: the live wait on rows ≤ `LIVE_LAG_MAX_LEAD` (3 h) after base_time, the same-hour-last-week profile beyond | live wait on every row, 48 h ahead too |
| `wait_time_velocity` | (x[t−1h] − x[t−7h]) / 6 on the hourly series; serving takes the live wait as x[t−1h], ≤ 3 h, 0 beyond | (now − 30 min ago) / 6, a twelfth of the scale, on every row |
| `time_since_park_open_mins` | minutes since the opening of the window the row is in (its own day, or the previous day's for a slot past midnight), 0 without a window | today's opening for every row, 0 when unknown |
| `is_bridge_day` | per park-local date from the holidays (`add_bridge_day_feature`), computed before the holiday-distance features | one flag for today (`featureContext.isBridgeDay`, no longer sent) |
| `rolling_avg_weekday/_weekend`, `volatility_weekday/_weekend` | Mon–Fri vs Sat–Sun on the **park-local** weekday | UTC weekday, on both paths |
| `park_occupancy_pct` (training) | the park's mean wait at that hour **without the row's own wait** (the target); a ride alone at its hour reads 100 | the mean included the target |
| interaction features | recomputed after training dropout | computed before it, so a dropped-out row kept the real occupancy inside them |
| `is_park_open` (PAR-1045) | 0 before opening and after close on a day with a published window, 1 inside it | stayed 1 outside the window (only `status` turned CLOSED) |
| `is_park_open` (PAR-1052) | 1 on a day without a published window (training admits such a day only when rides were measured running); training sets 1 from a fresh after-hours reading, never from the label | training read the target (`waitTime >= 10`) |
| `avg_wait_same_dow_4w` > 28 d ahead (PAR-1045) | the same weekday and hour 52 weeks before the row (`LAST_YEAR_LAG` = 364 d), only for a ride first seen ≥ 330 days before base_time (`LAST_YEAR_MIN_OBSERVED_DAYS`, the `MIN_OBSERVED_DAYS` rule) and only where that slot holds a reading; otherwise `rolling_avg_7d`. Training rows always have their real lags; the origin hold-out's 61-day lead uses the same rule | `rolling_avg_7d` on every such row |

Two more parity rules, each from a bug:

- **Defaults must match.** A fallback value for a missing feature is the same on both paths
  (`avg_wait_last_1h` once defaulted to 30 at serving and 0 in training).
- **Rolling statistics come from the same windows.** Serving's `fetch_recent_rolling_stats`
  mirrors `features.add_historical_features` (time-based, left-closed windows over one median
  per hour). Its predecessor read 730 days and took `.iloc[-1]` of a descending sort — the
  oldest row — so every served `rolling_avg_*` was the value from the start of the window
  (Taron, 2026-10-01: 54.75 served against 28.8 real). See
  [ml-service query performance](performance-optimizations.md).

`queue_data` heartbeat rows (`is_heartbeat`) are excluded from every training, heuristic,
occupancy-profile and serving-context read: a heartbeat copies the previous status and wait
with a fresh timestamp and never ends a status.

### Ride-type flags (PAR-1045)

`is_coaster`, `is_water_ride`, `is_indoor` and `is_wind_sensitive` come from name regexes
(*Splash*, *River*, *Flume* → water; *Tower*, *Sky*, *Loop* → wind-sensitive). With
`CATBOOST_CURATED_RIDE_TYPE` on (default off),
the curated columns overrule the regex **where they are set**, and null (nobody judged the ride)
keeps the regex: `indoor_outdoor = indoor` → `is_indoor` 1, `outdoor` / `covered_queue` → 0;
`attraction_kind` `TRANSPORT` / `SHOW` / `WALKTHROUGH` / `MAZE` → `is_coaster` and
`is_water_ride` 0, `RIDE` keeps the name's flags; `is_wind_sensitive` stays the regex. One
function on both paths (`features.add_attraction_type_features`). The flag is read by
**training only** and stored as `curated_ride_type` in the model metadata; serving follows the
active model's value, so flipping the env never feeds a regex-trained model curated inputs.

Every training logs `Curated ride type coverage … attraction_kind n/N, indoor_outdoor n/N`.
**When to flip:** once that line shows the curated columns cover the headliners (the rides that
carry the busy MAE), set `CATBOOST_CURATED_RIDE_TYPE=true` on the ml-service; the next training
takes it in and the champion gate decides as for any retrain. Coverage ad hoc, through
`scripts/prod-psql.sh`:

```sql
SELECT count(*) AS rides,
       count(attraction_kind) AS kind_set,
       count(indoor_outdoor) AS indoor_outdoor_set
FROM attractions a
WHERE EXISTS (SELECT 1 FROM wait_time_predictions w
              WHERE w."attractionId" = a.id AND w."createdAt" > now() - interval '2 days');
```

`attractions.attractionType` is not a feature: it was empty for every ride when it was tried
(2026-03-29) and carried zero variance.

## Occupancy and baselines

The API runs two crowd-level regimes — live is ratio-vs-P50, the calendar is typical-day-peak
([Unified crowd levels](../rules/crowd-levels.md)). The model's inputs follow them:

- **`park_occupancy_pct` at serving** comes from `AnalyticsService.getCurrentOccupancy` (current
  recent peak ÷ the park's P50 headliner baseline, 0–200 %), sent as
  `featureContext.parkOccupancy`. It is a different function from either crowd level on
  purpose. Training builds it from the park's own hourly means (parity table above).
- **Future rows** read a (park, weekday, hour) occupancy profile from
  `fetch_historical_park_occupancy`. It groups on **park-local** weekday and hour
  (`EXTRACT(... FROM qd.timestamp AT TIME ZONE p.timezone)`); a UTC grouping once shifted every
  non-UTC park's profile by 1–2 hours against a lookup that was local. `queue_data` has no
  `parkId`: join `attractions` and `parks` for the timezone.
- **Crowd level on a prediction:** `typicalDayPeakBaseline` first, `p50Baseline` as the
  fallback, `unknown` without either (PAR-943; it once fell back to the ride's own
  `rolling_avg_7d`, which rates on another scale). `p90Baseline` was removed: Python never read
  it. Same thresholds as the TypeScript `determineCrowdLevel()`: 60 / 89 / 110 / 150 / 200 %.

## Holiday features

- **One implementation:** `holiday_features.assign_holiday_features`, called by training
  (`features.add_holiday_features`) and serving (`create_prediction_features`). Region codes go
  through `holiday_utils.normalize_region_code`, whose alias table (`NRW→NW`, `NDS→NI`,
  `England→ENG`, …) mirrors `src/common/utils/region.util.ts`.
- **Regions:** the park's own region and every influencing region (`influencingRegions`, e.g.
  NL-LI, NL-GE and BE for Phantasialand) — the neighbour flags carry the source markets.
- **Duplicates are flags, not a type (PAR-816):** the `holidays` table holds several rows for one
  (country, region, date) — CH-ZH 2025-04-21 five times. They collapse to two flags combined with
  OR: `public` (any `public`/`bank`/`bridge` row) and `school`; `observance` sets neither. A
  located park reads regional OR national rows; a country-level influencing region
  (`regionCode: null`) matches if any region of that country has the flag.
- **Never assign a merge result back by index label:** a merge onto the holidays table gets
  longer whenever a key is duplicated. The features are evaluated once per (park, local date)
  and written back by position, with a row-count assertion.
- **Past the school-holiday data, school columns are NaN (PAR-1049):** an unpublished break is
  unknown, not 0 — [School-holiday coverage](../architecture/school-holiday-coverage.md).
- **Easter Sunday** counts as a public holiday for parks in `EASTER_COUNTRIES` on both paths
  (Nager.Date lists it only for DE-BB).

## Schedule status and `parkLiveStatus`

How predictions follow `schedule_entries` (OPERATING / CLOSED / UNKNOWN), what
`schedule_filter.py` drops and how the calendar reads the result is owned by
[Calendar, Schedule & ML Rules §3](../architecture/calendar-schedule-and-ml-rules.md#3-ml-services-and-the-schedule). The
current behaviour in short:

- CLOSED rows are not inferred (`predict.py` sends only OPERATING and UNKNOWN rows to the
  model) and `schedule_filter.filter_predictions_by_schedule` returns nothing for a CLOSED
  date; hourly rows on an OPERATING day are cut to the published window.
- Where there is no schedule to go by, `_filter_by_active_rides` keeps a park's predictions only
  while one of its rides has a current wait ≥ 5 min — and all of them when the request carries
  no current waits.
- `featureContext.parkLiveStatus` comes from `ParksService.getBatchParkStatus` →
  `isParkOpen` (`src/common/utils/status-calculator.util.ts`): open inside an OPERATING window;
  a park with an OPERATING row between yesterday and tomorrow is otherwise closed; without one,
  open if any ride's latest reading in the last 30 minutes is OPERATING with a wait > 0.
  `predict.py` uses `OPERATING` only to flip UNKNOWN rows with `is_park_open = 0` to open,
  never CLOSED rows.

## Traps

- **`groupby(key).rolling(...).values` assigned by position needs `df` sorted by
  `[key, "timestamp"]` first.** The result comes back in (key, time) order; on a time-sorted
  frame the values land on the wrong rows. On 2026-03-29 this made `avg_wait_last_1h` correlate
  0.02 with the wait (should be ~0.86). `transform`, `apply` with the group's index, or
  `.loc`-based assignment are safe.
- **Park-local time everywhere:** a pandas column carries one timezone, so park-local time is
  stored naive in `local_timestamp` — a reading, never an instant
  ([Date & time rules §8](../development/datetime-handling.md#8-python-and-pandas-one-column-one-timezone)).
- **The occupancy feature depends on the baseline jobs.** When the `p50-baseline` and
  percentile crons stalled in January 2026, `attraction_p50_baselines` stayed empty for 83 days,
  `park_occupancy_pct` (then ~17 % importance) lost its meaning and busy days were predicted at
  half their wait. The scheduler now removes and re-registers an overdue repeatable job
  (`QueueSchedulerService.hasRepeatableJob`).
- **A uuid filter compares uuid to uuid** — the cast goes on the parameter
  ([uuid casts](../rules/uuid-casts.md)).

## Open

- Rides curated as `SHOW` / `WALKTHROUGH` (`attraction_kind`) and series whose wait is flat even
  at P90 (296 of 2,456 rides had P90 ≤ 5 min, 2026-05-30) are predicted and scored like rides;
  `storePredictions` only drops the Queue-Times exclusion list (`isQueueTimesExcluded`).
- No per-park event calendar: Wintertraum, Halloween and similar seasons are the biggest
  Phantasialand crowd drivers and no feature carries them (`has_special_event` covers only
  ticketed/private schedule entries).
