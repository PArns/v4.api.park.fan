# TFT grid shadows (PAR-1033, PAR-1041, PAR-1057)

The two TFT variants nf-service trains every night after the served run and never serves: the
calendar-day grid, and the grid with the PAR-830 level drivers and a time-weighted target. Open it
before switching `NF_CALENDAR_GRID` or reading `tft_grid` / `tft_grid_drivers` in the forward
archive. The served TFT is on [TFT](tft.md).

## The calendar-day grid (`NF_CALENDAR_GRID`, default off)

The served panel holds operating days only, and NeuralForecast reads rows by position, so with
`freq="D"` `input_size=90` meant the last 90 *operating* days and step k the k-th operating day
after the last one. For a park with rest days or a closed season the spacing in training and
serving was not calendar days, and `build_future_frame` started every series at its own
`last ds + 1` — a ride last seen 20 days ago got a frame that began in the past and reached only
40 days ahead. The grid changes the training data's shape, so it runs as a shadow before it
serves.

- `to_calendar_grid`: every day from a series' first observation to park-local yesterday; absent
  days are `y = 0` with `available_mask = 0` (NeuralForecast 3.1.8 keeps them out of the scaler,
  the loss and the window sampler). `y` is 0 rather than NaN because TFT's forward pass never reads
  the mask — it still reads those zeros as context — so the grid adds `is_observed` (1 / 0) as a
  past covariate: the encoder can tell a closed day from a quiet one even past the schedule's
  horizon.
- Every series ends on park-local yesterday, so every future frame starts on park-local today (the
  day the reader serves first).
- A series last observed more than `NF_MAX_STALE_DAYS` (90 = `NF_INPUT_SIZE`) days ago is not
  forecast — its whole context would be masked. Its last forecast serves up to 3 more days, then
  CatBoost.
- Five more future covariates (`FUTR_EXOG_GRID`): `sched_open` / `sched_closed` from the published
  park schedule (both 0 = unknown, never closed), `days_until_school_holiday`,
  `days_since_school_holiday` and `school_holiday_known` (PAR-1049).

**Known limit of the schedule flags:** in history almost every day of a scheduled park has an
entry; in the future only the operator's publishing horizon does. Beyond it both flags are 0, a
state the model rarely saw in training. Measure before trusting the far leads.

## The grid as a nightly shadow (PAR-1041)

`tft_forecasts` cannot hold two variants side by side (its key has no model version), so the
grid writes to its own table.

- **nf-service.** With `NF_GRID_SHADOW` (default `true`) `train_runner.py` trains the grid variant
  after the served run (`run_grid_shadow`): grid forced on through `forecast.calendar_grid(True)`
  whatever `NF_CALENDAR_GRID` says, model version `<served>-grid`, written under the served run's
  `forecast_date`. Skipped when `NF_CALENDAR_GRID` is on (the served run is the grid then).
- **Sequencing.** Same process, after the served forecast is persisted, never concurrently. The
  `is_training` lock stays on (`phase: "grid_shadow"` in the status), so no second `/train` and no
  Chronos-2 call shares the GPU, and the `train-nf` job waits for it. A shadow failure (OOM
  included) is reported in `info.shadow.error` and logged; it never fails the night.
- **Cost.** One more fit, which reads more rows per series (closed days are masked rows now). Peak
  memory of one fit is unchanged — same batch sizes and chunks, and the fits never overlap.
  Kill switch: `NF_GRID_SHADOW=false`.
- **Table.** `CREATE TABLE IF NOT EXISTS tft_forecasts_shadow (LIKE tft_forecasts INCLUDING ALL)`
  in `nf-service/db.py` — same columns and key, cannot drift. Plain table, no FK, 120-day retention
  with `tft_forecasts` (PAR-1054). Registered in the merge (`ATTRACTION_DEPENDENCIES`) and the E2E
  schema.
- **Comparison.** The forward archive reads it at scoring time as model `tft_grid` on the calendar
  questions `cal_rank`, `cal_busier` and `cal_far` —
  [model comparison: the shadow levels](forward-archive/model-comparison.md#the-shadow-levels).

## The PAR-830 drivers in the grid shadow (PAR-1057)

A **third** nightly run, its own model: after the served run and the grid-only shadow,
`run_grid_shadow(..., drivers=True)` trains into `tft_forecasts_shadow_drivers`
(`LIKE tft_forecasts`, model version `<served>-grid-drivers`), paired in the archive as
`tft_grid_drivers` beside `tft_grid`. It adds the drivers
[level-drivers.md §5](level-drivers.md#5-ranked-covariates-worth-adding-to-tft--catboost) ranks
highest **and** a different target, so the grid-only shadow keeps running and both are read
separately against the served TFT — one comparison would otherwise mix three changes.

`NF_SHADOW_DRIVERS` (default `true`) switches only this run, `NF_GRID_SHADOW` only the grid-only
one; it still runs when `NF_CALENDAR_GRID` serves the grid. `forecast.calendar_grid(True,
drivers=True)` is the only way in — `db.DRIVERS` is not an env setting, and `futr_exog()` returns
`DRIVER_EXOG` only with the grid and the flag. Cost: the night's `train-nf` job ~40–50 min instead
of ~25, inside the 90-min deadline.

| future covariate | meaning | unknown |
|---|---|---|
| `hours_ratio` | opening hours ÷ their mean over the last four same weekdays (d−7 … d−28) with a published window | 1.0 |
| `close_vs_ref` | closing hour − those days' mean closing hour (past midnight counts past 24) — the late close | 0.0 |
| `closed_days_before` | days without a published window since the previous one, cap 14; > 0 = first operating day after a closure. Only a gap between two published windows counts | 0 |
| `sched_known` | the day's OPERATING window was published as of the origin | — (flag) |
| `school_break_first_day` / `_last_day` / `_day_after` | change of the own region's school-break state (d−1 → d, d → d+1) | 0, with `school_holiday_known` |

All are NaN-free, as NeuralForecast requires of a future covariate: an unknown value is its
neutral value plus a flag. Built in `db.schedule_drivers` and `add_calendar_covariates`.

**As of the origin — what is possible.** `schedule_entries` keeps only the latest version of a
day, with `updatedAt` the one hint of when it became known. A future covariate has one value per
day, shared by every training window whatever its lead, so ml-bench's per-origin as-of masking
cannot be expressed. What is built: `sched_asof` = the window's last write came before the day
began (for today and later: before now). In the future frame that is exactly the schedule as
published at the forecast origin; in the history it is "published by the day before", a lead-1
as-of for every training day, and a re-sync that rewrote an unchanged row reads as unknown
(conservative). At long leads the history therefore knows more windows than the forecast does
(84 % known at d1, 59 % at d60 — level-drivers §4); `sched_known` lets the model tell the two
apart.

**Target.** The shadow trains on the **time-weighted** daily P90 (`db.time_weighted_p90`): each
quarter-hour of the published OPERATING window (cut at midnight; without one, the span of the
park's own OPERATING readings) takes the last reading before its end, of any status, held for at
most 3 h; slots that are OPERATING with a wait ≥ `NF_MIN_WAIT` enter the P90 (≥ 3 slots per
ride-day) — the rule of `rideQ90` in `src/parks/services/forecast-archive.service.ts`. The served
target stays the P90 over change-log rows, which weighs a jittery busy hour above a calm one.
Because the archive's truth is built from 15-min slots too, part of any `cal_busier` / `cal_far`
gain can be target alignment rather than the drivers; `cal_rank` is scale-free. Separating the two
needs a second shadow — an owner decision, raised on the PAR-1057 pull request.

**Memory.** The target is read as raw change-log rows (`fetch_time_weighted_panel`, every status,
~3 M rows per 10-park chunk over 730 days) and expanded to a 15-min grid (roughly rides × days ×
40 rows per chunk), so ids and status are categoricals and the groupbys pass `observed=True`.
Each chunk logs `time-weighted panel: <parks> parks, <readings> readings -> <ride-days> ride-days,
RSS <GB>`; expected ~1–3 GB peak per chunk in the 14 GB container.

## Deciding

Compare `cal_rank` / `cal_busier` / `cal_far` and `leadTimeMae` for `tft_grid` and
`tft_grid_drivers` against the served `tft`, overall, for parks with weekly rest days and for the
Asian parks (Tokyo, Shanghai), where partial-today and the spacing hit hardest. Switching
`NF_CALENDAR_GRID` for a fortnight and comparing with the fortnight before is cheaper but
confounded by the season. `backtest_horizon.py` feeds the realised weather into the future frame
(oracle), so its MAE is optimistic against serving.
