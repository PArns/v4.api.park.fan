# Reader-facing accuracy figures measure the served model

**Since PAR-854 (2026-10-11).** Three numbers tell a reader how far to trust a
forecast. Each must measure the forecast that reader was actually shown — not
the model that happens to write the table the number used to come from.

| Figure | Surface | Forecast it describes | Scored from |
| --- | --- | --- | --- |
| `leadTimeMae` | `/plan/day` | the **served** daily level: TFT for headliners over days 1–60, CatBoost elsewhere | `prediction_lead_snapshots`, rows with `source` set |
| accuracy badge (`badge`, `last30Days.mae`) | attraction detail | the **served** intraday wait: PCN + §7.7 persistence blend when `SERVE_PCN_INTRADAY=true`, CatBoost otherwise; `served_source = chronos` where Chronos-2 served the scored slot (PAR-911 — rare at the 2-hour scored lead, see forward-archive.md §10.4) | `prediction_accuracy.served_*` → `attraction_accuracy_stats.mae` |
| `forecastError` | ride day curve (`/stats/day`) | **raw CatBoost** — the curve plots `wait_time_predictions` | `prediction_accuracy.absolute_error` → `attraction_accuracy_stats.raw_mae` |

The admin boards (`/v1/ml/accuracy/*`, the dashboard, drift) keep scoring raw
CatBoost on purpose: they compare models, and the PCN-vs-CatBoost board must
not see PCN on both sides. That is out of scope here.

## What was wrong

- **`leadTimeMae`** was the error of CatBoost's raw daily answer. The nightly
  `generate-daily` run snapshotted the response it stores, while the calendar
  and `/plan/day` serve `MLService.getServingDailyPredictions` — TFT over
  CatBoost for d1–60. The figure next to a TFT number was CatBoost's error.
- **The accuracy badge** scored `prediction_accuracy.predicted_wait_time`, which
  is raw CatBoost by design (PAR-817). With `SERVE_PCN_INTRADAY=true` in
  production, the ride page shows PCN + blend; the badge graded something else.

## How each is fixed

### `leadTimeMae`

`generate-daily` still stores raw CatBoost (the TFT-vs-CatBoost board needs it),
then reads `getServingDailyPredictions(parkId)` — a cache hit on the daily
response it just wrote plus one TFT query — and snapshots **that**. Each row
records `source`: `tft` (the row came from `getTftDailyPredictions`,
`modelVersion: "tft"`) or `catboost` (TFT had no forecast, a stale one, or
nf-service was down). If the served read fails nothing is snapshotted: filing
the raw answer under a served label would recreate the mix.

The target day is read with `predictionLocalDate` (moved out of
`MLService.localDateOf`): TFT names the park-local day without an offset, and
`new Date("YYYY-MM-DDT12:00:00")` reads it on the server's clock — a day off for
parks twelve or more hours from UTC.

`getLeadTimeMae` averages rows with `source IS NOT NULL` only. Redis key
`ml:lead-mae:v2:{bucket}`.

### The badge

`storePredictions` (hourly) computes, for every row it records, what
`getParkPredictions` would serve **right now**: copies of the CatBoost rows,
`applyPcnIntradayOverride` from the current slot (same staleness guard, same
blend, same anchor read). `prediction_accuracy` gains three nullable columns:

- `served_wait_time`, `served_source` (`pcn` | `catboost`) — written at
  generation time, never re-derived later. The blend's anchor is "the wait now";
  recomputing after the slot happened would hand the forecast the answer.
- `served_absolute_error` — filled by `compareWithActuals` beside
  `absolute_error`, against the same actual. An unplanned closure charges the
  full served wait, as it does the raw one.

`aggregate-stats` and the badge's live fallback score `served_absolute_error`
over rows with `served_source IS NOT NULL`.

#### The scored lead is one hour, on purpose

`prediction_accuracy` keeps the latest write per slot, and the generator runs
every 15 minutes including the slot that has just begun. Measured 2026-10-11 on
production (261 419 rows over one day): the scored row's **median lead was
−0.6 minutes** — the old badge was a nowcast. For the served value it would be
worse: at lead 0 the blend serves the anchor, the reading of that moment, and
the comparison scores it against the reading nearest the slot — largely itself.
Every busy ride would read "excellent" by construction.

So the served columns are written only while the slot is at least
`SERVED_ACCURACY_MIN_LEAD_MS` (2 h) away. Closer writes leave them alone —
they go in a separate upsert statement, because TypeORM overwrites a column for
every row of a statement once one row carries it. The value kept is the last
one shown 120–135 minutes ahead. Not one hour: the blend is the pure anchor up
to 1 h and hands over to PCN only by 3 h, so a badge scored at 60–75 minutes
would grade persistence and call every steady busy ride "excellent" (review of
PR #452). At 2 h the served value is half anchor, half PCN. The badge therefore
reads "accuracy of the forecast about two hours ahead", never a whole-day figure.
The raw columns keep their old (latest-write) semantics; the admin boards are
unchanged.

### The day-curve band stays CatBoost's

`/stats/day` plots `wait_time_predictions`, which holds raw CatBoost — the PCN
override is applied on the read paths, never written there. A PCN error drawn
around a CatBoost line would describe neither, so `forecastError` reads
`attraction_accuracy_stats.raw_mae` (CatBoost, all compared rows, NULL below the
badge's 10-comparison floor). That the curve itself is CatBoost while the ride
page serves PCN is a separate served-consistency gap, not fixed here.

## Transition — "no figure yet", never the old figure

Rows written before PAR-854 carry no source and count for nothing:

- **`leadTimeMae`** is absent per bucket until it has 100 scored served rows
  (`MIN_SCORED`). About 360 headliner rows per bucket per night get an actual,
  and a target day is scored once it is over everywhere (the 03:00 job,
  UTC−12 cutoff). So the bucket for lead *L* reports roughly *L* + 2 nights
  after the deploy: d1 ≈ 3 days, d3 ≈ 5, d7 ≈ 9, d14 ≈ 16, d30 ≈ 32, d60 ≈ 62.
  `accuracy.basis` is unaffected — it reads the forecast-accuracy profile, not
  this archive.
- **The badge** reads `insufficient_data` until a ride has 10 compared served
  slots. `aggregate-stats` still groups every ride with completed rows, so a
  ride with only old rows is rewritten as `insufficient_data` with a NULL `mae`
  rather than keeping its CatBoost badge, and stats rows for rides with no
  completed row in the window are deleted. With 15-minute slots that is a few
  hours into the first operating day after the deploy, plus the hourly
  aggregation. The badge cache key moved to `accuracy:badge:v2:…`.
- **`forecastError`** is unaffected: `raw_mae` is computed from the full
  window on the first aggregation run.

## Diagnostic SQL

```sql
-- Served coverage of the badge's input, last day
SELECT served_source, count(*),
       count(*) FILTER (WHERE comparison_status = 'COMPLETED') AS completed,
       avg(served_absolute_error) AS served_mae,
       avg(absolute_error) FILTER (WHERE served_source IS NOT NULL) AS raw_mae_same_rows
FROM prediction_accuracy
WHERE target_time >= now() - interval '1 day'
GROUP BY 1;

-- Served lead-time archive per bucket and source
SELECT lead_days, source, count(*) FILTER (WHERE absolute_error IS NOT NULL) AS scored,
       avg(absolute_error) AS mae
FROM prediction_lead_snapshots
GROUP BY 1, 2 ORDER BY 1, 2;
```
