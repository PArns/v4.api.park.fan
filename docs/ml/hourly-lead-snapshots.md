# Hourly lead snapshots (PAR-658)

`hourly_lead_snapshots` holds the hourly forecast as it stood at fixed leads before each slot, so
the error of the hourly forecast can be measured per lead. Open this page before reading the
table, changing its sampling or adding a reader. The daily counterpart is
`prediction_lead_snapshots` ([long-range forecasting §6](long-range-forecasting.md#6-the-lead-time-snapshot-archive)).

## Why `prediction_accuracy` cannot answer it

`PredictionAccuracyService.recordPredictions` upserts on `(attraction_id, target_time)`. The
15-minute run rewrites that row until the slot arrives, so the row keeps the lead of the last run
that still predicted the slot. Over 30 days (4,669,055 completed hourly rows, measured for
PAR-658), the 31,360 rows with a lead over 24 h were 345 rides at an actual wait of 0.03 min:
slots of parks that then stayed CLOSED, which `predict.py` drops from every later run. That
remainder was selected by what happened afterwards, so it cannot serve as a sample. The upsert
stays as it is: the served forecast is the newest one.

## What is written

`HourlyLeadSnapshotService.record`, called by `recordPredictions` with the same rows and served
values, once per park and run:

- **Sampling windows:** only the slot on the full hour is copied, one of the four 15-minute slots
  the hourly run predicts per hour; the other three come from the same run and differ little. It
  is copied when its lead lies in `(edge − 60 min, edge]` for an edge in `SAMPLE_LEADS` = 2 h,
  6 h, 24 h, 36 h. Four runs see it inside that window; the first insert wins (`orIgnore`), so a
  row records the lead at the edge, and a skipped run is covered by the next one.
  `h24+` samples at 36 h because the last slot of the 48 h horizon is not always predicted.
- **Buckets:** `lead_bucket` is `bandLeadBucket(lead_minutes)`, the band calibration's `h0-2` /
  `h2-6` / `h6-24` / `h24+`. `lead_minutes` keeps the exact lead.
- **Per row:** raw CatBoost `predicted_wait_time`, its `uncertainty_minutes` and `model_version`,
  and what the reader was served with its model (`served_wait_time`, `served_source`: `pcn`,
  `chronos`, `catboost`). The served pair is NULL below the 2 h scored lead
  (`SERVED_ACCURACY_MIN_LEAD_MS`), the same rule as in `prediction_accuracy`, so `h0-2` rows are
  raw only.
- **Population:** every ride the hourly run predicts with status OPERATING, UNKNOWN or none,
  the same rows `prediction_accuracy` gets. Not only headliners: the band calibration and the
  PCN comparison run over every ride, and the busy segment (≥ 60 min predicted) is thin enough
  that a headliner-only table would leave its cells empty.

## Volume

Each full-hour slot gets at most four rows, one per bucket, so the table grows by at most four
times the full-hour slots `prediction_accuracy` records per day. Counted in production for the
target days 2026-10-30 to 2026-11-07: 28,700–59,600 full-hour slots a day over 3,200–5,500 rides
(59,600 on Halloween, 28,700–37,000 in the first November week). Expected: **about 115,000 rows a
day off-season, up to 240,000 on a peak day**, ~4–8 M rows at the 35-day retention. Fewer in
practice: a slot gets its `h24+` and `h6-24` rows only if the run 36 h and 24 h ahead already
predicted it. Each run offers only the full-hour slots inside the four windows, at most 4 per
predicted ride, against the 192 slots of the 48 h horizon the accuracy upsert writes.

Per ride that is up to ~9–11 slots a day per bucket, ~250–300 in a 28-day window, so unlike the
daily axis (see
[§8 of long-range forecasting](long-range-forecasting.md#8-why-there-is-no-per-ride-error-curve))
a per-ride hourly curve has the mass for four buckets, and the population can stay every ride.

## Storage

A hypertable on `target_time` (`HYPERTABLES` in `src/database/hypertables.ts`): 7-day chunks,
compressed after 7 days, segmented by `attraction_id` like `wait_time_predictions`, so a merge
decompresses one ride's rows, not the whole chunk. No foreign key. The nightly
`predictions/cleanup-old` job drops whole chunks past `HOURLY_LEAD_SNAPSHOT_RETENTION_DAYS` = 35
(`MLService.dropExpiredPredictionChunks` with `table`, same lock timeout and retries). A park or
ride merge moves the rows (`ATTRACTION_DEPENDENCIES`).

## Reading it

The table holds no actual. A slot's realised wait is on its `prediction_accuracy` row, which was
written in the same call and is kept 90 days. Join on the slot and use the scoring rule
([accuracy scoring rule](accuracy-scoring-rule.md)):

```sql
SELECT s.lead_bucket,
       CASE WHEN s.predicted_wait_time >= 60 THEN 'busy'
            WHEN s.predicted_wait_time >= 30 THEN 'mid' ELSE 'quiet' END AS segment,
       count(*) AS n,
       avg(s.predicted_wait_time - pa.actual_wait_time) AS bias,
       avg(abs(s.predicted_wait_time - pa.actual_wait_time)) AS mae
  FROM hourly_lead_snapshots s
  JOIN prediction_accuracy pa
    ON pa.attraction_id = s.attraction_id AND pa.target_time = s.target_time
 WHERE s.target_time >= now() - interval '28 days'
   AND <scoredSlotSql("pa", MAX_PLAUSIBLE_WAIT_TIME)>
 GROUP BY 1, 2;
```

Readers waiting on it: the per-lead band calibration
([quantile serving](quantile-serving-and-calibration.md#known-limits)), the CatBoost bias at long
lead (PAR-1140) and CatBoost against PCN past a few hours ([intraday serving](intraday-serving.md#known-limits)).
`h24+` and `h6-24` report from the day after deploy; a 28-day window is full four weeks later.
