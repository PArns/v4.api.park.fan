# `/plan/day`: how wrong the day is

The three measured error fields: `accuracy` (the day's planability signal),
`rides[].expectedError` (per ride, by predicted band × lead), and `leadTimeMae`
(the served model's error at this distance). Part of the
[`/plan/day` contract](./README.md). Which model produces which figure is in
[reader-facing accuracy](../../ml/reader-facing-accuracy.md).

```jsonc
"accuracy": { "basis": "measured", "typicalError": 15.4, "sampleSize": 742010 },
"rides": [
  { "attractionSlug": "taron", "dayPeak": 60, "expectedError": 25.1 },
  { "attractionSlug": "raik",  "dayPeak": 20, "expectedError": 10.7 }
]
```

## `accuracy.basis` is the planability signal

`measured`: the forecast at this distance has been scored against realised days,
and every ride carries an `expectedError`; `typicalError` is the day's typical
error and `sampleSize` the scored comparisons behind the cells quoted.
`unmeasured`: **past 60 days**, where the only model left is CatBoost and nobody
has scored it there (`deduplicatePredictions` keeps only a day's last prediction,
so of its rows for past days 72,795 sit at one day out and 179 at 8–30). Present
such a day as indicative, never as something to plan a morning around. A
`climatology` day is always `unmeasured`.

## `expectedError`: per ride, because the error has two axes

45 days of forecasts scored against the realised day-P90, MAE in minutes:

| predicted | ≤1 day | ≤3 days | ≤7 days | ≤14 days | ≤30 days | ≤60 days |
| --------- | ------ | ------- | ------- | -------- | -------- | -------- |
| ≥ 60 min  | 21.5   | 21.6    | 22.9    | 24.3     | 25.1     | 25.5     |
| 30–59 min | 12.9   | 13.2    | 13.6    | 14.7     | 15.5     | 16.5     |
| < 30 min  | 8.6    | 8.7     | 9.0     | 9.9      | 10.7     | 12.5     |

One figure per day would understate a headliner by ten minutes and overstate a
small ride by four.

- **Each column is an upper edge**: a 10-day request reads `≤14 days`. A missing
  cell widens to the next coarser bucket (`ForecastAccuracyService.lookup`), so the
  figure errs towards "at least this wrong". Past 60 days there is no column
  (`basis: "unmeasured"`). The six buckets match what the forward archive samples.
- **Do not turn a row into a multiplier.** Every band widens by about four minutes
  from one day to sixty — +19 % busy, +45 % quiet — so the growth is minutes, not a
  proportion. Read the cell; do not scale the prediction.
- **A typical miss, not a bound.** Roughly half of days land further out: "give or
  take", never a range that contains the answer.
- **The band is the predicted level, never the realised one.** Grouping by outcome
  reproduces regression to the mean and makes a calibrated model look biased
  ([long-range forecasting](../../ml/long-range-forecasting.md)).
- The grid is rebuilt nightly at 03:10 UTC (`rebuild-accuracy-profile`), never
  configured.

## `leadTimeMae`

The measured MAE of the **served** daily number (TFT for headliners over days 1–60,
CatBoost where TFT has nothing) at this distance, from `prediction_lead_snapshots`
(`PredictionLeadSnapshotService.getLeadTimeMae`). It reads the nearest sampled
bucket **at or below** `leadDays` (`LEAD_BUCKETS` 1/3/7/14/30/60): a 20-day question
gets the 14-day figure.

Absent until that bucket has 100 scored served rows (`MIN_SCORED`); the archive
counts served rows only since PAR-854 (2026-10-11), so a bucket fills roughly lead
+ 2 nights after that (d1 ≈ 3 days, d60 ≈ 62). **Absent is the honest answer**:
widen a band with distance without attaching a figure rather than invent one.
Absent on a `climatology` day and for `leadDays ≤ 0`.
