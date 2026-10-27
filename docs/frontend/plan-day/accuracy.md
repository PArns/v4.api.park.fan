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
has scored it there ([why](../../ml/long-range-forecasting.md#6-the-lead-time-snapshot-archive)).
Present such a day as indicative, never as something to plan a morning around. A
`climatology` day is always `unmeasured`.

## `expectedError`: per ride, because the error has two axes

The cell of the served error grid (predicted band × lead bucket, MAE in minutes
against the realised day-P90) for this ride's predicted level and this distance.
The grid, its buckets and how it is built:
[long-range forecasting §7](../../ml/long-range-forecasting.md#7-the-served-error-grid).
One figure per day would understate a headliner by ten minutes and overstate a
small ride by four.

- **Each column is an upper edge**: a 10-day request reads `≤14 days`. A missing
  cell widens to the next coarser bucket (`ForecastAccuracyService.lookup`), so the
  figure errs towards "at least this wrong". Past 60 days there is no column
  (`basis: "unmeasured"`).
- **Do not turn a row into a multiplier.** The bands widen by minutes, not by a
  proportion: read the cell, do not scale the prediction.
- **A typical miss, not a bound.** Roughly half of days land further out: "give or
  take", never a range that contains the answer.

## `leadTimeMae`

The measured MAE of the **served** daily number (TFT for headliners over days 1–60,
CatBoost where TFT has nothing) at this distance
(`PredictionLeadSnapshotService.getLeadTimeMae`;
[reader-facing accuracy](../../ml/reader-facing-accuracy.md#leadtimemae)). It reads
the nearest sampled bucket **at or below** `leadDays` (`LEAD_BUCKETS`
1/3/7/14/30/60): a 20-day question gets the 14-day figure.

Absent while that bucket has fewer than `MIN_SCORED` scored served rows (when each
fills: [the snapshot archive](../../ml/long-range-forecasting.md#6-the-lead-time-snapshot-archive)).
**Absent is the honest answer**: widen a band with distance without attaching a
figure rather than invent one. Absent on a `climatology` day and for `leadDays ≤ 0`.
