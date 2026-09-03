# What we can forecast, and how far (2026-09-03)

The trip planner asks for a date weeks or months out. This is the measured answer
to what each model can actually give it — horizons read from the forecast tables
rather than from the docstrings, and accuracy measured against realised days.

Every number here comes from the production database on 2026-09-03. The SQL is in
[`scripts/shape-backtest.sql`](../../scripts/shape-backtest.sql).

---

## 1. The four models and how far they reach

Measured from the forecast tables themselves, not from configuration:

| model | horizon | rides | note |
| --- | --- | --- | --- |
| **CatBoost daily** | **~183 days** | all active | `predict.py` walks the park's schedule, so the horizon is the operator's published calendar: 181–362 days across the live parks |
| **TFT** (`tft_forecasts`) | **60 days** | 3,581 | covers 1,233 of the 1,237 headliners **and ~2,350 more rides** |
| **shape** (`shape_forecasts`) | 1–14 days | 4,893 | expands a daily level into a 15-min curve |
| **PCN** (`pcn_forecasts`) | **0–1 day** (mean lead 6.1 **hours**) | 2,782 | a nowcaster; not a long-range candidate by construction |

CatBoost's own *hourly* generation stops at 24 hours (`HOURLY_PREDICTIONS`), which
is why `/plan/day` composes anything beyond tomorrow from a daily level and a
historical shape.

## 2. TFT degrades gently — and that is the headline

Error against the realised day-P90, 2.5 M comparisons over 45 days:

| lead | n | actual | predicted | MAE |
| --- | --- | --- | --- | --- |
| 1 day | 50 k | 38.7 | 35.3 | 11.66 |
| 2–3 d | 100 k | 38.7 | 35.0 | 11.93 |
| 4–7 d | 201 k | 38.4 | 34.2 | 12.36 |
| 8–14 d | 350 k | 38.4 | 33.1 | 13.22 |
| 15–30 d | 790 k | 38.2 | 31.6 | 13.92 |
| 31–60 d | 1,016 k | 38.3 | 29.7 | 15.09 |

MAE grows by 29 % while the horizon grows sixtyfold. **A forecast two months out
is not much worse than one for tomorrow** — which is the fact the planner rests on.

## 3. The trap that nearly produced a wrong fix

Grouping that same data by the **actual** outcome shows an alarming bias: on days
that turned out busy (≥60 min) the model predicts 58.6 against 88.1 realised, a
factor of 1.50 at long lead. It looks like a calibration defect and invites a
correction table.

It is not. Conditioning on the outcome produces exactly this pattern from
regression to the mean, even for a perfectly calibrated model. Grouped by the
**predicted** value — the only thing available at serving time — the same rows say:

| predicted band | lead | n | predicted | actual | factor | MAE raw | MAE corrected |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ≥60 | ≤7 d | 43 k | 83.3 | 82.2 | **0.987** | 21.93 | 21.89 |
| ≥60 | 8–30 d | 118 k | 83.1 | 82.1 | **0.987** | 23.85 | 23.78 |
| ≥60 | 31–60 d | 88 k | 84.0 | 84.8 | **1.009** | 25.00 | 25.04 |
| 30–59 | 31–60 d | 302 k | 42.2 | 49.0 | 1.160 | 16.57 | 16.27 |
| <30 | 31–60 d | 626 k | 16.0 | 26.6 | 1.660 | 12.99 | 11.95 |

**When the model says "busy", it is unbiased at every lead.** And a multiplicative
correction buys about 1 % of MAE — it is not worth building.

Where the error actually lives: when TFT calls a distant day quiet (<30) the
realised mean is 26.6 with an MAE of 13. It does not know *which* far-out days
will be busy. That is missing signal — events, school holidays, far-future
climatology — not a miscalibrated scale, and it matches what
[covariate usage](./model-overview.md) already concluded from the other side.

**`shape_comparisons` segments by the actual value too**, so its bias column
carries the same artefact. Read its MAE, not its bias.

## 4. TFT beats CatBoost outside the headliners as well

Serving restricts TFT to headliners because that is "the scope the TFT backtest
validated". Measured at lead 1–2 days against the realised day-P90:

| group | n | actual | MAE TFT | MAE CatBoost |
| --- | --- | --- | --- | --- |
| headliners | 5,986 | 54.3 | **15.47** | 23.84 |
| **non-headliners** | 4,696 | 25.4 | **8.67** | 12.06 |

TFT is 35 % better on headliners and **28 % better on everything else**. The
restriction is leaving that on the table for ~2,350 rides.

> Watch the lead definition. Computing it as `predictedTime::date - createdAt::date`
> (UTC) while grouping the day park-locally cut the sample from 10,682 rows to 73
> and made the comparison look inconclusive. Both have to be park-local.

## 5. The intraday shape: neither proposed change survived measurement

`/plan/day` composes a future day's curve from a daily level and the ride's
historical hour shape. Two candidate improvements were tested as a holdout —
training on everything before 2026-08-06, testing on 2026-08-06…09-02, every
candidate rescaled to the day's **actual** peak so only the shape is judged, and
the shape model taken at a fixed 7-day lead so all three know equally little:

| park | n | blind (current) | per weekday | shape model |
| --- | --- | --- | --- | --- |
| europa-park | 2,313 | 4.98 | 5.35 | **4.97** |
| phantasialand | 253 | **6.29** | 10.59 | 7.01 |
| disneyland-park | 6,097 | 7.62 | **7.53** | 8.20 |
| efteling | 1,403 | **4.61** | 5.38 | 4.62 |
| walibi-holland | 412 | 7.88 | 7.27 | **7.23** |
| heide-park | 415 | **4.48** | 5.22 | 5.13 |
| parc-asterix | 876 | **5.16** | 6.81 | **5.16** |
| gardaland | 780 | **3.82** | 4.67 | 4.22 |
| **weighted** | 12,549 | **6.27** | 6.64 | 6.59 |

**A per-weekday shape is worse** (+6 %), and the reason was visible before the
holdout: the noise floor — the same weekday split into even and odd weeks —
measures 0.120 in units of the day peak, against a weekday-vs-blind difference of
0.15. Splitting one year of days seven ways costs more precision than weekday
specificity buys. Phantasialand, with the fewest measured days, degrades worst
(10.59 against 6.29).

**The shape model is at parity**, not the 35 % behind its shadow board reports.
That board compares a 14-day-ahead shape forecast against CatBoost's *freshest*
prediction for the same slot — and CatBoost only forecasts 24 hours, so its series
is always a ≤24 h forecast whatever the `lead_bucket` column says. The giveaway is
in the board itself: CatBoost's MAE is identical across every lead bucket
(8.11 / 8.12 / 8.14), which no real forecast does.

> `shape_forecasts.target_slot` is **park-local** (the schema says so). Treating it
> as UTC shifts every curve by the park's offset and made the shape model look
> 47 % worse in the first cut of this measurement.

## 6. What is still unknown

**CatBoost's accuracy beyond one day cannot be measured retrospectively.** Of its
daily rows for target dates that have passed, 72,795 sit at lead 1 and only 179 at
8–30 days: `deduplicatePredictions` rewrites a day's prediction on every nightly
run until only the last survives. That is precisely why
[`prediction_lead_snapshots`](../../src/ml/entities/prediction-lead-snapshot.entity.ts)
records forward instead. It began filling on 2026-09-03; the 1-day bucket reports
within days, the 60-day bucket in sixty.

Until then, past 60 days the planner has exactly one model and no measurement of
it — which is why `/plan/day` labels that range and `leadTimeMae` answers `null`
rather than a number.

## Related

- [`/plan/day`](../frontend/plan-day-endpoint.md) — what the planner does with all this
- [TFT vs CatBoost — daily forecast split](./neuralforecast-tft-evaluation.md) — the production split this measurement revisits
- [PCN intraday review](./pcn-intraday-review.md) — the same scorer-window distortion, found earlier from the PCN side
