# Crowd Levels: Typical-Day-Peak Baseline (Calendar)

> Status: **implemented (2026-05-23)**, pending deploy steps (see below).
> This document is the working reference for the calendar crowd-level
> calibration. Continue from here.

## TL;DR

The calendar crowd level for a day = **a day's peak ÷ a typical day's peak**:

```
percentage = day_value / typical_day_peak * 100
crowdLevel = determineCrowdLevel(percentage)   // 60 / 89 / 110 / 150 / 200
```

- **day_value** = `AVG` across headliners of each ride's **daily P90** (peak-of-day, averaged so every headliner counts equally).
- **typical_day_peak** = **median over operating days** (548-day window) of that same `day_value`.
- 100% = a statistically typical day = `moderate`. Busy seasons (Wintertraum, Easter) reach `very_high`/`extreme`; quiet weekdays read `low`/`very_low`.
- **Headliner-only** on both sides.

## Why we got here (the investigation)

1. **Original bug:** recent commits used `day_P90 / P50_baseline` ("peak-vs-median"). For Phantasialand P90/P50 ≈ 51.6/30 ≈ 1.7×, so a *normal* day landed at ~170% = `very_high`. Live calendar (Apr–Jun) was 26% `extreme`, 0% `very_low` — a wall of red. The threshold ladder (90–110 = moderate) assumes the ratio ≈ 100% on a typical day; a peak ÷ median never does.

2. **First fix attempt — clean peak-vs-peak (`day_P90 / P90_baseline`):** also fixed the cross-ride aggregation mismatch (numerator was `PERCENTILE_CONT(0.9)` across rides, baseline was `AVG` across rides). Empirically (Phantasialand, 90 days) this **skewed low**: median day ≈ 74% = `low`, and the top **compressed** — even the busiest Wintertraum day only reached 138% = `high`, nothing hit very_high/extreme. Reason: the pooled P90 baseline is itself inflated by the busiest season (it lives in its own 548-day window), so the peak season can't exceed it by much.

3. **Ground-truth check (Wintertraum):** we have Dec 2025 / Jan 2026 (Wintertraum) data. Under peak-vs-peak the ordering was *correct* — Dec 2025 had the highest monthly median (112%) and 12 of the top-15 busiest days were Wintertraum — but the top still capped at `high`.

4. **Chosen fix — typical-day-peak baseline:** divide by the **median of daily peaks** (≈40.3 min for PHL) instead of the pooled P90 (51.6). This is the *right* reference for a calendar ("is this day busier than a typical day?"). Same ordering, but the buckets land naturally:

| Day | day_value | ÷P90 (51.6) | ÷typical-day (40.3) |
|---|---|---|---|
| Jan 11 (Wintertraum peak) | 71.4 | 138% high | **177% very_high** |
| Apr 18 (Easter Sat) | 60.2 | 117% high | 150% high |
| May 16 (busy Sat) | 56.5 | 109% moderate | 140% high |
| quiet Jan 20 | 23.7 | 46% very_low | 59% very_low |

Distribution (90 days) ÷typical-day: very_low 8 / low 24 / moderate 17 / high 25 / very_high 6 / extreme 0 — natural full spread, typical day = moderate.

> Note: do NOT validate against a fixed target distribution. Real crowd levels depend on conditions (e.g. Pfingsten + a discount promo currently makes PHL legitimately full). Validate the **calibration invariant** (typical day ≈ 100% = moderate) and **plausibility vs known days**.

## Design

### Numerator / baseline (both headliner-only, same cross-ride aggregation)
- Numerator: `AVG(per-ride daily P90)` across headliners — `analytics.service.ts` `calculateCrowdLevelForDate` (park branch SQL: `AVG(per_ride.p90)`).
- Baseline: `calculateTypicalDayPeak(parkId, headlinerIds)` — median over days of `AVG(per-ride daily P90)`.

### No fallback
The typical-day-peak is computed and stored **atomically** with P50/P90 (`calculateP50Baseline` returns it; `saveP50Baselines` persists it). So it is present iff the park has a baseline at all. A missing value ⇒ brand-new park with no baseline ⇒ existing no-baseline default (`moderate`). There is **no** typical→P90→P50 fallback chain.

### Storage
- **DB column** `park_p50_baselines.typicalDayPeak` (nullable `numeric(10,2)`) — durable, written in the same upsert as P50.
- **Redis** `park:typicalpeak:{parkId}` — read-through cache (`getTypicalDayPeakFromCache` reads Redis, then the column, then caches).

### Surfaces
- **Calendar past/historical (park):** `calculateCrowdLevelForDate` → typical-day-peak. (`baselineType = "typical_day"`.)
- **Calendar future (park):** `buildPredictedCrowdLevels` (calendar.service.ts) → AVG of predicted headliner waits ÷ typical-day-peak.
- **ML / Python crowd level:** `ml.service.ts` passes `typicalDayPeakBaseline`; `ml-service/predict.py` divides predicted wait by it (fallback: p50 → rolling_avg_7d → 30). Keeps the yearly-predictions endpoint + stored `wait_time_predictions.crowdLevel` on the same scale as the calendar.

### Deliberately UNCHANGED (point-in-time / live → ratio-vs-P50)
- Live overview `calculateParkOccupancy` / `getCurrentOccupancy` (÷P50). Also the ML feature `park_occupancy_pct`.
- Calendar "today" cell (uses the live signal — today is an incomplete day).
- Hourly within-a-day predictions (median ÷ P50).
- Attraction live ratings (÷ attraction P50).
- Boundary rule: **daily aggregates = typical-day-peak; point-in-time/live = ratio-vs-P50.**

### Role of P50 / P90 now
- **P50:** still load-bearing — live occupancy, ML `park_occupancy_pct` feature, attraction live, training. **Keep.**
- **P90:** no longer the calendar reference and no longer a calendar fallback. Still computed for free in the same cron (carries `confidence`/metadata, available for future use). The dead `p90Baseline` that `ml.service.ts` used to send (Python never read it) was removed.

## Lifecycle / consistency
- `identifyHeadliners` (548-day, 3-tier) → `calculateP50Baseline` (computes P50, P90, **typicalDayPeak**) → `saveP50Baselines` (atomic DB upsert + Redis). Daily cron (park job 3 AM) + boot bootstrap.
- New rides become headliners automatically once they accumulate enough data; closed headliners age out of the 548-day window. Self-heals daily.
- Historical correctness: a past day is judged against the *current* headliner set + *current* typical-day-peak (rolling window, 6h cache) — ratings can drift slightly as the window evolves; acceptable for a calendar. The numerator self-corrects (per-day query only includes rides with data that day).

## Deploy steps
1. **Schema:** the runtime runs with TypeORM `synchronize=true` (prod included), so the new nullable `typicalDayPeak` column is created automatically on boot — no manual `ALTER TABLE`. (The `.env.live_debug` file has `synchronize=false`, but that's only the read-mostly debug-script config, not the app runtime.)
2. **Backfill (chunked):** the boot bootstrap forces one batched `calculate-park-baselines` run when no `park_p50_baselines` row has `typicalDayPeak` yet (the job processes parks in groups of 5). If you don't want to wait for boot, trigger `calculate-park-baselines` on the `p50-baseline` queue manually — it batches in 5s.
3. Coolify auto-deploys on push to `main`; build takes a few minutes — verify only after the build completes.

## Verification
- **Calibration invariant** (live DB, read-only): median of `AVG(per-ride day-P90)` over the window ÷ typical-day-peak ≈ 100% (moderate). For PHL the typical-day-peak ≈ 40.3.
- **Plausibility:** known-busy days (Wintertraum, Easter, current Pfingsten weekend) read high/very_high/extreme; quiet days read low/very_low. Success = levels follow real occupancy, not a fixed shape.
- Build + the crowd-level unit specs.

## Known limitations / next steps
- **Future days are ML-limited:** ML emits one smoothed value per attraction/day (no real intra-day peak), so future days read more conservatively than completed days. Improving this is an ML-accuracy task, not a crowd-level-formula one.
- **Per-attraction vs park granularity (ML):** `predict.py` computes per-attraction crowd level using the *park* typical-day-peak (pre-existing pattern — it previously used park P50). Fine numerically/scale-wise; a per-attraction typical-day-peak could be added later if per-ride crowd levels need their own calibration.
- **Rolling-window drift / 6h cache:** documented above; revisit if "history rewriting" becomes user-visible.
- **Data window:** baselines currently span ~Dec 2025 → now (Wintertraum + spring). They will keep self-recalibrating as a full year accumulates.
- `getCrowdLevelTrainingData` (analytics.service.ts) has no callers (Python trains from its own SQL in `db.py`) — deletion candidate, confirm before removing.
