# Intraday serving path (NestJS)

How the API turns CatBoost's stored and cached hourly rows into the 15-minute curve a reader
sees: the PCN override, the persistence blend, the Chronos-2 window and the band factor — and
which reads must stay CatBoost-pure. Open it before touching an hourly read in `MLService`, or
anything that stores or scores predictions. The PCN model itself is on [PCN](pcn.md).

## Order of operations

Every served hourly read runs `MLService.serveHourlyRows` on **copies** of the CatBoost rows:

1. **PCN override + persistence blend** (`SERVE_PCN_INTRADAY=true`): every slot PCN has a fresh
   forecast for (`applyPcnIntradayOverride`).
2. **Chronos-2** (`SERVE_CHRONOS_INTRADAY=true`, PAR-911): the slots 45–120 minutes ahead its
   cached curve covers. ≤ 30 min stays the blend (persistence wins at 15 min), > 120 min stays
   PCN, and a missing or > 90-minute-old curve leaves what step 1 served. Design, cache, cadence
   and switch-on checklist: [Chronos-2 served](forward-archive/chronos-serving.md).
3. **Band factor** (PAR-1060) on the rows that stayed CatBoost:
   [Quantile serving](quantile-serving-and-calibration.md#catboost-hourly-band-split-conformal-factor-par-1060).

Both flags are read per call (`servePcnIntraday()` in `src/ml/pcn-serving.constants.ts`,
`serveChronosIntraday()` in `src/ml/chronos-serving.ts`), so a flip needs no restart. With both off
every read returns CatBoost apart from step 3. `servedIntradayValues` reports which model
produced each slot (`pcn` / `chronos` / `catboost`).

## Which reads are served

| Read | Readers | What it returns |
|---|---|---|
| `getParkPredictions(…, "hourly")` | park page curve, `/plan/day` today, calendar today, favourites | served (steps 1–3 on copies of the cached answer) |
| `getBatchStoredPredictions` | the attraction cards in the park response | served |
| `getAttractionPredictionsWithFallback` | ride page `hourlyForecast` (stored rows, or the live ml-service answer for a new ride) | served |
| `PredictionDeviationService.getServedPcnWait` | deviation badges | the served PCN value, so a badge judges the number a reader saw |
| `servedIntradayValues` | `storePredictions`, recording the reader-facing badge's columns (`prediction_accuracy.served_*`, captured ≥ 2 h ahead, `SERVED_ACCURACY_MIN_LEAD_MS`) | served value + source — [Reader-facing accuracy](reader-facing-accuracy.md) |
| `getRawParkPredictions` | `predictions/generate-hourly` and `generate-daily` only | **CatBoost-pure**, Redis-cached (hourly 30 min, daily 13 h) |

### Serving and storing are separate reads

`getRawParkPredictions` is the **only** park-level read the prediction generator may hand to
`storePredictions`. `wait_time_predictions` and `prediction_accuracy` are the CatBoost side of the
PCN-vs-CatBoost board, so a served (PCN / blended) number written there would make the board
compare PCN with itself. The served `getParkPredictions` applies the overrides on copies, so
neither the Redis entry nor the stored rows ever carry a `+pcn` number.

This rule came out of PAR-817 (2026-10-09). The override was wired into the park-level path on
2026-07-02 but never fired there: `getPcnIntradayWaits` keyed its lookup on
`Date.toISOString()` (`…T12:15:00.000Z`), the DB read paths produce that same string, and the
park-level path hands over the ml-service response, whose `predictedTime` is Python
`isoformat()` (`…T12:15:00+00:00`). Same instant, different string, zero matches — the park page,
`/plan/day` today, calendar today and favourites served pure CatBoost while ride pages served PCN.
**The lookup compares instants** (epoch ms on both sides) since then. Fixing only the keys would
have written PCN into the CatBoost board, because the generator then called the served method.

`prediction_accuracy` therefore measures CatBoost, the fallback, wherever PCN serves — the
admin "live MAE" is not the served product. The served figures are the `servedIntraday` panel of
the ML dashboard and `GET /v1/ml/accuracy/system`
(`PredictionAccuracyService.getServedIntradayAccuracy`, read off the PCN board), the forward
archive and the reader-facing badge; the drift monitor splits by horizon (`byHorizon`).

## The PCN override

`getPcnIntradayWaits` reads, per ride and target slot, the freshest origin's q0.5 and q0.8 from
`pcn_forecasts`, only rows written in the last **3 hours** (`PCN_MAX_FORECAST_AGE_H`). A frozen
producer — the 15-minute cron stuck on a lock on 2026-06-30 — would otherwise keep serving
day-old waits; under healthy operation the newest row is < 15 minutes old and the guard never
trips. A park whose forecast is skipped (stale feed, no model) falls back the same way. Any read
error → pure CatBoost.

Per matched slot (`applyPcnIntradayOverride`):

- **wait** = q0.5 through the persistence blend, then `roundServedWait` — half-up to 5-minute
  steps and ≥ 5 when positive, the exact mirror of `predict.py round_to_nearest_5` and
  `pcn-service/score.py serve_round` (a plain round leaked 1-minute waits);
- **crowd level** = q0.8 through the same blend, clamped to ≥ the blended display, rounded, ÷ the
  row's carried baseline (the typical-day-peak, P50 when missing), via `determineCrowdLevel`;
  only when the baseline is > 0. Crowd from q0.5 was a bug (fixed 2026-07-02): it rated every
  busy slot low.
- `uncertaintyMinutes` = `null` — PCN stores no q0.95, and CatBoost's band beside a PCN wait
  described a different forecast;
- `modelVersion` gets `+pcn`.

A slot or ride PCN has nothing for keeps CatBoost.

## The persistence blend (PAR-1026, PAR-1029)

At short lead "the wait now" predicts the wait an hour from now better than the model.
`persistenceBlendServe` (`src/ml/utils/persistence-blend.util.ts`; called the "§7.7 blend" in code
comments):

| lead (from now) | anchor | served q0.5 |
|---|---|---|
| ≤ 1 h | ≥ 5 min (`BLEND_FIRST_HOUR_MIN_ANCHOR`) | the anchor |
| ≤ 1 h | < 5 min or none | raw PCN |
| 1 – 3 h | ≥ 40 min (`BLEND_GATE_MIN`) | linear anchor → PCN |
| 1 – 3 h | < 40 min or none | raw PCN |
| ≥ 3 h | any | raw PCN |

- **Anchor** (`fetchCurrentWaits`): the newest STANDBY reading within 3 h, used only when it is
  OPERATING with a wait — a ride that just went DOWN has no anchor (PAR-1029, the rule of
  `ForecastArchiveService.liveWaits`). Read from `queue_data` per call, so every path agrees.
- **Lead** is measured from the actual time (`nowMs`), not the slot start: in the first hour that
  changes nothing on the 15-min grid, but on the 1–3 h ramp α follows the clock, so the value can
  drift by up to ~1/8 of the anchor–PCN gap within one slot, and a cached response or the :05
  archive capture can differ slightly from a later read in the same slot.
- **Why this shape.** Validated on the PCN lead-curve board over ~2 weeks (linear → piecewise →
  forward-filled anchor → anchor gate): busy 1-hour MAE improved 2.2–2.8 min over raw PCN with a
  neutral aggregate (`all` ≈ −0.04). The gate kept quiet rides with stale anchors on raw PCN. Then
  PAR-1026 (2026-10-24): 94.3 % of live anchors were under 40 min, so the gate left almost every
  ride on raw PCN even 15 minutes ahead, and on the live forward archive persistence beat the
  served curve by 1.7 min MAE in the first hour — hence the first-hour rule for every real wait.
- **Mirror.** `pcn-service/score.py persistence_blend` applies the same rules, so the `pcn_blend`
  model on the lead-curve board scores what is served. Check live: `live_h0_1` persistence vs
  served in `forecast_archive_paired` (definition 2) should move toward 0.

## Open

- A CatBoost head-to-head at PCN's longer leads needs CatBoost's forward forecasts stored per
  origin (CatBoost keeps only the newest row per slot); not built.
- Which parks the forecast tick skipped, and why, is only in the pcn-service log; neither
  `/health` nor `/v1/admin/system-health` shows it.
