# Chronos-2 served on the live surface (PAR-911)

While the constant `SERVE_CHRONOS_INTRADAY` in `src/ml/chronos-serving.ts` is `true`, Chronos-2's
`live` curve serves the intraday slots 45 minutes to 4 hours ahead (to 2 hours until PAR-1409);
nothing else changes. **On since 2026-11-22.** It was an env flag until PAR-1410 and is a code
constant now, so no Coolify variable can override it:
prepared so that, once the [live A/B](chronos-shadow.md) confirms the offline result (Chronos-2
beats persistence from 30 min and the served PCN/profiles at 30–120 min; persistence stays best at
15 min — [chronos-2.md](../foundation-models/chronos-2.md)), switching is a flag flip. Open this
page to switch it on or off, or to understand a `+chronos` model version.

## Which model serves which lead

Lead = slot start − the current slot start (`src/ml/chronos-serving.ts`,
`MLService.applyServedIntradayOverride`); the persistence blend measures its own lead from the
actual time (PAR-1026).

| Lead | Both flags on | `SERVE_PCN_INTRADAY` off |
| --- | --- | --- |
| 0–30 min | PCN + persistence blend (pure anchor for a ride with an OPERATING reading ≥ 5 min) | CatBoost |
| 45–240 min | **Chronos-2** q50 | **Chronos-2** q50 |
| > 240 min | PCN + blend (the anchor share has decayed to 0 by 3 h) | CatBoost |

**Why 4 hours (PAR-1409, 2026-11-22).** The live archive, against what was served, over 28 days
and 136 parks (autumn only): Chronos-2 −1.2 min at 1–2 h and −1.1 min at 2–4 h, both sure; in the
first hour −0.1 min, not sure, so ≤ 30 min stays the blend. The `live` curve reaches 240 min
(`keep_minutes`), so no new call is needed; a curve up to 90 min old covers only up to ~150 min
from now, and the slots it lacks stay PCN. 6–12 h (−0.8 min, sure) comes from the 06:00
`chronos_grid` curve, which is not in the serving cache.

A ride without a cached curve, a curve older than **90 min** (`CHRONOS_MAX_CURVE_AGE_MS`), or a slot
the curve has no value for keeps what the row above would serve — the fallback is the incumbent,
never an empty slot. The seam is at 240 → 255 min (Chronos-2 → pure PCN, the blend's anchor share is 0 from 3 h).

Per served slot, the same quantile split CatBoost and PCN use
([quantile serving](../quantile-serving-and-calibration.md)):

- wait = q50 through `roundServedWait` (5-minute steps, ≥ 5 when positive);
- `uncertaintyMinutes` = q95 − q50, one-sided (Chronos-2's own quantile, not a measured residual);
- `crowdLevel` = q80 (clamped ≥ the shown wait) ÷ the baseline the row carries — the
  typical-day-peak baseline, P50 only where that is missing (`crowd_level_for` in
  `ml-service/predict.py`; [crowd-level rules](../../rules/crowd-levels.md)), as the PCN override
  does; no baseline → the crowd level is left as it was;
- `modelVersion` = `<catboost version>+chronos` (replacing `+pcn`).

## Serving cache and cadence

The request path never calls nf-service. It reads one Redis `MGET` of
`ml:chronos-serve:<attractionId>` (JSON: `originAt`, `revision`, the curve's segments with
q50/q80/q95; TTL 3 h, `CHRONOS_SERVE_TTL_S`; the 90-min guard decides) for the rides being served.
Writers, both on the hourly `forecast-archive` job (:05):

| Park-local hour | Writer | GPU cost |
| --- | --- | --- |
| 10, 12, 14, 16, 18 | the intraday archive capture: its `live` response is written to the cache whatever the flag says, so the cache can be inspected before the switch | none extra |
| 11, 13, 15, 17 | `refreshChronosServing`: one `live` call (`keep_minutes` 240) per park the daily capture marked active, cache only, claimed per park and hour (`NX`) | only while the flag is on |

Hourly keeps the newest curve ≤ ~80 min old (2-hourly would leave it up to ~2 h 15 min old and the
guard would retire it half the time). Measured 2026-10-16/17: up to 64 parks at one origin hour
(~1.3 k rides), ~3 s per 40-ride park; a TFT training run (503) only costs those hours' refresh.
Hours 19+ are not refreshed because the A/B measured origins 10–18 only: after ~19:30 a park falls
back to PCN. With the flag on, an intraday capture awaits the Chronos-2 call before its served read,
so that origin's surface-1 row already shows the new curve.

## Where it applies

Every read path that applies the PCN override applies this one too (step 2 of the served read; the
list of served and CatBoost-pure reads is in [intraday serving](../intraday-serving.md)). Deviation
badges are not affected: they compare the current slot, inside the ≤ 30 min stretch Chronos-2 never
serves.

**The accuracy badge follows Chronos-2 since PAR-1409.** The recorder scores each slot at the last
write ≥ 2 h ahead (`SERVED_ACCURACY_MIN_LEAD_MS`); with Chronos-2 serving up to 4 h, that row is
Chronos-2's wherever its curve covered the slot, and `prediction_accuracy.served_source` reads
`chronos` there ([reader-facing accuracy](../reader-facing-accuracy.md)). Until 2026-11-22 the
window ended at 2 h and the scored row was mostly PCN/blend.

## The archive after the switch

- Surface 1 (`park_hourly`) keeps archiving what is served; its Chronos-2 slots carry source `s`
  (`chronos2_served`).
- Surface 8 (`park_hourly_incumbent`), captured only while the flag is on: the same read with
  `{ chronos: false }`. `scorePairedShadows` pairs `chronos_live` with it as `served` when present,
  so Chronos-2 is never compared with its own served copy, and the admin matrix continues as one
  series across the switch. Scored under source `incumbent`, never into `all`.
- The `persistence` reference is unchanged.

## Switch-on checklist

1. [Chronos board](chronos-shadow.md#paired-scoring-and-the-board): `chronos2` vs `served` on UC1
   `h0-1` … UC2 2-hour leads reads `chronos_better` with ≥ 14 days and ≥ 10 parks, and vs
   `persistence` is not worse beyond 30 min. The ML overview's decision `live_chronos` is judged on
   2026-11-09.
2. Cache is warm: `redis-cli --scan --pattern 'ml:chronos-serve:*' | wc -l` during an intraday hour
   ≈ the rides of the parks at that origin; a sample's `originAt` is the last even hour.
3. `GET /v1/admin/ml-overview`: the Chronos component is not `failed` / `warming`.
4. Set the constant `SERVE_CHRONOS_INTRADAY` to `true` in `src/ml/chronos-serving.ts` and deploy.
5. Next odd hour: the archive job log shows the refresh; the overview says "Live ausgeliefert";
   `GET /v1/parks/<slug>` shows `modelVersion …+chronos` on 45–240 min slots.
6. Next day's scoring: surface 8 rows exist, `chronos2` vs `served` keeps its sign, and
   `chronos2_served` slots appear in the scores.

**Rollback:** set the constant `SERVE_CHRONOS_INTRADAY` to `false` and deploy. Every read path is back to
PCN/blend/CatBoost on the next request; nothing persisted depends on the flag (cache keys expire
within 3 h and are only read while it is on; surface 8 stops being written). The shadow keeps
running under `CHRONOS_SHADOW_ENABLED`.
