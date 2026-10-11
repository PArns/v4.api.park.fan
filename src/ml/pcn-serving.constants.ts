/**
 * PCN champion-swap serving constants, shared by every consumer that must present
 * the SAME served intraday numbers (ml.service read paths, prediction-deviation).
 *
 * Staleness guard: only PCN forecasts written in the last N hours may override
 * CatBoost. PCN's whole edge is being a nowcaster; a frozen producer would
 * otherwise keep serving day-old waits live (happened 2026-06-30, stuck lock).
 * Under the healthy every-15-min cadence the newest row is under 15 min old, so
 * this never fires — it only trips on a real producer outage.
 */
export const PCN_MAX_FORECAST_AGE_H = 3;

/**
 * How far ahead of its slot a served intraday value must have been shown to be
 * scored for the reader-facing badge (PAR-854).
 *
 * `prediction_accuracy` keeps the LATEST write per slot, and the generator runs
 * every 15 minutes including the slot that has just begun: measured on
 * production (2026-10-11, 261 419 rows over a day) the scored row's median lead
 * was −0.6 minutes. For CatBoost that was already a nowcast. For the served
 * value it would be worse than meaningless: at lead 0 the §7.7 blend serves the
 * anchor — the wait read at that moment — and the comparison job scores it
 * against the queue reading nearest the slot, i.e. largely against itself. A
 * badge built from that would say "excellent" for every busy ride by
 * construction.
 *
 * So the served columns are written only while the slot is still at least this
 * far away; the latest such write wins, which pins the scored lead to between
 * one hour and one generation cycle more. One hour is the near end of what a
 * reader plans with ("what will it be when I get there"), and at that lead the
 * blend is still the anchor — persistence an hour ahead, which is a forecast,
 * not a reading.
 */
export const SERVED_ACCURACY_MIN_LEAD_MS = 60 * 60 * 1000;

/** Champion-swap flag: serve PCN's intraday 15-min forecast in place of CatBoost's
 * `hourly` predictions. Read per call so tests/env toggles apply without restart. */
export function servePcnIntraday(): boolean {
  return process.env.SERVE_PCN_INTRADAY === "true";
}

/**
 * Serve-side wait rounding — the EXACT mirror of ml-service `predict.py`
 * `round_to_nearest_5` + the operating min-10 rule (`predict.py` ~:1959-1969):
 * half-up to 5-minute steps (2.5→5, 7.5→10), then a floor of 10 for anything
 * positive. CatBoost applies this before storing, so every wait users ever saw is
 * 0 or a 5er-step ≥10; PCN's raw q0.5 must go through the same boundary or the
 * swap leaks 1-minute values into the UI. pcn_forecasts stays RAW on purpose —
 * scoring wants precision; only serving (and the scorer's served-fairness column)
 * quantizes. Slots reaching this code are always operating forecasts (CLOSED rows
 * are never stored), so the min-10 rule applies unconditionally here.
 */
export function roundServedWait(value: number): number {
  const rounded = Math.floor((value + 2.5) / 5) * 5;
  return rounded > 0 ? Math.max(10, rounded) : 0;
}
