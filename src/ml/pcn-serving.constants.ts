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
