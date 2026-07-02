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
