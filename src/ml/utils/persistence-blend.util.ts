/**
 * Anchor-gated piecewise persistence blend — the served short-lead intraday wait.
 *
 * Mirrors `pcn-service/score.py persistence_blend`, validated on the shadow lead-curve board
 * over ~2 weeks (linear → piecewise → ffill-anchor → anchor-gate): busy 1h MAE improved
 * +2.2..+2.8 min vs raw PCN with a ~neutral aggregate (`all` ≈ −0.04). It blends the served
 * PCN q0.5 toward the CURRENT (anchor) wait at short lead — where the wait "an hour from now"
 * is better predicted by "the wait now" than by the model's forecast:
 *   - weight α is 1 (pure persistence) up to FULL_H, then linear 1→0 to HORIZON_H, then 0
 *     (pure PCN) beyond — i.e. serve the model that wins at each lead (persistence ≤1h, PCN ≥3h);
 *   - up to FULL_H every anchor of at least FIRST_HOUR_MIN_ANCHOR is served as-is (PAR-1026):
 *     94 % of live anchors sit under GATE_MIN, and in the first hour persistence beat the served
 *     curve by 1.7 min MAE on the live forward archive — the wait now is the best 15-min forecast
 *     for a quiet ride too. Below 5 min the reading is a walk-on placeholder, not a wait;
 *   - the 1–3 h ramp stays GATED on the anchor: below GATE_MIN the recent value is noisy/stale
 *     that far out, so it stays pure PCN — that's what kept the shadow aggregate neutral.
 */
export const BLEND_FULL_H = 1.0;
export const BLEND_HORIZON_H = 3.0;
export const BLEND_GATE_MIN = 40;
export const BLEND_FIRST_HOUR_MIN_ANCHOR = 5;

export function persistenceBlendServe(
  pcn: number,
  anchor: number | undefined | null,
  leadHours: number,
  gateMin: number = BLEND_GATE_MIN,
): number {
  if (anchor === undefined || anchor === null) return pcn;
  // First hour: the current wait, for every ride with a real one.
  if (leadHours <= BLEND_FULL_H) {
    return anchor >= BLEND_FIRST_HOUR_MIN_ANCHOR ? anchor : pcn;
  }
  // Ramp: a below-gate (quiet) anchor → serve raw PCN unchanged.
  if (anchor < gateMin) return pcn;
  const alpha = Math.max(
    0,
    Math.min(
      1,
      (BLEND_HORIZON_H - leadHours) / (BLEND_HORIZON_H - BLEND_FULL_H),
    ),
  );
  return alpha * anchor + (1 - alpha) * pcn;
}
