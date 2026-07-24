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
 *   - GATED on the anchor: below GATE_MIN the ride has no real wait (quiet walk-on) and its
 *     recent value is noisy/stale, so it stays pure PCN — that's what kept the aggregate neutral.
 */
export const BLEND_FULL_H = 1.0;
export const BLEND_HORIZON_H = 3.0;
export const BLEND_GATE_MIN = 40;

export function persistenceBlendServe(
  pcn: number,
  anchor: number | undefined | null,
  leadHours: number,
  gateMin: number = BLEND_GATE_MIN,
): number {
  // No anchor, or a below-gate (quiet) anchor → serve raw PCN unchanged.
  if (anchor === undefined || anchor === null || anchor < gateMin) return pcn;
  const alpha = Math.max(
    0,
    Math.min(
      1,
      (BLEND_HORIZON_H - leadHours) / (BLEND_HORIZON_H - BLEND_FULL_H),
    ),
  );
  return alpha * anchor + (1 - alpha) * pcn;
}
