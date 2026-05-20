/**
 * Unified Crowd Level Type
 *
 * Standard 6-level crowd rating used across all park and attraction
 * endpoints.
 *
 * **Semantic (since the P90 switch):**
 *
 * Crowd readings are now **peak-vs-peak** — current/today's P90 wait
 * divided by the 548-day P90 baseline. The previous implementation used
 * P50-vs-P50 (avg-vs-avg) which underweighted the peak waits users
 * actually remember (the headliner at 14:00 on a busy Saturday).
 *
 * - **Park live occupancy:** avg-of-per-headliner-MAX in the last 60 min
 *   ÷ park P90 baseline (headliner-only). The 60-min window keeps the
 *   reading responsive while still being statistically meaningful with
 *   ~12 samples per ride.
 * - **Calendar daily crowd level:** today's P90 wait ÷ park P90 baseline.
 * - **Attraction crowd level:** current wait ÷ per-attraction P90
 *   baseline.
 *
 * Both numerator and denominator are P90 — apples-to-apples, 100%
 * reads as "typical day's peak".
 *
 * **Fallback:** When a P90 baseline isn't yet populated (brand-new
 * entity before the next 3 AM / 4 AM cron), the API falls back to the
 * P50 baseline + P50-shaped numerator. Still apples-to-apples, just an
 * avg-vs-avg reading until P90 fills in.
 *
 * **Thresholds (see determineCrowdLevel):**
 * - very_low: ≤ 60%   - low: 61-89%      - moderate: 90-110%
 * - high: 111-150%    - very_high: 151-200%   - extreme: > 200%
 *
 * **Final fallback (no baseline at all):** Returns 'moderate'.
 */
export type CrowdLevel =
  | "very_low"
  | "low"
  | "moderate"
  | "high"
  | "very_high"
  | "extreme";
