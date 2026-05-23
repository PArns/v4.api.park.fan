/**
 * Unified Crowd Level Type
 *
 * Standard 6-level crowd rating used across all park and attraction
 * endpoints.
 *
 * **Semantic — daily vs live boundary:**
 *
 * Two regimes coexist. Daily/historical aggregates compare a day's peak to
 * a **typical day's peak**; point-in-time/live signals are
 * **ratio-vs-P50**. They are never mixed on a single surface.
 * See docs/analytics/crowd-level-typical-day-peak.md for the full story.
 *
 * - **Calendar daily crowd level:** a day's value is the **AVG across
 *   headliner rides** of each ride's daily P90 (peak-of-day, every
 *   headliner contributing equally — NOT a percentile across rides). The
 *   denominator is the **typical-day-peak baseline** = the median over
 *   operating days of that same day value (548-day window, headliner-only).
 *   100% reads as "a statistically typical day" (= moderate); busy seasons
 *   (Wintertraum, Easter) reach very_high/extreme. The pooled P90 baseline
 *   is NOT used — it's inflated by the busiest season and compresses the
 *   top. Future/predicted days use the same baseline (AVG of predicted
 *   headliner waits ÷ typical-day-peak).
 * - **Park live occupancy (ratio-vs-P50):** current short-window peak
 *   ÷ park P50 baseline (headliner-only). Also consumed as an ML feature
 *   (getCurrentOccupancy). Deliberately NOT changing.
 * - **Calendar "today" cell (ratio-vs-P50):** uses the live signal,
 *   because today is an incomplete day.
 * - **Hourly within-a-day predictions (ratio-vs-P50):** per-hour median
 *   ÷ park P50 baseline.
 *
 * **No calendar fallback:** the typical-day-peak is written atomically with
 * P50/P90 (park_p50_baselines.typicalDayPeak + Redis), so a missing value
 * means the park has no baseline at all (brand-new) → neutral default.
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
