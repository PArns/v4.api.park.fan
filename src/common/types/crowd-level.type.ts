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
 * - **Park live occupancy (ratio-vs-P50):** the **baseline-weighted mean**
 *   across the headliners reporting in the last 60 min — Σ their current
 *   waits ÷ Σ their P50 baselines (`getHeadlinerLoad`). Sum the minutes,
 *   then divide; never a mean or percentile of the per-ride ratios. A
 *   percentile across ratios is an extreme-value estimator — over a
 *   ten-ride headliner set its P90 is just the second-busiest ride, it can
 *   only push the reading up, and it lets a 10-minute-baseline ride outvote
 *   a marquee. Falls back to avg-current ÷ park P50 when no per-ride
 *   baselines exist. This is `calculateParkOccupancy` — the user-facing
 *   reading. The ML feature `park_occupancy_pct` is a *different* function
 *   (`getCurrentOccupancy`) and deliberately stays on the older park-wide
 *   `avg latest ÷ park P50` shape: trained models depend on that exact
 *   feature distribution, so moving it would require a retrain cycle.
 * - **Calendar "today" cell (ratio-vs-P50):** uses the live signal,
 *   because today is an incomplete day.
 * - **Hourly within-a-day predictions (ratio-vs-P50):** per-hour median
 *   ÷ park P50 baseline.
 *
 * **No calendar fallback:** the typical-day-peak is written atomically with
 * P50/P90 (park_p50_baselines.typicalDayPeak + Redis), so a missing value
 * means the park is not ratable yet → `unknown` (see below).
 *
 * **Thresholds (see determineCrowdLevel):**
 * - very_low: ≤ 60%   - low: 61-89%      - moderate: 90-110%
 * - high: 111-150%    - very_high: 151-200%   - extreme: > 200%
 *
 * **`unknown` ("keine Prognose"):** emitted when a park is **not ratable** —
 * it has no baseline / fewer than 30 operating days of valid headliner data
 * (park_p50_baselines.typicalDayPeak IS NULL). A median over a handful of
 * days is noise, so rather than render a made-up `moderate` we say so
 * explicitly. It is also what every other "nothing to rate against" path
 * emits: `rateOrUnknown`, the `isParkRatable` gate, `getLoadRating` with a
 * missing/non-positive baseline, `calculateParkOccupancy` when the park has
 * no live sample at all, and the callers of `getAttractionCrowdLevel` when it
 * returns null (i.e. no wait reported, or no baseline).
 * `determineCrowdLevel`'s thresholds never return it — a 0-minute wait against
 * a *real* baseline is a walk-on, not missing data, and reads `very_low`.
 * Frontend renders `unknown` as "Keine Prognose / noch nicht genug Daten".
 */
export const CROWD_LEVEL_VALUES = [
  "very_low",
  "low",
  "moderate",
  "high",
  "very_high",
  "extreme",
  "unknown",
] as const;

export type CrowdLevel = (typeof CROWD_LEVEL_VALUES)[number];

/**
 * The same list plus `closed`, for the surfaces that fold "park/ride is shut"
 * into the same field (calendar days, attraction badges).
 *
 * Both constants exist so a Swagger `enum:` can never drift from the TS union
 * the handler actually returns — that drift is what left `unknown` out of the
 * published contract while the API had been emitting it for months.
 */
export const CROWD_LEVEL_WITH_CLOSED_VALUES = [
  ...CROWD_LEVEL_VALUES,
  "closed",
] as const;
