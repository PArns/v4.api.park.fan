/**
 * Unified Crowd Level Type
 *
 * Standard 6-level crowd rating used across all park and attraction endpoints.
 *
 * **Park occupancy:** Uses P50 (median) baseline from headliners only: occupancy = (current / p50) * 100
 * - 100% = P50 = **"moderate"** (typical day). Baseline from getP50BaselineFromCache.
 *
 * **Attraction crowd level:** Uses per-attraction P90 (or P50 where used) for load rating.
 *
 * Thresholds (P50-relative for parks, see determineCrowdLevel):
 * - very_low: ≤ 50%  - low: 51-79%  - moderate: 80-120%  - high: 121-170%
 * - very_high: 171-250%  - extreme: > 250%
 *
 * **Fallback (when baseline unavailable):** Returns 'moderate'.
 */
export type CrowdLevel =
  | "very_low"
  | "low"
  | "moderate"
  | "high"
  | "very_high"
  | "extreme";
