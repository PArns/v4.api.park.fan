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
 * - very_low: ≤ 60%  - low: 61-89%  - moderate: 90-110%  - high: 111-150%
 * - very_high: 151-200%  - extreme: > 200%
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
