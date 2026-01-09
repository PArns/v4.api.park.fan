/**
 * Unified Crowd Level Type
 *
 * Standard 6-level crowd rating used across all park and attraction endpoints.
 *
 * **Unified Thresholds for Parks and Attractions:**
 * Both use P90 (90th percentile) as baseline: occupancy = (current / p90) * 100
 * - 100% = P90 = typical "busy day" baseline
 *
 * Thresholds (based on occupancy percentage relative to P90):
 * - very_low: ≤ 20% (≤ 0.2x P90) - Much quieter than typical
 * - low: 21-40% (0.21-0.4x P90) - Below typical
 * - moderate: 41-70% (0.41-0.7x P90) - Around typical
 * - high: 71-90% (0.71-0.9x P90) - Approaching typical busy day
 * - very_high: 91-120% (0.91-1.2x P90) - At or above typical busy day
 * - extreme: > 120% (> 1.2x P90) - Significantly above typical busy day
 *
 * **Fallback (when P90 baseline unavailable):**
 * For attractions only, uses absolute wait time thresholds:
 * - very_low: ≤ 10 min
 * - low: 11-20 min
 * - moderate: 21-45 min
 * - high: 46-75 min
 * - very_high: 76-120 min
 * - extreme: > 120 min
 */
export type CrowdLevel =
  | "very_low"
  | "low"
  | "moderate"
  | "high"
  | "very_high"
  | "extreme";
