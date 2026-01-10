/**
 * Unified Crowd Level Type
 *
 * Standard 6-level crowd rating used across all park and attraction endpoints.
 *
 * **Unified Thresholds for Parks and Attractions (Option B):**
 * Both use P90 (90th percentile) as baseline: occupancy = (current / p90) * 100
 * - 100% = P90 = "high" baseline (typical busy day)
 *
 * Thresholds (based on occupancy percentage relative to P90):
 * - very_low: ≤ 15% (≤ 0.15x P90) - Much quieter than typical
 * - low: 16-35% (0.16-0.35x P90) - Below typical
 * - moderate: 36-65% (0.36-0.65x P90) - Around typical
 * - high: 66-100% (0.66-1.0x P90) - At typical busy day (P90)
 * - very_high: 101-130% (1.01-1.3x P90) - Above typical busy day
 * - extreme: > 130% (> 1.3x P90) - Significantly above typical busy day
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
