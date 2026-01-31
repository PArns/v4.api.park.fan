/**
 * Unified Crowd Level Type
 *
 * Standard 6-level crowd rating used across all park and attraction endpoints.
 *
 * **Unified Thresholds (Option B - P90 as Expected Baseline):**
 * Both parks and attractions use P90 (90th percentile) as baseline: occupancy = (current / p90) * 100
 * - 100% = P90 = **"moderate"** (expected baseline by park standards)
 *
 * Thresholds (based on occupancy percentage relative to P90):
 * - very_low: ≤ 40% (≤ 0.4x P90) - Much quieter than expected
 * - low: 41-70% (0.41-0.7x P90) - Below expected
 * - moderate: 71-100% (0.71-1.0x P90) - At expected baseline (P90)
 * - high: 101-130% (1.01-1.3x P90) - Above expected
 * - very_high: 131-160% (1.31-1.6x P90) - Significantly above expected
 * - extreme: > 160% (> 1.6x P90) - Exceptionally crowded
 *
 * **Fallback (when P90 unavailable):**
 * Returns 'moderate' to avoid arbitrary absolute thresholds.
 */
export type CrowdLevel =
  | "very_low"
  | "low"
  | "moderate"
  | "high"
  | "very_high"
  | "extreme";
