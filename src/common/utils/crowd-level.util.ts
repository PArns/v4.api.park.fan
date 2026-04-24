import { CrowdLevel } from "../types/crowd-level.type";

/**
 * Convert a P50-relative occupancy percentage into a CrowdLevel rating.
 *
 * Thresholds (see crowd-level.type.ts):
 * - very_low: ≤ 60%  - low: 61-89%  - moderate: 90-110%
 * - high: 111-150%  - very_high: 151-200%  - extreme: > 200%
 */
export function determineCrowdLevel(occupancy: number): CrowdLevel {
  if (occupancy <= 60) return "very_low";
  if (occupancy <= 89) return "low";
  if (occupancy <= 110) return "moderate";
  if (occupancy <= 150) return "high";
  if (occupancy <= 200) return "very_high";
  return "extreme";
}
