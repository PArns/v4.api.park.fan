import { CrowdLevel } from "../types/crowd-level.type";

/**
 * Convert an occupancy percentage into a CrowdLevel rating.
 *
 * The percentage is regime-specific (see crowd-level.type.ts): the calendar
 * uses a day's peak ÷ the typical-day-peak baseline; live signals use the
 * current peak ÷ the P50 baseline. Either way 100% ≈ a typical reading.
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
