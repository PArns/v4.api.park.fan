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

/**
 * A park's live headline, its Ø wait and crowd level, from ONE occupancy
 * reading (`calculateParkOccupancy`, or its `park:occupancy` entry). Every
 * payload that states the pair takes it from here, and from the reading it
 * serves beside it: two readings minutes apart put "Ø 25 min" on the park page
 * and "20 min" on the favourites card of the same park at the same moment,
 * and a mean over every ride put "Ø 10 min" on the country list beside both.
 */
export function liveHeadline(occupancy: {
  current: number;
  crowdLevel?: CrowdLevel;
  breakdown?: { currentAvgWait?: number };
}): { avgWaitTime: number; crowdLevel: CrowdLevel } {
  return {
    avgWaitTime: occupancy.breakdown?.currentAvgWait || 0,
    // The gated rating: re-deriving it from `current` bypasses the
    // ratability check ("unknown" for a thin park). The fallback is for an
    // entry written before the field existed.
    crowdLevel: occupancy.crowdLevel ?? determineCrowdLevel(occupancy.current),
  };
}

/**
 * Rate a numerator against a typical-day-peak baseline, returning `unknown`
 * ("keine Prognose") when the baseline is missing/non-positive — which on the
 * typical-day-peak surfaces means the park is not ratable (NULL typicalDayPeak,
 * i.e. < 30 operating days). Replaces the old
 * `baseline > 0 ? determineCrowdLevel((num / baseline) * 100) : "moderate"`
 * pattern so thin-data parks never render a made-up `moderate`.
 */
export function rateOrUnknown(numerator: number, baseline: number): CrowdLevel {
  if (!baseline || baseline <= 0) return "unknown";
  return determineCrowdLevel((numerator / baseline) * 100);
}
