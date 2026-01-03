/**
 * Unified Crowd Level Type
 *
 * Standard 6-level crowd rating used across all park and attraction endpoints.
 *
 * Park Levels (based on occupancy %):
 * - very_low: ≤ 20% occupancy
 * - low: 21-40% occupancy
 * - moderate: 41-70% occupancy
 * - high: 71-90% occupancy
 * - very_high: 91-120% occupancy
 * - extreme: > 120% occupancy (park beyond capacity)
 *
 * Attraction Levels (based on waitTime/P90 ratio OR absolute minutes):
 * - very_low: ≤ 0.25 ratio (or ≤ 10 min)
 * - low: 0.26-0.50 ratio (or 11-20 min)
 * - moderate: 0.51-0.75 ratio (or 21-45 min)
 * - high: 0.76-1.0 ratio (or 46-75 min)
 * - very_high: 1.01-1.5 ratio (or 76-120 min)
 * - extreme: > 1.5 ratio (or > 120 min)
 */
export type CrowdLevel =
  | "very_low"
  | "low"
  | "moderate"
  | "high"
  | "very_high"
  | "extreme";
