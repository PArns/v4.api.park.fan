/**
 * Unified Crowd Level Type
 *
 * Standard 6-level crowd rating used across all park and attraction endpoints.
 *
 * Levels:
 * - very_low: < 30% occupancy
 * - low: 30-50% occupancy
 * - moderate: 50-75% occupancy
 * - high: 75-95% occupancy
 * - very_high: 95-110% occupancy
 * - extreme: > 110% occupancy (park at or beyond capacity)
 */
export type CrowdLevel =
  | "very_low"
  | "low"
  | "moderate"
  | "high"
  | "very_high"
  | "extreme";
