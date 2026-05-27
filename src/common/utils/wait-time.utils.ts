/**
 * Wait Time Utility Functions
 *
 * Utilities for rounding and formatting wait times consistently across the application.
 */

/**
 * Upper bound for a plausible real wait time, in minutes. Values above this are
 * treated as data-source sentinels (e.g. 700, 999, 1000) that some feeds emit
 * for "closed / unavailable" rather than real queue lengths. The longest real
 * wait observed in queue_data is 360 min, with a clean gap up to ~420 where the
 * sentinels start, so 400 safely keeps every genuine value and drops the junk.
 * Used to stop sentinels from inflating accuracy/MAE aggregates.
 *
 * Kept in sync with the ML training set: ml-service/train.py remove_anomalies()
 * drops waitTime >= 400 as erroneous, so the live accuracy/drift population
 * matches the population the model was trained on. (Filters here keep <= 400
 * while training drops >= 400, so the value exactly 400 differs by one row —
 * irrelevant in practice: the real data has a gap between 360 and 420.)
 */
export const MAX_PLAUSIBLE_WAIT_TIME = 400;

/**
 * Round wait time to nearest 5 minutes for UX consistency
 *
 * Theme parks typically display wait times in 5-minute increments.
 * This provides better user experience and consistency with actual queue displays.
 *
 * @param value - Raw wait time value (any number)
 * @returns Rounded integer in 5-minute increments
 *
 * @example
 * ```ts
 * roundToNearest5Minutes(7.2)  // Returns 5
 * roundToNearest5Minutes(8.9)  // Returns 10
 * roundToNearest5Minutes(12.4) // Returns 10
 * roundToNearest5Minutes(34.7)  // Returns 35
 * roundToNearest5Minutes(0.5)  // Returns 0
 * ```
 */
export function roundToNearest5Minutes(value: number): number {
  // Coerce in case Postgres NUMERIC arrives as a string (`$x::numeric as foo`
  // is serialized by node-postgres as string, and the `+ 2.5` below would
  // otherwise concatenate — turning "45" + 2.5 = "452.5" → 450 instead of 45).
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n) || n < 2.5) {
    return 0; // Invalid input or very short wait → 0
  }

  // Add 2.5 and floor divide by 5, then multiply by 5
  // This ensures consistent rounding: 2.5→5, 7.5→10, 12.5→15
  return Math.floor((n + 2.5) / 5) * 5;
}
