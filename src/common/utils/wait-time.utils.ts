/**
 * Wait Time Utility Functions
 *
 * Utilities for rounding and formatting wait times consistently across the application.
 */

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
  if (value < 2.5) {
    return 0; // Very short wait → 0
  }

  // Add 2.5 and floor divide by 5, then multiply by 5
  // This ensures consistent rounding: 2.5→5, 7.5→10, 12.5→15
  return Math.floor((value + 2.5) / 5) * 5;
}
