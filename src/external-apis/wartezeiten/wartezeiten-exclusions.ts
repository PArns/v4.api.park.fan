/**
 * Wartezeiten Park Exclusions
 *
 * Parks to exclude from Wartezeiten.app data source.
 * These are typically:
 * - Temporary events (Halloween, Christmas, etc.)
 * - Test parks
 * - Duplicate entries
 *
 * Parks listed here will be filtered out during fetchAllParks()
 */
export const WARTEZEITEN_EXCLUDED_PARKS: string[] = [
  // Temporary Halloween event in Germany
  // "Traumatica", // Re-enabled per user request
  // "traumatica",
  // Add more exclusions here as needed
];

/**
 * Check if a park name should be excluded
 */
export function isWartezeitenParkExcluded(parkName: string): boolean {
  const normalized = parkName.toLowerCase().trim();
  return WARTEZEITEN_EXCLUDED_PARKS.some(
    (excluded) => excluded.toLowerCase() === normalized,
  );
}
