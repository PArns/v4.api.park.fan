/**
 * Region Code Utilities
 *
 * Centralized functions for normalizing and comparing region codes.
 * Handles both short codes (e.g., "NW") and full ISO codes (e.g., "DE-NW").
 */

/**
 * Normalizes a region code by extracting the region part (after last "-").
 *
 * This ensures consistent comparison between different region code formats:
 * - "DE-NW" -> "NW"
 * - "NW" -> "NW"
 * - "US-FL" -> "FL"
 * - null/undefined -> null
 *
 * @param code - Region code (can be "DE-NW", "NW", or null/undefined)
 * @returns Normalized region code (e.g., "NW") or null
 *
 * @example
 * normalizeRegionCode("DE-NW") // "NW"
 * normalizeRegionCode("NW") // "NW"
 * normalizeRegionCode("US-FL") // "FL"
 * normalizeRegionCode(null) // null
 */
export function normalizeRegionCode(
  code: string | null | undefined,
): string | null {
  if (!code) return null;
  // If code contains "-", extract the part after the last "-" (e.g., "DE-NW" -> "NW")
  // Otherwise use the code as-is (e.g., "NW" -> "NW")
  return code.includes("-") ? code.split("-").pop() || code : code;
}
