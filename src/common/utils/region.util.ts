/**
 * Region Code Utilities
 *
 * Centralized functions for normalizing and comparing region codes.
 * Handles both short codes (e.g., "NW") and full ISO codes (e.g., "DE-NW").
 *
 * Note: Some geocoding APIs return non-ISO codes for certain regions.
 * We normalize these to ISO 3166-2 codes for consistent matching with holidays.
 */

/**
 * Region code aliases for non-ISO codes returned by Google Geocoding API.
 * Maps non-standard -> ISO 3166-2 standard codes.
 *
 * Example: Google returns "NRW" for Nordrhein-Westfalen, but ISO 3166-2 uses "NW"
 */
const REGION_ALIASES: Record<string, string> = {
  // Germany (DE)
  NRW: "NW", // Nordrhein-Westfalen
  NDS: "NI", // Niedersachsen
};

/**
 * Normalizes a region code by:
 * 1. Extracting the region part (after last "-")
 * 2. Applying alias mappings for non-ISO codes
 *
 * This ensures consistent comparison between different region code formats:
 * - "DE-NW" -> "NW"
 * - "NRW" -> "NW" (alias applied)
 * - "NDS" -> "NI" (alias applied)
 * - "US-FL" -> "FL"
 * - null/undefined -> null
 *
 * @param code - Region code (can be "DE-NW", "NW", "NRW", or null/undefined)
 * @returns Normalized region code (e.g., "NW") or null
 *
 * @example
 * normalizeRegionCode("DE-NW") // "NW"
 * normalizeRegionCode("NRW") // "NW" (alias)
 * normalizeRegionCode("NDS") // "NI" (alias)
 * normalizeRegionCode("US-FL") // "FL"
 * normalizeRegionCode(null) // null
 */
export function normalizeRegionCode(
  code: string | null | undefined,
): string | null {
  if (!code) return null;
  // Extract short code (part after last "-") or use as-is
  const shortCode = code.includes("-") ? code.split("-").pop() || code : code;
  // Apply aliases for known non-ISO codes
  return REGION_ALIASES[shortCode] || shortCode;
}
