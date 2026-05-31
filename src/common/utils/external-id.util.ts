/**
 * External ID Utility Functions
 *
 * Helpers for parsing/normalizing external entity IDs across data sources.
 */

/**
 * Extract numeric Queue-Times ID from external ID
 *
 * Examples:
 * - "qt-ride-8" -> "8"
 * - "qt-park-56" -> "56"
 * - "8" -> "8" (already numeric)
 *
 * @param externalId - The external ID to parse
 * @returns The numeric id as a string, or null if it cannot be derived
 */
export function extractQueueTimesNumericId(externalId: string): string | null {
  if (!externalId) return null;

  // Handle prefixed IDs like "qt-ride-8" or "qt-park-56"
  if (externalId.startsWith("qt-ride-")) {
    return externalId.replace("qt-ride-", "");
  }
  if (externalId.startsWith("qt-park-")) {
    return externalId.replace("qt-park-", "");
  }

  // If already numeric, return as-is
  if (/^\d+$/.test(externalId)) {
    return externalId;
  }

  return null;
}
