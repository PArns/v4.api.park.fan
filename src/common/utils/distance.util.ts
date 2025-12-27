/**
 * Distance Utility Functions
 *
 * Provides geographic distance calculations using the Haversine formula.
 * Used for location-based features such as finding nearby parks and attractions.
 */

export interface GeoCoordinate {
  latitude: number;
  longitude: number;
}

/**
 * Calculate distance between two geographic coordinates using Haversine formula
 *
 * The Haversine formula determines the great-circle distance between two points
 * on a sphere given their longitudes and latitudes.
 *
 * @param point1 - First coordinate
 * @param point2 - Second coordinate
 * @param unit - Unit of measurement ('m' for meters, 'km' for kilometers)
 * @returns Distance in the specified unit
 *
 * @example
 * ```typescript
 * const distance = calculateHaversineDistance(
 *   { latitude: 48.266, longitude: 7.722 },
 *   { latitude: 48.270, longitude: 7.725 },
 *   'm'
 * );
 * console.log(distance); // ~500 meters
 * ```
 */
export function calculateHaversineDistance(
  point1: GeoCoordinate,
  point2: GeoCoordinate,
  unit: "m" | "km" = "m",
): number {
  const R = unit === "km" ? 6371 : 6371000; // Earth radius (km or meters)

  const dLat = toRadians(point2.latitude - point1.latitude);
  const dLon = toRadians(point2.longitude - point1.longitude);

  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRadians(point1.latitude)) *
      Math.cos(toRadians(point2.latitude)) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c;
}

/**
 * Convert degrees to radians
 */
function toRadians(degrees: number): number {
  return (degrees * Math.PI) / 180;
}

/**
 * Sort an array of items by distance from a reference point
 *
 * @param items - Array of items with latitude/longitude
 * @param referencePoint - Point to calculate distance from
 * @returns Sorted array with distance property added
 */
export function sortByDistance<T extends GeoCoordinate>(
  items: T[],
  referencePoint: GeoCoordinate,
): Array<T & { distance: number }> {
  return items
    .map((item) => ({
      ...item,
      distance: calculateHaversineDistance(referencePoint, item, "m"),
    }))
    .sort((a, b) => a.distance - b.distance);
}

/**
 * Filter items within a certain radius from a reference point
 *
 * @param items - Array of items with latitude/longitude
 * @param referencePoint - Point to calculate distance from
 * @param radiusInMeters - Maximum distance in meters
 * @returns Filtered array of items within radius
 */
export function filterByRadius<T extends GeoCoordinate>(
  items: T[],
  referencePoint: GeoCoordinate,
  radiusInMeters: number,
): T[] {
  return items.filter((item) => {
    const distance = calculateHaversineDistance(referencePoint, item, "m");
    return distance <= radiusInMeters;
  });
}
