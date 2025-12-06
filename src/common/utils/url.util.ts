/**
 * URL generation utilities for API paths
 */

/**
 * Interface for entities with geocoded slugs
 */
export interface GeocodedEntity {
  continentSlug?: string | null;
  countrySlug?: string | null;
  citySlug?: string | null;
  slug: string;
}

/**
 * Build geocoded URL path for parks and attractions
 *
 * Returns null if any required slug is missing (continent, country, city)
 *
 * Examples:
 * - Park: `/v1/parks/europe/germany/bruhl/phantasialand`
 * - Attraction: `/v1/parks/europe/germany/bruhl/phantasialand/attractions/taron`
 *
 * @param entity - Entity with geocoded slug fields
 * @param attractionSlug - Optional attraction slug (for nested attraction URLs)
 * @returns Geocoded URL path or null if slugs are incomplete
 */
export function buildGeocodedUrl(
  entity: GeocodedEntity,
  attractionSlug?: string,
): string | null {
  // Validate all required slugs are present
  if (!entity.continentSlug || !entity.countrySlug || !entity.citySlug) {
    return null;
  }

  const basePath = `/v1/parks/${entity.continentSlug}/${entity.countrySlug}/${entity.citySlug}/${entity.slug}`;

  // If attraction slug provided, append it
  if (attractionSlug) {
    return `${basePath}/attractions/${attractionSlug}`;
  }

  return basePath;
}

/**
 * Build geocoded URL for park by combining park and attraction slugs
 *
 * @param park - Park entity with geocoded slugs
 * @param attraction - Optional attraction entity
 * @returns Geocoded URL or null
 */
export function buildParkUrl(park: GeocodedEntity): string | null {
  return buildGeocodedUrl(park);
}

/**
 * Build geocoded URL for attraction within a park
 *
 * @param park - Park entity with geocoded slugs
 * @param attraction - Attraction entity with slug
 * @returns Geocoded URL or null
 */
export function buildAttractionUrl(
  park: GeocodedEntity,
  attraction: { slug: string },
): string | null {
  return buildGeocodedUrl(park, attraction.slug);
}
