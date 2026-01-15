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

/**
 * Build geocoded URL for show within a park
 *
 * @param park - Park entity with geocoded slugs
 * @param show - Show entity with slug
 * @returns Geocoded URL or null
 */
export function buildShowUrl(
  park: GeocodedEntity,
  show: { slug: string },
): string | null {
  // Validate all required slugs are present
  if (!park.continentSlug || !park.countrySlug || !park.citySlug) {
    return null;
  }

  const basePath = `/v1/parks/${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}`;
  return `${basePath}/shows/${show.slug}`;
}

/**
 * Build geocoded URL for restaurant within a park
 *
 * @param park - Park entity with geocoded slugs
 * @param restaurant - Restaurant entity with slug
 * @returns Geocoded URL or null
 */
export function buildRestaurantUrl(
  park: GeocodedEntity,
  restaurant: { slug: string },
): string | null {
  // Validate all required slugs are present
  if (!park.continentSlug || !park.countrySlug || !park.citySlug) {
    return null;
  }

  const basePath = `/v1/parks/${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}`;
  return `${basePath}/restaurants/${restaurant.slug}`;
}

/**
 * Build discovery URL for a country
 *
 * Example: `/discovery/europe/germany`
 *
 * @param continentSlug - Continent slug
 * @param countrySlug - Country slug
 * @returns Discovery URL
 */
export function buildCountryDiscoveryUrl(
  continentSlug: string,
  countrySlug: string,
): string {
  return `/discovery/${continentSlug}/${countrySlug}`;
}

/**
 * Build discovery URL for a city
 *
 * Example: `/discovery/europe/germany/bruehl`
 *
 * @param continentSlug - Continent slug
 * @param countrySlug - Country slug
 * @param citySlug - City slug
 * @returns Discovery URL
 */
export function buildCityDiscoveryUrl(
  continentSlug: string,
  countrySlug: string,
  citySlug: string,
): string {
  return `/discovery/${continentSlug}/${countrySlug}/${citySlug}`;
}
