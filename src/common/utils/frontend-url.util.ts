import type { GeocodedEntity } from "./url.util";

/**
 * Paths on the FRONTEND site (park.fan), not this API.
 *
 * `url.util.ts` builds this API's own `/v1/parks/...` paths, which is a
 * different shape — a ride there is `.../attractions/<slug>`, on the site it
 * is the park's path with the slug appended directly, no segment in between
 * (confirmed against the frontend's own `lib/utils/url-utils.ts`). Unprefixed
 * by locale on purpose, matching `notification-planner.ts`'s existing
 * `url: "/"` for trip notifications: the frontend's own i18n routing
 * (`proxy.ts`) resolves a bare path to the visitor's locale on the click,
 * so nothing here needs to guess it.
 */

/** Null when any geo slug is missing — same guard as `buildGeocodedUrl`. */
export function frontendParkPath(park: GeocodedEntity): string | null {
  if (!park.continentSlug || !park.countrySlug || !park.citySlug) return null;
  return `/parks/${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}`;
}

export function frontendAttractionPath(
  park: GeocodedEntity,
  attraction: { slug: string },
): string | null {
  const base = frontendParkPath(park);
  return base ? `${base}/${attraction.slug}` : null;
}

/** The park's shows tab — there is no per-show page on the site. */
export function frontendShowsPath(park: GeocodedEntity): string | null {
  const base = frontendParkPath(park);
  return base ? `${base}#shows` : null;
}
