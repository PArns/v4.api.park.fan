import { getFrontendUrl } from "../../config/oauth.config";
import {
  frontendAttractionPath,
  frontendParkPath,
} from "../../common/utils/frontend-url.util";
import type { GeocodedEntity } from "../../common/utils/url.util";

/** The park's page on park.fan, so an answer can link where a person looks. */
export function parkPageUrl(park: GeocodedEntity): string | null {
  const path = frontendParkPath(park);
  return path ? `${getFrontendUrl()}${path}` : null;
}

export function attractionPageUrl(
  park: GeocodedEntity,
  attraction: { slug: string },
): string | null {
  const path = frontendAttractionPath(park, attraction);
  return path ? `${getFrontendUrl()}${path}` : null;
}
