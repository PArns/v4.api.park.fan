import { Park } from "../entities/park.entity";
import { buildParkUrl } from "../../common/utils/url.util";

/**
 * Park Response DTO
 *
 * Used for API responses when returning park data.
 */
export class ParkResponseDto {
  id: string;
  name: string;
  slug: string;
  url: string | null;
  timezone: string;
  latitude: number | null;
  longitude: number | null;
  continent: string | null;
  continentSlug: string | null;
  country: string | null;
  countrySlug: string | null;
  city: string | null;
  citySlug: string | null;
  // Destination field removed - was redundant with park name/slug
  status?: "OPERATING" | "CLOSED";
  isOpen?: boolean;

  static fromEntity(park: Park): ParkResponseDto {
    return {
      id: park.id,
      name: park.name,
      slug: park.slug,
      url: buildParkUrl(park),
      timezone: park.timezone,
      latitude: park.latitude !== undefined ? park.latitude : null,
      longitude: park.longitude !== undefined ? park.longitude : null,
      continent: park.continent || null,
      continentSlug: park.continentSlug || null,
      country: park.country || null,
      countrySlug: park.countrySlug || null,
      city: park.city || null,
      citySlug: park.citySlug || null,
      // Destination field removed
    };
  }
}
