import { Park } from "../../parks/entities/park.entity";

/**
 * Compact parent-park block embedded in attraction/show/restaurant
 * response DTOs. One shape + one mapper instead of three copies.
 */
export interface ParkSummaryDto {
  id: string;
  name: string;
  slug: string;
  timezone: string;
  continent: string | null;
  country: string | null;
  city: string | null;
}

export function mapParkSummary(
  park: Park | null | undefined,
): ParkSummaryDto | null {
  if (!park) return null;
  return {
    id: park.id,
    name: park.name,
    slug: park.slug,
    timezone: park.timezone,
    continent: park.continent || null,
    country: park.country || null,
    city: park.city || null,
  };
}
