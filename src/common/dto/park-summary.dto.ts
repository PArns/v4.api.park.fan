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
  /**
   * Whether the parent park is currently operating.
   *
   * Optional because {@link mapParkSummary} cannot know it — park status comes from the live
   * queue snapshot, not the park row. Callers that already hold it fill it in afterwards; the
   * attraction-detail endpoint does, shows and restaurants do not.
   *
   * It exists so a client rendering ONE ride does not have to fetch the whole park just to learn
   * whether it is open. The rule "a closed park closes its rides" needs exactly this field, and
   * the frontend was pulling a ~90 KB park payload every 5 minutes to get it (plus `timezone`,
   * which was already here) next to the attraction payload it fetches anyway.
   */
  status?: "OPERATING" | "CLOSED";
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
