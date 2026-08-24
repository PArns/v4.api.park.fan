import { Park } from "../../parks/entities/park.entity";
import {
  LiveWaitTimesDto,
  buildLiveWaitTimes,
} from "../../parks/dto/live-wait-times.dto";
import { resolveCuratedPark } from "../../parks/utils/curated-park-facts.util";

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

  /**
   * Whether the parent park's wait times are readable at all.
   *
   * Not optional and not filled in later, because unlike {@link status} it is a property of the
   * park row rather than of the live snapshot — the mapper can always answer it. A ride page
   * needs it for the same reason the park page does: with no source, "no wait time" reads as a
   * walk-on instead of as an absence, and the ride's own forecast has nothing behind it.
   */
  liveWaitTimes: LiveWaitTimesDto;
}

export function mapParkSummary(
  park: Park | null | undefined,
): ParkSummaryDto | null {
  if (!park) return null;
  const curated = resolveCuratedPark(park);
  return {
    id: park.id,
    name: curated.name,
    slug: park.slug,
    timezone: park.timezone,
    continent: park.continent || null,
    country: park.country || null,
    city: park.city || null,
    liveWaitTimes: buildLiveWaitTimes(curated.noWaitTimesReason),
  };
}
