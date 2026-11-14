import type {
  ParkSeasonKind,
  ParkSeasonStatus,
} from "../../parks/entities/park-season.entity";

/**
 * A park season that binds a ride: one of `PARK_SEASON_BINDING_KINDS`, not
 * `cancelled`, naming the ride in `attractionIds`. Its days are the ride's
 * operating window.
 *
 * The fields a reader needs to say "only during Wintertraum, 20 Nov – 18 Jan",
 * in the shape of the public season (`/v1/parks/…/seasons`): park-local
 * `YYYY-MM-DD` bounds, both inclusive, and `dates` null for every day between
 * them.
 */
export interface SeasonBinding {
  id: string;
  kind: ParkSeasonKind;
  /** The park's own name for the season; null falls back to the kind's label. */
  name: string | null;
  status: ParkSeasonStatus;
  startDate: string;
  endDate: string;
  dates: string[] | null;
}
