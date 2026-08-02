import { calculateHaversineDistance } from "../../common/utils/distance.util";

/** The two rows a merge is about, reduced to what this decision needs. */
export interface MergeGeoCandidate {
  latitude?: number | null;
  longitude?: number | null;
}

/**
 * How far apart two rows may sit and still be believed to be one park.
 *
 * Generous on purpose. This is not a duplicate test — the detector already
 * decided these are the same park, and it works to a kilometre. This is the
 * last check before one row's *identity* is copied onto another, so it only has
 * to recognise "these are not the same place at all". Ten kilometres clears
 * every geocoding wobble we see in practice and still separates Orlando from
 * Tampa.
 */
export const MAX_INHERIT_DISTANCE_KM = 10;

/**
 * May the winner inherit the loser's upstream ids?
 *
 * A merge fills the winner's empty source ids from the loser, which is how a
 * park keeps its feeds when the row carrying them disappears. The danger is
 * that an id is not a fact about the loser — it is a claim about which park the
 * *upstream* is describing. Copy it onto a row it does not belong to and the
 * park is wired to someone else's data, silently: the sync then no longer finds
 * the real park under its own id and creates a second row for it days later.
 *
 * That is not hypothetical. Islands of Adventure in Orlando spent three days
 * carrying `qt-park-97`, which Queue-Times uses for Adventure Island in Tampa,
 * a hundred kilometres away, inherited from a row that sat in Tampa. A new
 * Islands of Adventure row appeared two days later because the sync could not
 * find the park under its real id.
 *
 * So: when both rows state coordinates and those coordinates describe different
 * places, the ids are not inherited. The cost of refusing is that a park may
 * lose a feed and the next sync creates a row for it — visible, and repairable
 * with a merge. The cost of accepting is a park quietly serving another park's
 * wait times. Refusing is the cheaper mistake.
 *
 * Missing coordinates are not evidence of anything, so inheritance proceeds —
 * this check only ever fires on a positive contradiction.
 */
export function canInheritSourceIds(
  winner: MergeGeoCandidate,
  loser: MergeGeoCandidate,
  maxDistanceKm: number = MAX_INHERIT_DISTANCE_KM,
): { allowed: boolean; distanceKm: number | null } {
  const winnerGeo = toCoordinate(winner);
  const loserGeo = toCoordinate(loser);
  if (!winnerGeo || !loserGeo) return { allowed: true, distanceKm: null };

  const distanceKm = calculateHaversineDistance(winnerGeo, loserGeo, "km");
  return {
    allowed: distanceKm <= maxDistanceKm,
    distanceKm: Math.round(distanceKm * 10) / 10,
  };
}

/**
 * A usable coordinate pair, or null.
 *
 * `0, 0` is Null Island — a park that failed geocoding, not a park in the Gulf
 * of Guinea. Treating it as a real position would put every unlocated row
 * thousands of kilometres from its partner and block inheritance for the one
 * case where we have no information at all.
 */
function toCoordinate(
  candidate: MergeGeoCandidate,
): { latitude: number; longitude: number } | null {
  const { latitude, longitude } = candidate;
  if (typeof latitude !== "number" || typeof longitude !== "number")
    return null;
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
  if (latitude === 0 && longitude === 0) return null;
  return { latitude, longitude };
}
