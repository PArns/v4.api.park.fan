import type { OptimizerRide, PlannerDayPrefs } from "./types";

/** What a party's answers say about one ride. Neither ever hides a ride outside the headliner button. */
export interface PartyFlags {
  /** The shortest rider is under this ride's minimum. */
  tooShort: boolean;
  /** The party asked to stay dry and this ride may soak them. */
  wet: boolean;
}

const NONE: PartyFlags = { tooShort: false, wet: false };

/**
 * What this ride's own facts say about this party. Only the minimum height is asked, and a missing
 * limit means nobody wrote one down. Ported from park.fan's `lib/planner/party.ts`.
 */
export function partyFlags(
  ride: Pick<OptimizerRide, "minimumHeight" | "mayGetWet">,
  prefs: PlannerDayPrefs | undefined,
): PartyFlags {
  if (!prefs) return NONE;
  const tooShort =
    prefs.riderHeightCm !== undefined &&
    ride.minimumHeight != null &&
    prefs.riderHeightCm < ride.minimumHeight;
  const wet = prefs.avoidWet === true && ride.mayGetWet === true;
  if (!tooShort && !wet) return NONE;
  return { tooShort, wet };
}
