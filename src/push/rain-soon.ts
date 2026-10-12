import type { IndoorOutdoor } from "../common/types/indoor-outdoor.type";

/**
 * What the nowcast and the park's rides together say is worth a `rain-soon` push.
 *
 * Pure, like `notification-planner.ts`: the thing that decides whether somebody's
 * phone buzzes about weather is tested against a nowcast and a timestamp, not
 * against a running queue.
 *
 * The covered-ride half mirrors the frontend's `lib/utils/covered-rides.ts`
 * (`isCovered`, `coveredOfferReady`, `rankCoveredRides`, `COVERED_MIN_KNOWN_SHARE`)
 * and the trigger mirrors `lib/utils/nowcast-shelter.ts` (`SHELTER_LEAD_MINUTES`).
 * The rule is written down here once because a push has no browser to ask; the
 * spec runs the same cases as `covered-rides.test` there. A change to the rule
 * in one repository is a change to the other.
 */

/**
 * How far ahead rain makes the push go out — the frontend's
 * `SHELTER_LEAD_MINUTES`: about the time to cross a large park and join a queue.
 * Further out the forecast start moves more than that between updates.
 */
export const RAIN_LEAD_MIN = 30;

/** Share of in-season attractions that must carry an `indoorOutdoor` value. */
export const COVERED_MIN_KNOWN_SHARE = 0.5;

/** Rides named in the push. The banner lists four; a lock screen fits three. */
export const RAIN_PUSH_RIDE_LIMIT = 3;

/** The nowcast fields the decision reads. */
export interface RainNowcastInput {
  currentlyRaining: boolean;
  rainStartsAt: string | null;
  rainEndsAt: string | null;
}

/**
 * Where one park's rain front stands, as stored between ticks.
 *
 * - `warned` — a push went out for the front that is coming.
 * - `raining` — the rain is here (we warned, or it was already falling when we
 *   first looked). The park is re-armed only from this phase and only once it
 *   has stopped.
 */
export type RainPhase = "warned" | "raining";

export type RainDecision =
  | { action: "send"; startsAtMs: number }
  | { action: "mark-raining" }
  | { action: "rearm" }
  | { action: "none" };

/**
 * One tick's verdict for one park.
 *
 * `phase` is the stored state, `null` when there is none. The sequence is
 * dry -> warned -> raining -> dry (re-armed), so a second front on the same day
 * warns again and a flickering forecast does not: a warned park whose rain
 * disappears from the forecast stays warned until the marker expires, because
 * re-arming on "the forecast no longer shows rain" would send a second push the
 * minute the forecast shows it again.
 *
 * Rain that is already falling is not "soon". No push goes out for it; the park
 * is marked as raining so the end of that rain re-arms it.
 */
export function decideRainSoon(
  nowcast: RainNowcastInput,
  phase: RainPhase | null,
  nowMs: number,
): RainDecision {
  const startsMs = nowcast.rainStartsAt
    ? Date.parse(nowcast.rainStartsAt)
    : NaN;
  const endsMs = nowcast.rainEndsAt ? Date.parse(nowcast.rainEndsAt) : NaN;
  const startsInFuture = !Number.isNaN(startsMs) && startsMs > nowMs;
  const endsInFuture = !Number.isNaN(endsMs) && endsMs > nowMs;
  // The API sets `rainStartsAt` only while rain is still ahead, so a future
  // start means it is not raining yet however far off the end is (the
  // frontend's `isRainingNow`).
  const raining = !startsInFuture && (nowcast.currentlyRaining || endsInFuture);

  if (raining)
    return phase === "raining"
      ? { action: "none" }
      : { action: "mark-raining" };

  if (phase === "raining") return { action: "rearm" };
  if (phase === "warned") return { action: "none" };

  if (startsInFuture && startsMs - nowMs <= RAIN_LEAD_MIN * 60_000) {
    return { action: "send", startsAtMs: startsMs };
  }
  return { action: "none" };
}

/** The fields of an attraction the gate reads. */
export interface CoveredGateInput {
  indoorOutdoor: IndoorOutdoor | null;
  /** The resolved answer; only a hard `false` leaves a ride out. */
  isCurrentlyInSeason: boolean | null;
}

/** Indoors, or a roofed queue. Absent is unknown and never counts. */
export function isCovered(attraction: {
  indoorOutdoor: IndoorOutdoor | null;
}): boolean {
  return (
    attraction.indoorOutdoor === "indoor" ||
    attraction.indoorOutdoor === "covered_queue"
  );
}

/**
 * Whether the park knows enough to name covered rides at all: at least
 * {@link COVERED_MIN_KNOWN_SHARE} of its in-season attractions carry a value and
 * at least one is covered. Below it the list is whatever somebody happened to
 * check, and a reader takes a short list for a complete one.
 */
export function coveredOfferReady(
  attractions: readonly CoveredGateInput[],
): boolean {
  let total = 0;
  let known = 0;
  let covered = 0;
  for (const a of attractions) {
    if (a.isCurrentlyInSeason === false) continue;
    total++;
    if (a.indoorOutdoor == null) continue;
    known++;
    if (isCovered(a)) covered++;
  }
  return total > 0 && covered > 0 && known / total >= COVERED_MIN_KNOWN_SHARE;
}

export interface CoveredCandidate extends CoveredGateInput {
  id: string;
  name: string;
  /** Whether the ride's latest standby reading says it is running. */
  operating: boolean;
  /** Live standby wait in minutes, or null when none is posted. */
  waitTime: number | null;
}

export interface CoveredRideLine {
  id: string;
  name: string;
  waitTime: number | null;
}

/**
 * Open, in-season, covered rides, shortest queue first.
 *
 * Unlike the frontend's `rankCoveredRides` there is no walking distance: a push
 * does not know where the visitor stands, and the frontend drops the walk
 * whenever one ride lacks a distance, which leaves the queue alone. A ride that
 * posts no wait sorts last, since an unknown queue is not a short one. Ties go
 * to the name.
 */
export function rankCoveredRides(
  candidates: readonly CoveredCandidate[],
  limit = RAIN_PUSH_RIDE_LIMIT,
): CoveredRideLine[] {
  return candidates
    .filter(
      (c) => c.operating && c.isCurrentlyInSeason !== false && isCovered(c),
    )
    .map((c) => ({
      id: c.id,
      name: c.name,
      waitTime:
        typeof c.waitTime === "number" && Number.isFinite(c.waitTime)
          ? c.waitTime
          : null,
    }))
    .sort((a, b) => {
      if (a.waitTime === null && b.waitTime !== null) return 1;
      if (b.waitTime === null && a.waitTime !== null) return -1;
      return (
        (a.waitTime ?? 0) - (b.waitTime ?? 0) || a.name.localeCompare(b.name)
      );
    })
    .slice(0, limit);
}
