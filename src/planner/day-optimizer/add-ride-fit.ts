import { gridForDay, withEarlyEntry } from "./day-grid";
import {
  addWishKey,
  fitBlocks,
  fitChoiceAll,
  fitWishes,
  needsFitHelp,
  togglePin,
  type FitChoice,
  type FitInput,
  type FitWish,
} from "./fit";
import { canOptimize } from "./optimize";
import type {
  DayClock,
  OptimizerDay,
  OptimizerRide,
  PlannerEntry,
} from "./types";

/**
 * The wishes of a fit question about adding `rides` to a day: {@link fitWishes}, plus a second go
 * on a ride the day already holds, under its `a:<slug>` key, since `fitWishes` drops repeats. For
 * the headliners the two are the same list, because those are never already planned.
 */
export function wishesWithAdditions(
  day: OptimizerDay,
  entries: readonly PlannerEntry[],
  rides: readonly OptimizerRide[],
  clock?: DayClock,
): FitWish[] {
  const wishes = fitWishes(day, entries, rides, clock);
  for (const ride of rides) {
    const key = addWishKey(ride.attractionSlug);
    if (wishes.some((wish) => wish.key === key)) continue;
    wishes.push({
      key,
      entryId: null,
      attractionSlug: ride.attractionSlug,
      attractionName: ride.attractionName,
      ride,
      headliner: Boolean(ride.isHeadliner),
    });
  }
  return wishes;
}

/**
 * The question a press on "add to plan" raises, or `null` where it raises none: the day plus this
 * ride, and whether every wish gets a slot before closing. Ported from park.fan's
 * `lib/planner/add-ride-fit.ts`; `null` wherever the question cannot be asked honestly: no payload,
 * no hours, no readable waits, a walked day, or a ride the payload does not list.
 */
export function noRoomForRide(params: {
  day: OptimizerDay | null | undefined;
  entries: readonly PlannerEntry[];
  attractionSlug: string;
  clock: DayClock;
  earlyEntry?: boolean;
}): FitInput | null {
  const { entries, attractionSlug, clock } = params;
  const day = withEarlyEntry(params.day, params.earlyEntry);
  if (!day || clock.phase === "past") return null;
  const ride = day.rides.find(
    (candidate) => candidate.attractionSlug === attractionSlug,
  );
  if (!ride) return null;
  const grid = gridForDay(day);
  if (!grid || !canOptimize(day, grid)) return null;

  const input: FitInput = {
    day,
    grid,
    entries,
    wishes: wishesWithAdditions(day, entries, [ride], clock),
    blocks: fitBlocks(entries),
    clock,
  };
  return needsFitHelp(input, fitChoiceAll()) ? input : null;
}

/**
 * The answer the assistant opens on after "add to plan": everything ticked and the pressed ride
 * pinned, so the proposed plan cannot leave it out.
 */
export function requestedRideChoice(attractionSlug: string): FitChoice {
  return togglePin(fitChoiceAll(), addWishKey(attractionSlug));
}
