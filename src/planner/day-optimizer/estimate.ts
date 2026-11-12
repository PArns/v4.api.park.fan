import {
  DEFAULT_OCCUPIED_MINUTES,
  earlyEntryOpenMin,
  opensEarly,
  unfoldedCloseHour,
} from "./day-grid";
import { isQuarterGap, rideWaitAt } from "./ride-series";
import type {
  OptimizerDay,
  OptimizerRide,
  OptimizerTier,
  OptimizerWorksPeriod,
  PlannerEntry,
} from "./types";

/**
 * What a planned entry is expected to cost: the join between a plan and `/plan/day`. Ported from
 * park.fan's `lib/planner/estimate.ts`, which still draws the blocks with the same rules; the two
 * must answer the same minutes, or the optimizer plans a day the grid draws differently.
 */

/** The expected wait for one entry, and why there is none where there is none. */
export interface PlannerEstimate {
  /** Expected wait in minutes at the planned quarter hour, or null when unknown. */
  wait: number | null;
  /** Half-width of the model's band; null means no spread was reported. */
  uncertaintyMinutes: number | null;
  /** The typical error of this ride's own numbers, or null where unmeasured. */
  expectedError: number | null;
  /** Which regime this hour's figure came from. */
  tier: OptimizerTier | null;
  missing:
    | "none"
    | "assumed"
    | "early-entry"
    | "no-day"
    | "no-curve"
    | "no-source"
    | "outside-hours"
    | "works"
    | "custom";
}

/**
 * What a ride with no curve is taken to cost, so it is not counted as zero in every total: the
 * shortest queue a park can post.
 */
export const ASSUMED_WAIT_MIN = 5;

const UNKNOWN: PlannerEstimate = {
  wait: null,
  uncertaintyMinutes: null,
  expectedError: null,
  tier: null,
  missing: "no-day",
};

/**
 * Whether anybody can read this park's wait times. An absent flag reads as readable; `false`
 * means no number will ever arrive.
 */
export function hasReadableWaitTimes(
  context: OptimizerDay["context"] | null | undefined,
): boolean {
  const live = context?.liveWaitTimes;
  return !live || live.available;
}

function withoutFigure(
  day: OptimizerDay,
  missing: PlannerEstimate["missing"],
): PlannerEstimate {
  return {
    wait: null,
    uncertaintyMinutes: null,
    expectedError: null,
    tier: day.tier ?? null,
    missing,
  };
}

function assumed(
  day: OptimizerDay,
  missing: "assumed" | "early-entry" = "assumed",
): PlannerEstimate {
  return {
    wait: ASSUMED_WAIT_MIN,
    uncertaintyMinutes: null,
    expectedError: null,
    tier: day.tier ?? null,
    missing,
  };
}

function rideOf(day: OptimizerDay, slug: string): OptimizerRide | undefined {
  return day.rides.find((r) => r.attractionSlug === slug);
}

/** The works window that closes a ride on the planned day, or null. */
export function worksPeriodFor(
  day: OptimizerDay | null | undefined,
  slug: string,
): OptimizerWorksPeriod | null {
  return (
    day?.ridesInWorks?.find((ride) => ride.attractionSlug === slug)
      ?.worksPeriod ?? null
  );
}

/** The expected wait for one planned entry. */
export function estimateFor(
  day: OptimizerDay | null | undefined,
  entry: PlannerEntry,
): PlannerEstimate {
  if (!day) return UNKNOWN;

  const { openHour, closeHour } = day.context;
  if (openHour === null || closeHour === null)
    return withoutFigure(day, "no-day");
  const hour = Math.floor(entry.startMinute / 60);

  // Before the gates on an early-entry day, for a ride that opens early: the assumption, marked.
  if (hour < openHour && !entry.custom) {
    const early = earlyEntryOpenMin(day.context);
    if (
      early !== null &&
      hour >= Math.floor(early / 60) &&
      entry.attractionSlug &&
      opensEarly(rideOf(day, entry.attractionSlug), openHour * 60)
    ) {
      return assumed(day, "early-entry");
    }
  }

  if (hour < openHour || hour > unfoldedCloseHour(openHour, closeHour)) {
    return withoutFigure(day, "outside-hours");
  }

  // A free block is a duration the visitor wrote down, not a queue anybody predicted.
  if (entry.custom) return withoutFigure(day, "custom");

  const ride = entry.attractionSlug
    ? rideOf(day, entry.attractionSlug)
    : undefined;
  const readable = hasReadableWaitTimes(day.context);

  if (
    !ride &&
    entry.attractionSlug &&
    worksPeriodFor(day, entry.attractionSlug)
  ) {
    return withoutFigure(day, "works");
  }
  if (!ride) return readable ? assumed(day) : withoutFigure(day, "no-source");

  const point = rideWaitAt(ride, entry.startMinute);
  // A quarter missing inside an hour that has slots is the ride not running then.
  if (!point && isQuarterGap(ride, entry.startMinute)) {
    return withoutFigure(day, "outside-hours");
  }
  if (!point) return readable ? assumed(day) : withoutFigure(day, "no-source");

  const lookBack = day.tier === "climatology";
  return {
    wait: point.wait,
    uncertaintyMinutes: lookBack ? null : (ride.uncertaintyMinutes ?? null),
    expectedError: lookBack ? null : (ride.expectedError ?? null),
    tier: point.source ?? day.tier ?? null,
    missing: "none",
  };
}

/**
 * The two halves of a block's length: what the entry is expected to cost, and the model's spread
 * on top, which only a forecast has.
 */
function spanParts(
  day: OptimizerDay | null | undefined,
  entry: PlannerEntry,
  fallback: number,
): { planned: number; band: number } {
  if (entry.custom) return { planned: entry.custom.durationMinutes, band: 0 };
  if (entry.done) return { planned: entry.actualWait ?? fallback, band: 0 };
  const estimate = estimateFor(day, entry);
  if (estimate.wait === null) return { planned: fallback, band: 0 };
  return { planned: estimate.wait, band: estimate.uncertaintyMinutes ?? 0 };
}

/** How long an entry is drawn: the forecast plus its band. */
export function occupiedMinutes(
  day: OptimizerDay | null | undefined,
  entry: PlannerEntry,
  fallback = DEFAULT_OCCUPIED_MINUTES,
): number {
  const { planned, band } = spanParts(day, entry, fallback);
  return planned + band;
}

/**
 * The same length with the band left off, which is what a plan is built on: scheduling all of it
 * would pace the day off the pessimistic end of every queue.
 */
export function plannedMinutes(
  day: OptimizerDay | null | undefined,
  entry: PlannerEntry,
  fallback = DEFAULT_OCCUPIED_MINUTES,
): number {
  return spanParts(day, entry, fallback).planned;
}
