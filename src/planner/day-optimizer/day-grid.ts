import type {
  DayClock,
  OptimizerContext,
  OptimizerDay,
  OptimizerRide,
} from "./types";
import { rideFirstMinute } from "./ride-series";

/**
 * The day's axis as the optimizer needs it: where the park opens, where it closes, and where the
 * early-entry rides open. Ported from park.fan's `lib/planner/day-grid.ts`; the pixel geometry
 * stayed in the browser, because nothing here draws.
 */

/**
 * How long one ride takes, boarding to the platform, for every ride alike: an assumption, since
 * `/plan/day` carries no duration. Spent in the transfer and once more after the last stop.
 */
export const RIDE_DURATION_MIN = 5;

/** The quarter hour every start the planner files lands on. */
export const SNAP_MIN_FINE = 15;

/** What a block with no measured length occupies: the planner's default for an unknown wait. */
export const DEFAULT_OCCUPIED_MINUTES = 45;

/** Days of measurement below which a missing early hour says more about sampling than the ride. */
export const SOFT_FLOOR_MIN_SAMPLE_DAYS = 30;

/**
 * How long after the gates open somebody can actually be queueing: turnstile, bag check and walk.
 * An allowance, so it raises the soft floor and never the hard one.
 */
export const GATE_TO_FIRST_RIDE_MIN = 15;

/** The park's hours in park-local minutes. */
export interface DayGrid {
  /** Park-local minutes since midnight. */
  openMin: number;
  /** The minute the park is known to be open until; past 1440 on a day past midnight. */
  closeMin: number;
  /** Where the early-entry rides open, below {@link openMin}, or `null`. */
  earlyEntryOpenMin: number | null;
}

/** The first minute anything on this day may be filed at. */
export function dayStartMin(grid: DayGrid): number {
  return grid.earlyEntryOpenMin ?? grid.openMin;
}

/**
 * Where the early-entry rides open, or `null`. Three facts must agree: the park offers early
 * entry, says how early, and the visitor holds it on this day.
 */
export function earlyEntryOpenMin(
  context:
    | Pick<
        OptimizerContext,
        "openHour" | "hasEarlyEntry" | "earlyEntryMinutesPeak" | "earlyEntry"
      >
    | null
    | undefined,
): number | null {
  if (!context || context.earlyEntry !== true || context.hasEarlyEntry !== true)
    return null;
  if (context.openHour === null || context.openHour === undefined) return null;
  const minutes = context.earlyEntryMinutesPeak;
  if (typeof minutes !== "number" || !Number.isFinite(minutes) || minutes <= 0)
    return null;
  return context.openHour * 60 - Math.round(minutes);
}

/**
 * The day with the visitor's early-entry answer folded into its context. The same object where
 * nothing changes.
 */
export function withEarlyEntry<T extends OptimizerDay | null | undefined>(
  day: T,
  confirmed: boolean | undefined,
): T {
  if (!day) return day;
  const next = confirmed === true;
  if ((day.context.earlyEntry === true) === next) return day;
  return { ...day, context: { ...day.context, earlyEntry: next } };
}

/** `HH:mm` in the park's own clock as minutes since midnight, or `null` on any other shape. */
export function opensAtMinute(
  opensAt: string | null | undefined,
): number | null {
  if (!opensAt) return null;
  const match = /^(\d{1,2}):(\d{2})$/.exec(opensAt.trim());
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (hours > 23 || minutes > 59) return null;
  return hours * 60 + minutes;
}

/**
 * Whether this ride opens before the park on an early-entry day: the park's headliners, minus any
 * whose own `opensAt` is later than the park's opening.
 */
export function opensEarly(
  ride: Pick<OptimizerRide, "isHeadliner" | "opensAt"> | undefined | null,
  openMin: number,
): boolean {
  if (ride?.isHeadliner !== true) return false;
  const opens = opensAtMinute(ride.opensAt);
  return opens === null || opens <= openMin;
}

/** The closing hour on the day's own axis: 16:00–01:00 closes in hour 25. */
export function unfoldedCloseHour(openHour: number, closeHour: number): number {
  return closeHour < openHour ? closeHour + 24 : closeHour;
}

/**
 * The day's axis, or `null` when the park's hours are unknown; inventing hours would invent a
 * schedule. `closeHour` is the hour the closing time falls in, so `closeMin` is the certifiable end.
 */
export function buildDayGrid(
  openHour: number | null | undefined,
  closeHour: number | null | undefined,
  earlyOpenMin: number | null = null,
): DayGrid | null {
  if (openHour === null || openHour === undefined) return null;
  if (closeHour === null || closeHour === undefined) return null;
  const openMin = openHour * 60;
  const closeMin = unfoldedCloseHour(openHour, closeHour) * 60;
  // Only a window that is really earlier counts.
  const early =
    earlyOpenMin !== null && earlyOpenMin < openMin ? earlyOpenMin : null;
  return { openMin, closeMin, earlyEntryOpenMin: early };
}

/** The axis of a day, early entry included, as the planner's day column builds it. */
export function gridForDay(day: OptimizerDay): DayGrid | null {
  return buildDayGrid(
    day.context.openHour,
    day.context.closeHour,
    earlyEntryOpenMin(day.context),
  );
}

/** The two floors under a ride; see {@link rideFloor}. */
export interface RideFloor {
  /** A published fact. */
  hardMin: number;
  /** Where a new block is filed. */
  softMin: number;
  reason: "park" | "ride";
}

/**
 * The earliest minute a block may be filed at today, and `startMin` on every other date. Snapped
 * up, never to the nearest: rounding 14:03 down would file into a past nobody can act on.
 */
export function nowFloor(
  grid: DayGrid,
  clock?: DayClock,
  startMin: number = grid.openMin,
): number {
  if (clock?.phase !== "today") return startMin;
  return Math.max(
    startMin,
    Math.ceil(clock.nowMinute / SNAP_MIN_FINE) * SNAP_MIN_FINE,
  );
}

/**
 * The two floors under a ride. The hard floor is the ride's own `opensAt` or the park's opening.
 * The soft floor adds the first minute this ride has a curve for (only with a month of data and an
 * hour past the floor), the walk from the gate, and today's clock. On an early-entry day a ride
 * {@link opensEarly} names takes the early opening as its park opening.
 */
export function rideFloor(
  grid: DayGrid,
  ride: OptimizerRide | undefined | null,
  clock?: DayClock,
): RideFloor {
  const opens = opensAtMinute(ride?.opensAt);
  const knowsOpening = opens !== null && opens > grid.openMin;
  const early =
    grid.earlyEntryOpenMin !== null && opensEarly(ride, grid.openMin);
  const startMin = early ? (grid.earlyEntryOpenMin as number) : grid.openMin;
  const hardMin = Math.min(
    knowsOpening ? (opens as number) : startMin,
    grid.closeMin - SNAP_MIN_FINE,
  );

  const first = rideFirstMinute(ride);
  const measuredEnough = (ride?.sampleDays ?? 0) >= SOFT_FLOOR_MIN_SAMPLE_DAYS;
  // An early ride's curve still starts at `openHour`, so it is measured against that.
  const measuredFrom = early ? grid.openMin : hardMin;
  const raised =
    ride && first !== undefined && measuredEnough && first >= measuredFrom + 60
      ? first
      : hardMin;

  // A ride with its own later opening already says when somebody can queue; no turnstile on top.
  const withEntry = knowsOpening
    ? raised
    : Math.max(raised, hardMin + GATE_TO_FIRST_RIDE_MIN);

  return {
    hardMin,
    softMin: Math.max(
      Math.min(withEntry, grid.closeMin - SNAP_MIN_FINE),
      nowFloor(grid, clock, startMin),
    ),
    reason: raised > hardMin || knowsOpening || early ? "ride" : "park",
  };
}
