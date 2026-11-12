import type { OptimizerRide, OptimizerTier } from "./types";

/**
 * One ride's day on the minute axis: `/plan/day`'s quarter-hour `slots` where it sends them, its
 * `hours` where it does not. Ported from park.fan's `lib/planner/ride-series.ts`; the contract is
 * "The 15-minute contract" in docs/frontend/plan-day/slots-and-h5.md.
 */

/** The width of one `/plan/day` slot, in minutes. */
export const SLOT_MIN = 15;

/** One point of a ride's day. */
export interface RidePoint {
  /** Park-local minute the point starts at; an hourly point starts on the hour. */
  minute: number;
  wait: number;
  /** As `hours[].source`: set only where the point departs from the day's tier. */
  source?: OptimizerTier | null;
  /** {@link SLOT_MIN} for a slot, 60 for an hour. */
  span: number;
}

interface SlotIndex {
  byMinute: Map<number, RidePoint>;
  /** Hours with at least one slot; their missing quarters have no value at all. */
  slottedHours: Set<number>;
  series: RidePoint[];
}

// Keyed on the ride object, so a new payload builds a new index and nothing can go stale.
const indexCache = new WeakMap<OptimizerRide, SlotIndex>();

function indexOf(ride: OptimizerRide): SlotIndex {
  const cached = indexCache.get(ride);
  if (cached) return cached;
  const byMinute = new Map<number, RidePoint>();
  const slottedHours = new Set<number>();
  for (const slot of ride.slots ?? []) {
    if (!Number.isFinite(slot.minute) || !Number.isFinite(slot.wait)) continue;
    byMinute.set(slot.minute, {
      minute: slot.minute,
      wait: slot.wait,
      source: slot.source,
      span: SLOT_MIN,
    });
    slottedHours.add(Math.floor(slot.minute / 60));
  }
  // An hour with some slots is partly outside the ride's window; its hourly figure would invent
  // the missing quarters.
  const series: RidePoint[] = [...byMinute.values()];
  for (const h of ride.hours) {
    if (slottedHours.has(h.hour)) continue;
    series.push({
      minute: h.hour * 60,
      wait: h.wait,
      source: h.source,
      span: 60,
    });
  }
  series.sort((a, b) => a.minute - b.minute);
  const index = { byMinute, slottedHours, series };
  indexCache.set(ride, index);
  return index;
}

/** Whether `/plan/day` sent this ride a quarter-hour series. */
export function hasSlots(ride: OptimizerRide | null | undefined): boolean {
  return !!ride?.slots && ride.slots.length > 0;
}

/**
 * The ride's figure for the quarter hour `minute` falls in: the slot that starts there, or the
 * hour's figure where that hour has no slot at all. Never interpolated.
 */
export function rideWaitAt(
  ride: OptimizerRide,
  minute: number,
): RidePoint | null {
  const hour = Math.floor(minute / 60);
  if (hasSlots(ride)) {
    const { byMinute, slottedHours } = indexOf(ride);
    const slot = byMinute.get(Math.floor(minute / SLOT_MIN) * SLOT_MIN);
    if (slot) return slot;
    if (slottedHours.has(hour)) return null;
  }
  const point = ride.hours.find((h) => h.hour === hour);
  return point
    ? { minute: hour * 60, wait: point.wait, source: point.source, span: 60 }
    : null;
}

/**
 * Whether `minute` falls in an hour the ride has quarter-hour slots for, but in none of them: the
 * ride does not run there, so no assumption fills it.
 */
export function isQuarterGap(ride: OptimizerRide, minute: number): boolean {
  if (!hasSlots(ride)) return false;
  const { byMinute, slottedHours } = indexOf(ride);
  return (
    !byMinute.has(Math.floor(minute / SLOT_MIN) * SLOT_MIN) &&
    slottedHours.has(Math.floor(minute / 60))
  );
}

/** The first minute the ride has a figure for, or `undefined` for a ride with none. */
export function rideFirstMinute(
  ride: OptimizerRide | null | undefined,
): number | undefined {
  if (!ride) return undefined;
  return indexOf(ride).series[0]?.minute;
}
