/**
 * The shapes the day optimizer reads and writes. Structural on purpose: a `PlanDayDto` from
 * `/plan/day` satisfies {@link OptimizerDay}, and so does a fixture in a spec, without either
 * importing the other. Nothing here depends on Nest, the database or a clock, so the same files
 * run in the request handler and in a worker thread.
 *
 * The field names are the frontend's (`lib/planner/types.ts`, `lib/api/types.ts` in park.fan):
 * the browser stores a plan in this shape and posts it as it is.
 */

/** As `PlanDayDto.tier`. */
export type OptimizerTier =
  "observed" | "measured" | "composed" | "climatology" | "long_range";

/** One hour of a ride's curve, as `/plan/day` serves it. */
export interface OptimizerHour {
  hour: number;
  wait: number;
  source?: OptimizerTier | null;
}

/** One quarter hour of a ride's curve, as `/plan/day` serves it. */
export interface OptimizerSlot {
  minute: number;
  wait: number;
  source?: OptimizerTier | null;
}

/** The part of a `/plan/day` ride the optimizer reads. */
export interface OptimizerRide {
  attractionSlug: string;
  attractionName: string;
  hours: readonly OptimizerHour[];
  slots?: readonly OptimizerSlot[] | null;
  dayPeak?: number | null;
  sampleDays?: number | null;
  latitude?: number | null;
  longitude?: number | null;
  land?: string | null;
  uncertaintyMinutes?: number | null;
  expectedError?: number | null;
  opensAt?: string | null;
  isHeadliner?: boolean | null;
  minimumHeight?: number | null;
  mayGetWet?: boolean | null;
}

/** The part of a `/plan/day` show the optimizer reads: where it is. */
export interface OptimizerShow {
  showSlug: string;
  latitude?: number | null;
  longitude?: number | null;
}

/** A curated works window, as a payload serves it. */
export interface OptimizerWorksPeriod {
  from: string | null;
  to: string | null;
  toUncertain?: boolean;
}

/** A ride a works window closes on the planned day and `/plan/day` therefore leaves out. */
export interface OptimizerRideInWorks {
  attractionSlug: string;
  attractionName: string;
  worksPeriod: OptimizerWorksPeriod;
}

/** The part of `/plan/day`'s `context` the optimizer reads. */
export interface OptimizerContext {
  openHour: number | null;
  closeHour: number | null;
  /** The curated readability flag; absent reads as readable. */
  liveWaitTimes?: { available: boolean; reason?: string | null } | null;
  hasEarlyEntry?: boolean | null;
  earlyEntryMinutesPeak?: number | null;
  /**
   * The visitor's own early-entry answer, folded in by {@link withEarlyEntry}. Never part of the
   * served payload.
   */
  earlyEntry?: boolean;
}

/** One day as the optimizer plans it: `/plan/day` plus the rides in works that day. */
export interface OptimizerDay {
  tier?: OptimizerTier | null;
  context: OptimizerContext;
  rides: readonly OptimizerRide[];
  shows?: readonly OptimizerShow[] | null;
  /**
   * Rides a works window shuts on the day. `/plan/day` leaves them out without naming them; the
   * service adds them, as park.fan's relay does, so an entry for one is held still rather than
   * priced as an unknown ride.
   */
  ridesInWorks?: readonly OptimizerRideInWorks[] | null;
}

/** The icons a free block may carry, as the frontend stores them. */
export const PLANNER_BLOCK_ICONS = [
  "break",
  "food",
  "show",
  "shop",
  "photo",
  "meet",
  "star",
] as const;

/** One of {@link PLANNER_BLOCK_ICONS}. */
export type PlannerBlockIcon = (typeof PLANNER_BLOCK_ICONS)[number];

/** A block the visitor wrote themselves: a duration, never a queue. */
export interface PlannerCustomBlock {
  label: string;
  icon: PlannerBlockIcon;
  durationMinutes: number;
}

/** One block of a planned day: a ride, a free block or a show. */
export interface PlannerEntry {
  /** Unique within the day; a ride may appear twice, so the slug cannot be the key. */
  id: string;
  /** The ride, absent on a free block or a show. */
  attractionSlug?: string;
  attractionName?: string;
  /** Set on a free block, and on a show alongside {@link showSlug}. */
  custom?: PlannerCustomBlock;
  /** One performance of a show: a fixed block with a position. */
  showSlug?: string;
  /** Park-local minutes since midnight; past 1440 on a day that runs past midnight. */
  startMinute: number;
  /** Ticked off: a record, never moved. */
  done?: boolean;
  /** The wait actually queued, recorded when ticked off. */
  actualWait?: number;
}

/** What the visitor said about the day itself. */
export interface PlannerDayPrefs {
  /** The shortest rider, in centimetres. */
  riderHeightCm?: number;
  /** The party would rather stay dry. */
  avoidWet?: boolean;
  /** The visitor holds early entry on this day. */
  earlyEntry?: boolean;
}

/**
 * Where the planned day sits against the park's clock. `future` is the default wherever it is
 * optional, and every rule keyed to it then behaves as if there were no clock.
 */
export type DayClock =
  | { phase: "past" }
  | { phase: "today"; nowMinute: number }
  | { phase: "future" };

/**
 * The latest minute a planned stop may carry: past the end of the next day a minute is no reading
 * of the park's clock. The frontend's `MAX_PLANNED_MINUTE`.
 */
export const MAX_PLANNED_MINUTE = 48 * 60;
