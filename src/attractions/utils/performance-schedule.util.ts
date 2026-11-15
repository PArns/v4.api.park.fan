import {
  compareDayWindows,
  dayWindowCovers,
  type DayWindow,
} from "./curated-out-of-service.util";

/**
 * The hand-written start times of an attraction that runs as performances
 * rather than continuously: a 4D cinema, a walk-through show, a theatre ride
 * with a fixed programme (PAR-1355).
 *
 * The feed cannot say it. Phantasialand's „Die 3 Mausketiere" runs continuously
 * in summer and at 13:00, 14:00, 15:00, 16:00 and 17:00 during Wintertraum, and
 * ThemeParks.wiki reports the cinema as an ATTRACTION without showtimes that
 * reads CLOSED all winter. A wait time is the wrong answer for such a ride
 * anyway: the question a visitor has is when the next performance begins.
 *
 * A ride may hold several schedules, each for a run of park-local days (a
 * season's dates). Both bounds are inclusive and either may be open; a
 * schedule with neither holds every day. Schedules never share a day, so
 * "which one runs today" has a single answer.
 *
 * Times are park-local wall-clock `HH:MM` on the day they belong to, sorted
 * and without repeats. Day-stable, so the payload serves every schedule and a
 * client picks the one covering the park's today (the same reasoning as
 * `worksPeriods`: a cached payload must not carry a "today" of its own).
 */
export interface PerformanceSchedule extends DayWindow {
  /** First park-local day, inclusive, or null for a schedule with no start. */
  from: string | null;
  /** Last park-local day, inclusive, or null for a schedule with no end. */
  to: string | null;
  /** Park-local start times, `HH:MM`, ascending. Never empty. */
  times: string[];
}

export interface PerformanceScheduleSource {
  curatedPerformanceSchedules?: readonly PerformanceSchedule[] | null;
}

/** Most schedules one ride may hold: a season or two a year, kept for a few years. */
export const MAX_PERFORMANCE_SCHEDULES = 10;

/** Most start times one schedule may hold: one every half hour around the clock. */
export const MAX_PERFORMANCE_TIMES = 48;

/** A 24-hour `HH:MM`, 00:00 to 23:59. */
export const PERFORMANCE_TIME_PATTERN = /^([01]\d|2[0-3]):[0-5]\d$/;

/**
 * One stored schedule with its keys in a fixed order and its times sorted and
 * unique. Also the entity's read transformer for the jsonb list: jsonb stores
 * keys sorted by length, and the admin's diff and undo compare
 * `JSON.stringify` output.
 */
export function normalizePerformanceSchedule(schedule: {
  from?: string | null;
  to?: string | null;
  times?: readonly unknown[] | null;
}): PerformanceSchedule {
  const times = (schedule.times ?? []).filter(
    (time): time is string => typeof time === "string",
  );
  return {
    from: schedule.from ?? null,
    to: schedule.to ?? null,
    times: [...new Set(times)].sort(),
  };
}

/**
 * Every curated schedule, sorted by `from` (an open start first), then by `to`
 * (an open end last). A schedule without a start time claims nothing and is
 * dropped.
 */
export function resolvePerformanceSchedules(
  source: PerformanceScheduleSource,
): PerformanceSchedule[] {
  return (source.curatedPerformanceSchedules ?? [])
    .map(normalizePerformanceSchedule)
    .filter((schedule) => schedule.times.length > 0)
    .sort(compareDayWindows);
}

/**
 * `{ performanceSchedules }` for a payload, or nothing: almost no ride has
 * one, and an empty list on every ride of every park page is payload for an
 * absent fact.
 */
export function performanceSchedulesOf(source: PerformanceScheduleSource): {
  performanceSchedules?: PerformanceSchedule[];
} {
  const performanceSchedules = resolvePerformanceSchedules(source);
  return performanceSchedules.length > 0 ? { performanceSchedules } : {};
}

/**
 * The start times on a park-local day, or null when no schedule covers it.
 * Null means the ride runs as the feed says, not that it has no performance.
 *
 * @param day - Park-local `YYYY-MM-DD`.
 */
export function performanceTimesOn(
  schedules: readonly PerformanceSchedule[],
  day: string,
): string[] | null {
  return (
    schedules.find((schedule) => dayWindowCovers(schedule, day))?.times ?? null
  );
}
