/**
 * How long the statistic keeps its rows, and what a report can still read of a
 * window: the first UTC day the hourly rows hold, the days a week-chunked
 * table is dropped with, and the roll-up markers a chunk needs before it may
 * go. See docs/frontend/site-analytics.md.
 */
import { addIsoDays } from "../common/utils/date.util";
import { utcDay } from "../common/utils/reader-hash.util";
import {
  HOURLY_RETENTION_DAYS,
  SITE_CHUNK_DAYS,
} from "./site-analytics.constants";

const DAY_MS = 86_400_000;
const CHUNK_MS = SITE_CHUNK_DAYS * DAY_MS;

/** A stretch of instants, `[start, end)`, as ISO strings. */
export interface InstantRange {
  start: string;
  end: string;
}

/**
 * The first day of the chunk that holds `day`. TimescaleDB cuts the chunks of
 * a table by multiples of the chunk interval counted from the Unix epoch, so
 * the week chunks start on a Thursday, whatever day the first row was written.
 */
export function chunkStartDay(day: string): string {
  const ms = Date.parse(`${day}T00:00:00Z`);
  return new Date(Math.floor(ms / CHUNK_MS) * CHUNK_MS)
    .toISOString()
    .slice(0, 10);
}

/**
 * The instant before which the nightly job takes a table's chunks: the UTC
 * midnight `days` days before today. A chunk goes when all of it lies before
 * this.
 */
export function retentionCutoff(now: Date, days: number): string {
  return `${addIsoDays(utcDay(now), -days)}T00:00:00.000Z`;
}

/**
 * The first UTC day the hourly rows hold: the start of the chunk that holds
 * the retention's cutoff day, since the chunk before it is the last one
 * dropped. Every day from here on is there whole; an older day is there only
 * while its chunk waits for its roll-up (or the job has not run yet today).
 */
export function hourlyHorizon(now: Date): string {
  return chunkStartDay(addIsoDays(utcDay(now), -HOURLY_RETENTION_DAYS));
}

/** One marker of `site_rollup_days`: a cuboid of a source rolled up for a day. */
export interface RollupMarker {
  source: string;
  cuboid: number;
  day: string;
}

/**
 * The days among `days` that miss the marker of at least one wanted cuboid:
 * the days whose hourly rows are not summed into every daily table yet, and
 * so must not be dropped. A marker for a cuboid nobody wants any more counts
 * for nothing.
 */
export function unrolledDays(
  days: readonly string[],
  markers: readonly RollupMarker[],
  wanted: ReadonlyArray<{ source: string; cuboid: number }>,
): string[] {
  const have = new Set(markers.map((m) => `${m.source}:${m.cuboid}:${m.day}`));
  return days.filter((day) =>
    wanted.some((w) => !have.has(`${w.source}:${w.cuboid}:${day}`)),
  );
}

/**
 * What the statements of one report read from the hourly rows beyond their
 * horizon. A statement no daily cuboid can serve (a filter on the path, an
 * hourly series) reads the hourly rows for its whole window; for a day whose
 * chunk the nightly job has dropped it finds nothing, and the report answers
 * with what it has. The report says so with {@link HourlyReach.hourlyFrom}.
 */
export class HourlyReach {
  private cut = false;

  constructor(readonly horizon: string) {}

  /** Whether any of the ranges starts before the horizon. */
  reachesBefore(ranges: readonly InstantRange[]): boolean {
    const horizon = Date.parse(`${this.horizon}T00:00:00Z`);
    return ranges.some((range) => Date.parse(range.start) < horizon);
  }

  /**
   * Records that a statement read these ranges from the hourly rows. It cut
   * the report if one of their days before the horizon is rolled up
   * (`rolled`, the days with a marker): such a day's chunk is dropped. A day
   * without a marker keeps its chunk until the job has rolled it up, and a
   * statistic younger than the horizon has no such day, so nothing is cut.
   */
  note(ranges: readonly InstantRange[], rolled: ReadonlySet<string>): void {
    if (this.cut) return;
    const horizon = Date.parse(`${this.horizon}T00:00:00Z`);
    for (const range of ranges) {
      const end = Math.min(Date.parse(range.end), horizon);
      for (
        let at = Math.floor(Date.parse(range.start) / DAY_MS) * DAY_MS;
        at < end;
        at += DAY_MS
      ) {
        if (rolled.has(utcDay(new Date(at)))) {
          this.cut = true;
          return;
        }
      }
    }
  }

  /**
   * The day the figures that needed hourly rows are complete from, or null
   * when every statement was served in full.
   */
  get hourlyFrom(): string | null {
    return this.cut ? this.horizon : null;
  }
}
