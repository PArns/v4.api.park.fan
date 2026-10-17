import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import { subDays } from "date-fns";
import type { TypicalWaitsDto } from "../../attractions/dto/attraction-response.dto";
import { roundToNearest5Minutes } from "../../common/utils/wait-time.utils";

/**
 * Pure typical-waits aggregation — separated from the service so the detail
 * endpoint (one ride) and the nightly park job (every ride of a park, one scan)
 * build the payload with the same code, and so it is unit-testable without a
 * database. See docs/frontend/ride-typical-waits.md.
 */

/** One full seasonal cycle. */
export const TYPICAL_WAITS_WINDOW_DAYS = 365;
/** Hours with fewer samples are dropped as noise before the daily peak. */
export const TYPICAL_WAITS_MIN_SAMPLES_PER_HOUR = 2;
/** Operating days with data needed before `displayable` is true. */
export const TYPICAL_WAITS_MIN_TOTAL_SAMPLE_DAYS = 20;

/** One operating day of one ride: its peak (max hourly P90). */
export interface TypicalWaitsDayInput {
  /** Park-local date, YYYY-MM-DD. */
  date: string;
  /** Day of week, 0=Sunday … 6=Saturday (Postgres EXTRACT(DOW) convention). */
  dow: number;
  peak: number;
}

export interface TypicalWaitsWindow {
  /** Window start, park-local YYYY-MM-DD. */
  dataFrom: string;
  /** Window end (yesterday), park-local YYYY-MM-DD. */
  dataTo: string;
  /** Scan bounds in UTC: [startUtc, endUtc). */
  startUtc: Date;
  endUtc: Date;
}

/** The look-back window ending yesterday in the park's timezone. */
export function typicalWaitsWindow(
  now: Date,
  timezone: string,
): TypicalWaitsWindow {
  const dataTo = formatInTimeZone(subDays(now, 1), timezone, "yyyy-MM-dd");
  const dataFrom = formatInTimeZone(
    subDays(now, TYPICAL_WAITS_WINDOW_DAYS),
    timezone,
    "yyyy-MM-dd",
  );
  // Bound the scan in UTC against the park-local date window.
  const startUtc = fromZonedTime(`${dataFrom}T00:00:00`, timezone);
  const endUtc = fromZonedTime(`${dataTo}T00:00:00`, timezone);
  endUtc.setUTCDate(endUtc.getUTCDate() + 1); // inclusive of dataTo
  return { dataFrom, dataTo, startUtc, endUtc };
}

/**
 * Linear-interpolation percentile over an ascending-sorted array, matching
 * PostgreSQL's percentile_cont semantics. `q` in [0, 1].
 */
export function percentileCont(sortedAsc: number[], q: number): number {
  const n = sortedAsc.length;
  if (n === 0) return 0;
  if (n === 1) return sortedAsc[0];
  const rank = q * (n - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return sortedAsc[lo];
  return sortedAsc[lo] + (rank - lo) * (sortedAsc[hi] - sortedAsc[lo]);
}

/** Bucket = {typical: P50, busy: P90} over a set of daily peaks. */
function bucketOf(peaks: number[]): TypicalWaitsDto["weekday"] {
  if (peaks.length === 0) {
    return { typical: null, busy: null, sampleDays: 0 };
  }
  const sorted = [...peaks].sort((a, b) => a - b);
  return {
    typical: roundToNearest5Minutes(percentileCont(sorted, 0.5)),
    busy: roundToNearest5Minutes(percentileCont(sorted, 0.9)),
    sampleDays: peaks.length,
  };
}

/**
 * Build the typical-waits payload from a ride's daily peaks (ascending by date).
 * Weekend membership is country-aware — pass the country's weekend days.
 */
export function buildTypicalWaits(
  daily: TypicalWaitsDayInput[],
  weekendDays: ReadonlySet<number>,
  window: Pick<TypicalWaitsWindow, "dataFrom" | "dataTo">,
  generatedAt: Date,
): TypicalWaitsDto {
  const weekday = bucketOf(
    daily.filter((d) => !weekendDays.has(d.dow)).map((d) => d.peak),
  );
  const weekend = bucketOf(
    daily.filter((d) => weekendDays.has(d.dow)).map((d) => d.peak),
  );

  // Per day-of-week (only days with data), ordered 0=Sun..6=Sat.
  const byDayOfWeek: TypicalWaitsDto["byDayOfWeek"] = [];
  for (let dow = 0; dow <= 6; dow++) {
    const peaks = daily.filter((d) => d.dow === dow).map((d) => d.peak);
    if (peaks.length === 0) continue;
    byDayOfWeek.push({
      dayOfWeek: dow,
      isWeekend: weekendDays.has(dow),
      ...bucketOf(peaks),
    });
  }

  // Record peak over the window, with the date it occurred. On ties, the
  // most recent date wins (daily is sorted ascending, so >= keeps the last).
  let record: { peak: number; date: string } | null = null;
  for (const d of daily) {
    if (record === null || d.peak >= record.peak) {
      record = { peak: d.peak, date: d.date };
    }
  }
  const peak: TypicalWaitsDto["peak"] = record
    ? { value: roundToNearest5Minutes(record.peak), date: record.date }
    : null;

  return {
    weekday,
    weekend,
    byDayOfWeek,
    peak,
    windowDays: TYPICAL_WAITS_WINDOW_DAYS,
    dataFrom: window.dataFrom,
    dataTo: window.dataTo,
    displayable: daily.length >= TYPICAL_WAITS_MIN_TOTAL_SAMPLE_DAYS,
    generatedAt: generatedAt.toISOString(),
  };
}
