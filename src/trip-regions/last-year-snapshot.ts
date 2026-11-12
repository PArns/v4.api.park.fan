/**
 * The `last_year` days' source: a past month of one park reduced to its
 * measured crowd level and headliner day peak per day, kept in Redis for a
 * week (`TRIP_LAST_YEAR_TTL`). Written by the calendar warmup for the region
 * parks and, on a miss, by the calendar read itself.
 *
 * Its own key rather than the calendar month cache: a schedule sync evicts every
 * month of a park twice a day (`invalidateCalendarMonthCache`), which is right
 * for future hours and pointless for a month that has passed. Rebuilding five
 * past months per region park after each sync would cost about 30 crowd-level
 * scans a month; this costs one build per month and week.
 */
import type { Redis } from "ioredis";
import { CacheKeys } from "../common/cache/cache-keys";
import { TRIP_LAST_YEAR_TTL } from "../common/cache/cache-ttl";
import { safeJsonParse } from "../common/utils/json.util";
import { addIsoDays } from "../common/utils/date.util";
import type { Park } from "../parks/entities/park.entity";
import type { CalendarService } from "../parks/services/calendar.service";
import { toLastYearEntry, type LastYearEntry } from "./trip-calendar.util";

export type LastYearMonth = Record<string, LastYearEntry>;

/** `YYYY-MM` of a date. */
export const monthOf = (date: string): string => date.slice(0, 7);

/** The first and last day of a month. */
export function monthBounds(ym: string): { first: string; last: string } {
  const [y, m] = ym.split("-").map(Number);
  const next =
    m === 12 ? `${y + 1}-01` : `${y}-${String(m + 1).padStart(2, "0")}`;
  return { first: `${ym}-01`, last: addIsoDays(`${next}-01`, -1) };
}

/** `ym` moved by `delta` months. */
export function shiftMonth(ym: string, delta: number): string {
  const [y, m] = ym.split("-").map(Number);
  const index = y * 12 + (m - 1) + delta;
  return `${Math.floor(index / 12)}-${String((index % 12) + 1).padStart(2, "0")}`;
}

/** Reads the snapshots of several months; a missing one is absent from the map. */
export async function readLastYearMonths(
  redis: Redis,
  parkId: string,
  months: string[],
): Promise<Map<string, LastYearMonth>> {
  const out = new Map<string, LastYearMonth>();
  if (months.length === 0) return out;
  const raw = await redis.mget(
    ...months.map((ym) => CacheKeys.tripLastYearMonth(parkId, ym)),
  );
  raw.forEach((value, i) => {
    const parsed = safeJsonParse<LastYearMonth>(value);
    if (parsed) out.set(months[i], parsed);
  });
  return out;
}

/**
 * Builds one month from the calendar (its measured past path) and stores the
 * snapshot. Only a month that has wholly passed is stored: a month still
 * running would freeze days that have not been measured yet.
 */
export async function writeLastYearMonth(
  calendar: Pick<CalendarService, "buildCalendarResponse">,
  redis: Redis,
  park: Park,
  ym: string,
  today: string,
): Promise<LastYearMonth> {
  const { first, last } = monthBounds(ym);
  const response = await calendar.buildCalendarResponse(
    park,
    new Date(`${first}T12:00:00.000Z`),
    new Date(`${last}T12:00:00.000Z`),
    "none",
  );
  const snapshot: LastYearMonth = {};
  for (const day of response.days) {
    if (day.date >= today) continue;
    snapshot[day.date] = toLastYearEntry(day);
  }
  if (last < today) {
    // Up to a day of spread, so the week's expiries of 80 months do not all
    // fall into the same warmup run.
    const spread = Math.floor(Math.random() * 24 * 60 * 60);
    await redis.set(
      CacheKeys.tripLastYearMonth(park.id, ym),
      JSON.stringify(snapshot),
      "EX",
      TRIP_LAST_YEAR_TTL + spread,
    );
  }
  return snapshot;
}
