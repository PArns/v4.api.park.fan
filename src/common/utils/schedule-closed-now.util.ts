import { isDecisiveScheduleRow } from "./schedule-window.sql";

/** How far around a published window a reading still counts as the park's day. */
export const SCHEDULE_CLOSED_GRACE_MS = 60 * 60 * 1000;

/** A reading whose own source timestamp is older than this is not a live observation. */
export const STALE_SOURCE_READING_MS = 24 * 60 * 60 * 1000;

interface TodayScheduleRow {
  scheduleType: string;
  openingTime?: Date | string | null;
  closingTime?: Date | string | null;
}

/**
 * Whether the park's own schedule says it is closed at `now`: today's rows decide
 * the day (`isDecisiveScheduleRow`), and no OPERATING row's window, widened by
 * {@link SCHEDULE_CLOSED_GRACE_MS} on both sides, contains `now`.
 *
 * False whenever the schedule cannot answer: no decisive row today, or an
 * OPERATING row without both times. Then the feed is all there is (PAR-1236).
 */
export function scheduleSaysClosedNow(
  rows: readonly TodayScheduleRow[] | null | undefined,
  now: Date,
): boolean {
  if (!rows?.some(isDecisiveScheduleRow)) return false;
  const t = now.getTime();
  for (const row of rows) {
    if (row.scheduleType !== "OPERATING") continue;
    if (!row.openingTime || !row.closingTime) return false;
    const open = new Date(row.openingTime).getTime();
    const close = new Date(row.closingTime).getTime();
    if (Number.isNaN(open) || Number.isNaN(close)) return false;
    if (
      t >= open - SCHEDULE_CLOSED_GRACE_MS &&
      t <= close + SCHEDULE_CLOSED_GRACE_MS
    ) {
      return false;
    }
  }
  return true;
}

interface PolledEntity {
  status: string;
  rawStatus?: string;
  lastUpdated?: string;
  /** The source the merged entity is based on, e.g. "themeparks-wiki" or "queue-times". */
  source?: string;
}

/** The one source that carries the park's own calendar; the others only ever see waits. */
const SCHEDULE_AWARE_SOURCE = "themeparks-wiki";

/**
 * For a park its schedule calls closed right now, takes back the two ways a
 * frozen feed reads as open (PAR-1236), in place, and returns how many entities
 * changed:
 *
 * - The conflict resolver's override: a second source's wait of 5+ minutes
 *   turned the base CLOSED or DOWN into OPERATING. A queue-times feed frozen on
 *   last weekend's waits does exactly that on a closed day, so the base status
 *   stands and the override leaves no `rawStatus`.
 * - An OPERATING from a source that never sees the park's calendar
 *   (queue-times, wartezeiten-app) on its own: those keep repeating the last
 *   open day's waits, with fresh timestamps, all through a closed day
 *   (Energylandia, 2026-11-05: 33 rides, one value each for 12 hours).
 * - An OPERATING whose own source timestamp is older than
 *   {@link STALE_SOURCE_READING_MS}: the wiki keeps serving such entries for
 *   months; they say what the ride did then, not now.
 *
 * Each reads CLOSED, which is what the park's own schedule says. A fresh wiki
 * reading stays, so a park the wiki shows running despite a "closed" day still
 * shows what it measures.
 */
export function retractClosedParkOverrides(
  entities: PolledEntity[],
  now: Date,
): number {
  let changed = 0;
  for (const entity of entities) {
    if (entity.rawStatus) {
      entity.status = entity.rawStatus;
      delete entity.rawStatus;
      changed++;
      continue;
    }
    if (entity.status !== "OPERATING") continue;
    const blindToCalendar =
      !!entity.source && entity.source !== SCHEDULE_AWARE_SOURCE;
    const at = entity.lastUpdated
      ? new Date(entity.lastUpdated).getTime()
      : Number.NaN;
    const stale =
      !Number.isNaN(at) && now.getTime() - at > STALE_SOURCE_READING_MS;
    if (blindToCalendar || stale) {
      entity.status = "CLOSED";
      changed++;
    }
  }
  return changed;
}
