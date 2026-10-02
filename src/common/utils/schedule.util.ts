interface ScheduleLike {
  openingTime?: Date | null;
  closingTime?: Date | null;
  scheduleType: string;
}

type FormattedSchedule = {
  openingTime: string;
  closingTime: string;
  scheduleType: string;
};

/**
 * The schedule types that state what a park-level day *is*. Every other type
 * (TICKETED_EVENT, PRIVATE_EVENT, EXTRA_HOURS, MAINTENANCE, INFO) describes
 * something that happens on the day and can sit beside one of these rows —
 * e.g. OPERATING 09–18 plus TICKETED_EVENT 19–01 (PAR-276, PAR-640).
 */
export const DAY_STATUS_SCHEDULE_TYPES = [
  "OPERATING",
  "CLOSED",
  "UNKNOWN",
] as const;

const DAY_STATUS_RANK: Record<string, number> = {
  OPERATING: 0,
  CLOSED: 1,
  UNKNOWN: 2,
};

/** OPERATING < CLOSED < UNKNOWN < any event type (lower wins). */
export function dayStatusRank(scheduleType: string): number {
  return DAY_STATUS_RANK[scheduleType] ?? DAY_STATUS_SCHEDULE_TYPES.length;
}

/**
 * Picks the row that states a day's status from that day's park-level rows:
 * OPERATING over CLOSED over UNKNOWN. A day that carries only event rows
 * returns its first row, as every reader did before event rows were kept.
 * Rows of equal rank keep their input order.
 */
export function pickDayStatusEntry<T extends { scheduleType: string }>(
  rows: readonly T[] | null | undefined,
): T | undefined {
  if (!rows || rows.length === 0) return undefined;
  let best = rows[0];
  for (const row of rows) {
    if (dayStatusRank(row.scheduleType) < dayStatusRank(best.scheduleType)) {
      best = row;
    }
  }
  return best;
}

/**
 * Maps a today-schedule array to a plain DTO, using the row that states the
 * day's status (see `pickDayStatusEntry`).
 * Shared between LocationService, FavoritesService, DiscoveryService and
 * ParkEnrichmentService.
 */
export function formatTodaySchedule(
  schedule: ScheduleLike[] | null | undefined,
): FormattedSchedule | undefined {
  const entry = pickDayStatusEntry(schedule);
  if (!entry) return undefined;
  return {
    openingTime: entry.openingTime?.toISOString() ?? "",
    closingTime: entry.closingTime?.toISOString() ?? "",
    scheduleType: entry.scheduleType,
  };
}

/**
 * Maps a single next-schedule entry to a plain DTO.
 * Shared between LocationService and FavoritesService.
 */
export function formatNextSchedule(
  schedule: ScheduleLike | null | undefined,
): FormattedSchedule | undefined {
  if (!schedule) return undefined;
  return {
    openingTime: schedule.openingTime?.toISOString() ?? "",
    closingTime: schedule.closingTime?.toISOString() ?? "",
    scheduleType: schedule.scheduleType,
  };
}
