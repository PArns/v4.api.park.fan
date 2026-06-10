import { formatInParkTimezone, getCurrentDateInTimezone } from "./date.util";

type OperatingHoursEntry = { type: string; startTime: string; endTime: string };

/**
 * How far back live data (wait times, show times, dining availability) is
 * considered at all — it is only useful for "today", the window just gives
 * slack for parks that span midnight.
 */
export const LIVE_DATA_CUTOFF_DAYS = 7;

/**
 * Cutoff timestamp for live-data queries. Bounding every query by this
 * enables TimescaleDB chunk exclusion on the hypertables.
 */
export function liveDataCutoff(): Date {
  return new Date(Date.now() - LIVE_DATA_CUTOFF_DAYS * 24 * 60 * 60 * 1000);
}

/**
 * Compares two operating hours arrays for changes.
 * Shared between ShowsService and RestaurantsService.
 *
 * @returns true if the arrays differ
 */
export function hasOperatingHoursChanged(
  oldHours: OperatingHoursEntry[] | null | undefined,
  newHours: OperatingHoursEntry[] | null | undefined,
): boolean {
  if (!oldHours && !newHours) return false;
  if (!oldHours || !newHours) return true;
  if (oldHours.length !== newHours.length) return true;

  const oldSorted = [...oldHours].sort((a, b) =>
    a.startTime.localeCompare(b.startTime),
  );
  const newSorted = [...newHours].sort((a, b) =>
    a.startTime.localeCompare(b.startTime),
  );

  for (let i = 0; i < oldSorted.length; i++) {
    if (
      oldSorted[i].type !== newSorted[i].type ||
      oldSorted[i].startTime !== newSorted[i].startTime ||
      oldSorted[i].endTime !== newSorted[i].endTime
    ) {
      return true;
    }
  }
  return false;
}

/**
 * Returns true if the given timestamp belongs to a different calendar day
 * than today, evaluated in the park's local timezone.
 *
 * Used by the delta-save strategy in ShowsService and RestaurantsService
 * to guarantee at least one data point per day even when nothing else changed.
 */
export function hasDateChangedInTimezone(
  lastTimestamp: Date,
  timezone: string,
): boolean {
  const lastDateStr = formatInParkTimezone(lastTimestamp, timezone);
  const currentDateStr = getCurrentDateInTimezone(timezone);
  return lastDateStr !== currentDateStr;
}
