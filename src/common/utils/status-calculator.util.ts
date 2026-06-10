import { ScheduleEntry } from "../../parks/entities/schedule-entry.entity";

/**
 * Interface for ride status data used in fallback logic
 */
export interface RideStatusData {
  status: string;
  waitTime: number | null;
  lastUpdated: Date;
}

/**
 * SINGLE SOURCE OF TRUTH for park status
 *
 * Hybrid strategy:
 * 1. Primary: schedule-based (when a schedule exists)
 * 2. Fallback: ride-based (only for parks WITHOUT a schedule)
 *
 * Every service uses this function so status calculations stay consistent
 * across all API endpoints.
 *
 * @param scheduleEntries - The park's schedule entries
 * @param rideStatusData - Current ride status data (optional, for the fallback)
 * @returns true if the park is open, false if closed
 *
 * @example
 * // With schedule (primary)
 * const isOpen = isParkOpen(scheduleEntries);
 *
 * @example
 * // Ohne Schedule (Fallback)
 * const isOpen = isParkOpen([], rideStatusData);
 */
export function isParkOpen(
  scheduleEntries: ScheduleEntry[],
  rideStatusData?: RideStatusData[],
): boolean {
  const now = new Date();

  // Strategy 1: schedule-based (primary)
  // Check whether an OPERATING schedule entry is currently active
  const operatingSchedule = scheduleEntries.find(
    (s) =>
      s.scheduleType === "OPERATING" &&
      s.openingTime &&
      s.closingTime &&
      now >= s.openingTime &&
      now < s.closingTime,
  );

  // Schedule present and park is open: OPERATING
  if (operatingSchedule) {
    return true;
  }

  // Schedule present but park is closed: CLOSED
  // (ignore ride data whenever a schedule exists)
  const hasSchedule =
    scheduleEntries.length > 0 &&
    scheduleEntries.some((s) => s.scheduleType === "OPERATING");
  if (hasSchedule) {
    return false;
  }

  // Strategy 2: ride-based fallback (only when there is NO schedule)
  // Used only for parks without schedule integration
  if (!rideStatusData || rideStatusData.length === 0) {
    return false; // No data → safe default: CLOSED
  }

  // Only consider recent ride data (last 30 minutes)
  const recentRides = rideStatusData.filter((r) =>
    isDataRecent(r.lastUpdated, 30),
  );

  if (recentRides.length === 0) {
    return false; // No recent data → CLOSED
  }

  // Check that at least one ride is OPERATING AND has a wait time > 0.
  // Closed parks often report 0 or 5 min (stale/placeholder) — only a real wait counts.
  const operatingRides = recentRides.filter(
    (r) => r.status === "OPERATING" && r.waitTime !== null && r.waitTime > 0,
  );

  return operatingRides.length > 0;
}

/**
 * Helper: checks whether data is recent.
 *
 * Used to filter out old/stale data so status calculations only rely on
 * current information.
 *
 * @param lastUpdated - Timestamp of the last update
 * @param maxAgeMinutes - Maximum age in minutes (default: 30)
 * @returns true if the data is recent, false if it is too old
 *
 * @example
 * const isFresh = isDataRecent(new Date(), 30); // true
 * const isStale = isDataRecent(new Date('2024-01-01'), 30); // false
 */
export function isDataRecent(
  lastUpdated: Date,
  maxAgeMinutes: number = 30,
): boolean {
  const now = new Date();
  const ageInMinutes = (now.getTime() - lastUpdated.getTime()) / 1000 / 60;
  return ageInMinutes <= maxAgeMinutes;
}
