import { formatInParkTimezone } from "./date.util";

/**
 * Holiday Calculation Utilities
 *
 * Centralized logic for determining if a date is a holiday, bridge day,
 * or weekend extension of a holiday. This logic is used in both TypeScript
 * services and can be replicated in the ML service (Python).
 *
 * Rules:
 * 1. Direct holiday: Date is in the holiday map
 * 2. Bridge day: Friday after Thursday holiday OR Monday before Tuesday holiday
 * 3. Weekend extension: Saturday/Sunday after Friday holiday
 */

export interface HolidayInfo {
  isHoliday: boolean;
  holidayName: string | null;
  isBridgeDay: boolean;
}

/**
 * Calculate holiday information for a given date
 *
 * @param date - The date to check
 * @param holidayMap - Map of date strings (YYYY-MM-DD) to holiday names
 * @param timezone - IANA timezone for date calculations (e.g., "Europe/Berlin")
 * @returns Holiday information including isHoliday, holidayName, and isBridgeDay
 *
 * @example
 * const holidayMap = new Map([
 *   ["2025-12-25", "Christmas Day"],
 *   ["2025-12-26", "Boxing Day"]
 * ]);
 * const info = calculateHolidayInfo(new Date("2025-12-27"), holidayMap, "Europe/Berlin");
 * // Returns: { isHoliday: true, holidayName: "Christmas Day", isBridgeDay: false }
 */
export function calculateHolidayInfo(
  date: Date,
  holidayMap: Map<string, string>,
  timezone: string,
): HolidayInfo {
  const dateStr = formatInParkTimezone(date, timezone);
  let holidayName = holidayMap.get(dateStr) || null;
  let isHoliday = !!holidayName;

  const dayOfWeek = date.getDay();

  // Check if weekend after Friday holiday
  // Ferien gelten nur übers Wochenende, wenn freitags ein ferientag ist
  if (!isHoliday && (dayOfWeek === 0 || dayOfWeek === 6)) {
    // Weekend (0 = Sunday, 6 = Saturday)
    // Check if Friday (5) is a holiday
    const fridayDate = new Date(date);
    // For Saturday: go back 1 day to get Friday
    // For Sunday: go back 2 days to get Friday
    const daysBack = dayOfWeek === 6 ? 1 : 2;
    fridayDate.setDate(date.getDate() - daysBack);
    const fridayDateStr = formatInParkTimezone(fridayDate, timezone);

    // Only mark weekend as holiday if Friday is a holiday
    if (holidayMap.has(fridayDateStr)) {
      isHoliday = true;
      holidayName = holidayMap.get(fridayDateStr) || null;
    }
  }

  // Check Bridge Day Logic
  // Friday (5) after Thursday Holiday OR Monday (1) before Tuesday Holiday
  let isBridgeDay = false;

  if (dayOfWeek === 5) {
    // Friday
    const prevDate = new Date(date);
    prevDate.setDate(date.getDate() - 1);
    const prevDateStr = formatInParkTimezone(prevDate, timezone);
    if (holidayMap.has(prevDateStr)) isBridgeDay = true;
  } else if (dayOfWeek === 1) {
    // Monday
    const nextDate = new Date(date);
    nextDate.setDate(date.getDate() + 1);
    const nextDateStr = formatInParkTimezone(nextDate, timezone);
    if (holidayMap.has(nextDateStr)) isBridgeDay = true;
  }

  // Bridge day cannot be a holiday
  const finalIsBridgeDay = isHoliday ? false : isBridgeDay;

  return {
    isHoliday,
    holidayName,
    isBridgeDay: finalIsBridgeDay,
  };
}

/**
 * Calculate holiday information for a date string (YYYY-MM-DD)
 *
 * Convenience function that parses a date string and calls calculateHolidayInfo.
 *
 * @param dateStr - Date string in YYYY-MM-DD format
 * @param holidayMap - Map of date strings (YYYY-MM-DD) to holiday names or Holiday objects
 * @param timezone - IANA timezone for date calculations
 * @returns Holiday information
 */
export function calculateHolidayInfoFromString<
  T extends string | { name: string },
>(dateStr: string, holidayMap: Map<string, T>, timezone: string): HolidayInfo {
  // Convert Holiday objects to string map if needed
  const stringMap = new Map<string, string>();
  for (const [date, value] of holidayMap.entries()) {
    stringMap.set(date, typeof value === "string" ? value : value.name);
  }

  // Parse date string (YYYY-MM-DD) - create date at midnight in local time
  const [year, month, day] = dateStr.split("-").map(Number);
  const date = new Date(year, month - 1, day);
  return calculateHolidayInfo(date, stringMap, timezone);
}
