import { formatInParkTimezone } from "./date.util";
import { formatInTimeZone } from "date-fns-tz";

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
  holidayType: HolidayType | null; // Type of holiday: "public", "school", "bank", "observance", or null
  isPublicHoliday: boolean; // Convenience: true if holidayType is "public" or "bank"
  isSchoolHoliday: boolean; // Convenience: true if holidayType is "school"
}

/**
 * Holiday type: "public" for public holidays, "school" for school holidays
 * Bridge days only apply to public holidays, not school holidays
 */
export type HolidayType = "public" | "school" | "observance" | "bank";

/**
 * Holiday entry with type information
 */
export interface HolidayEntry {
  name: string;
  type: HolidayType;
}

/**
 * Calculate holiday information for a given date
 *
 * @param date - The date to check
 * @param holidayMap - Map of date strings (YYYY-MM-DD) to holiday names or HolidayEntry objects
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
  holidayMap: Map<string, string | HolidayEntry>,
  timezone: string,
): HolidayInfo {
  const dateStr = formatInParkTimezone(date, timezone);
  const holidayEntry = holidayMap.get(dateStr);
  let holidayName: string | null = null;
  let holidayType: HolidayType | null = null;

  if (holidayEntry) {
    if (typeof holidayEntry === "string") {
      holidayName = holidayEntry;
      holidayType = "public"; // Default to public if only string provided
    } else {
      holidayName = holidayEntry.name;
      holidayType = holidayEntry.type;
    }
  }

  let isHoliday = !!holidayName;

  // Use formatInTimeZone to get day of week in target timezone (1=Mon, ..., 7=Sun)
  // We convert to 0-6 (0=Sun, 1=Mon, ..., 6=Sat) to match JS getDay()
  const dayOfWeekIso = Number(formatInTimeZone(date, timezone, "i"));
  const dayOfWeek = dayOfWeekIso === 7 ? 0 : dayOfWeekIso;

  // Check if weekend after Friday holiday
  // Ferien gelten nur übers Wochenende, wenn freitags ein ferientag ist
  // Wenn Freitag noch Ferien sind, soll das Wochenende auch noch als Ferien markiert werden
  // WICHTIG: Weekend-Extension gilt NUR für Schulferien, nicht für öffentliche Feiertage!
  if (!isHoliday && (dayOfWeek === 0 || dayOfWeek === 6)) {
    // Weekend (0 = Sunday, 6 = Saturday)
    // Check if Friday (5) is a SCHOOL holiday (not public holiday)
    const fridayDate = new Date(date);
    // For Saturday: go back 1 day to get Friday
    // For Sunday: go back 2 days to get Friday
    const daysBack = dayOfWeek === 6 ? 1 : 2;
    fridayDate.setDate(date.getDate() - daysBack);
    const fridayDateStr = formatInParkTimezone(fridayDate, timezone);

    // Only mark weekend as holiday if Friday is a SCHOOL holiday
    const fridayHoliday = holidayMap.get(fridayDateStr);
    if (fridayHoliday) {
      // Check if it's a school holiday - only extend school holidays to weekends
      const fridayType =
        typeof fridayHoliday === "string" ? "public" : fridayHoliday.type;

      if (fridayType === "school") {
        // If Friday is a school holiday, extend to weekend
        isHoliday = true;
        if (typeof fridayHoliday === "string") {
          holidayName = fridayHoliday;
          holidayType = "school";
        } else {
          holidayName = fridayHoliday.name;
          holidayType = fridayHoliday.type;
        }
      }
      // Public holidays do NOT extend to weekends - weekend after Christmas is just a normal weekend
    }
  }

  // Check Bridge Day Logic
  // 1. Friday (5) after Thursday Holiday
  // 2. Monday (1) before Tuesday Holiday
  // 3. Tuesday (2) between Monday and Wednesday holidays
  // 4. Wednesday (3) between Tuesday and Thursday holidays
  // 5. Thursday (4) between Wednesday and Friday holidays
  // Note: Only applies to public holidays, not school holidays
  let isBridgeDay = false;

  const checkIsPublicHoliday = (
    entry: string | HolidayEntry | undefined,
  ): boolean => {
    if (!entry) return false;
    if (typeof entry === "string") return true; // Default to public if only string
    return entry.type === "public" || entry.type === "bank";
  };

  if (dayOfWeek === 5) {
    // Friday - check if Thursday is a public holiday
    const prevDate = new Date(date);
    prevDate.setDate(date.getDate() - 1);
    const prevDateStr = formatInParkTimezone(prevDate, timezone);
    if (checkIsPublicHoliday(holidayMap.get(prevDateStr))) isBridgeDay = true;
  } else if (dayOfWeek === 1) {
    // Monday - check if Tuesday is a public holiday
    const nextDate = new Date(date);
    nextDate.setDate(date.getDate() + 1);
    const nextDateStr = formatInParkTimezone(nextDate, timezone);
    if (checkIsPublicHoliday(holidayMap.get(nextDateStr))) isBridgeDay = true;
  } else if (dayOfWeek === 2) {
    // Tuesday - check if both Monday and Wednesday are public holidays
    const prevDate = new Date(date);
    prevDate.setDate(date.getDate() - 1);
    const prevDateStr = formatInParkTimezone(prevDate, timezone);
    const nextDate = new Date(date);
    nextDate.setDate(date.getDate() + 1);
    const nextDateStr = formatInParkTimezone(nextDate, timezone);
    if (
      checkIsPublicHoliday(holidayMap.get(prevDateStr)) &&
      checkIsPublicHoliday(holidayMap.get(nextDateStr))
    ) {
      isBridgeDay = true;
    }
  } else if (dayOfWeek === 3) {
    // Wednesday - check if both Tuesday and Thursday are public holidays
    const prevDate = new Date(date);
    prevDate.setDate(date.getDate() - 1);
    const prevDateStr = formatInParkTimezone(prevDate, timezone);
    const nextDate = new Date(date);
    nextDate.setDate(date.getDate() + 1);
    const nextDateStr = formatInParkTimezone(nextDate, timezone);
    if (
      checkIsPublicHoliday(holidayMap.get(prevDateStr)) &&
      checkIsPublicHoliday(holidayMap.get(nextDateStr))
    ) {
      isBridgeDay = true;
    }
  } else if (dayOfWeek === 4) {
    // Thursday - check if both Wednesday and Friday are public holidays
    const prevDate = new Date(date);
    prevDate.setDate(date.getDate() - 1);
    const prevDateStr = formatInParkTimezone(prevDate, timezone);
    const nextDate = new Date(date);
    nextDate.setDate(date.getDate() + 1);
    const nextDateStr = formatInParkTimezone(nextDate, timezone);
    if (
      checkIsPublicHoliday(holidayMap.get(prevDateStr)) &&
      checkIsPublicHoliday(holidayMap.get(nextDateStr))
    ) {
      isBridgeDay = true;
    }
  }

  // Bridge day cannot be a holiday
  const finalIsBridgeDay = isHoliday ? false : isBridgeDay;

  // Convenience flags
  // Note: "observance" holidays are typically not official public holidays,
  // but some observances (like Christmas Eve in some regions) should be treated as public holidays
  // For now, we only mark "public" and "bank" as public holidays
  const isPublicHolidayFlag =
    holidayType === "public" || holidayType === "bank";
  const isSchoolHolidayFlag = holidayType === "school";

  return {
    isHoliday,
    holidayName,
    isBridgeDay: finalIsBridgeDay,
    holidayType,
    isPublicHoliday: isPublicHolidayFlag,
    isSchoolHoliday: isSchoolHolidayFlag,
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
  T extends string | { name: string } | HolidayEntry,
>(dateStr: string, holidayMap: Map<string, T>, timezone: string): HolidayInfo {
  // Convert to HolidayEntry map if needed
  const entryMap = new Map<string, string | HolidayEntry>();
  for (const [date, value] of holidayMap.entries()) {
    if (typeof value === "string") {
      entryMap.set(date, value);
    } else if (
      typeof value === "object" &&
      value !== null &&
      "name" in value &&
      "type" in value
    ) {
      // Ensure type is a valid HolidayType
      const entry = value as HolidayEntry;
      entryMap.set(date, {
        name: entry.name,
        type: entry.type as HolidayType,
      });
    } else if (typeof value === "object" && value !== null && "name" in value) {
      // Legacy: object with just name property
      entryMap.set(date, (value as { name: string }).name);
    } else {
      // Fallback: convert to string
      entryMap.set(date, String(value));
    }
  }

  // Parse date string (YYYY-MM-DD) - create date at midnight in UTC
  // This ensures consistent date formatting regardless of timezone
  const [year, month, day] = dateStr.split("-").map(Number);
  // Create date at noon UTC to avoid issues with date shifting across timezones.
  // Midnight UTC (00:00:00) can shift to the previous day in Western timezones (e.g., US),
  // whereas Noon UTC is safely within the same calendar day for all timezones between UTC-12 and UTC+12.
  const date = new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
  return calculateHolidayInfo(date, entryMap, timezone);
}
