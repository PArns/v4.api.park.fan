import { addDays } from "date-fns";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";

/**
 * Formats a date as "YYYY-MM-DD" in a specific timezone.
 *
 * This is crucial for matching dates to holidays or schedules that are
 * defined in the park's local calendar day, avoiding UTC shift issues.
 *
 * @param date - The date to format
 * @param timezone - IANA timezone (e.g., "Europe/Berlin", "America/New_York")
 * @returns Formatted date string (YYYY-MM-DD)
 */
export function formatInParkTimezone(date: Date, timezone: string): string {
  return formatInTimeZone(date, timezone, "yyyy-MM-dd");
}

/**
 * Gets the current date as "YYYY-MM-DD" in a specific timezone.
 *
 * Use this when you need "today" in a park's local timezone, not UTC.
 * This ensures operations like ML prediction generation happen for the
 * correct calendar day from the park visitor's perspective.
 *
 * @param timezone - IANA timezone (e.g., "Europe/Berlin", "America/New_York")
 * @returns Current date string in the specified timezone (YYYY-MM-DD)
 *
 * @example
 * // At 2024-01-01 01:00 UTC:
 * getCurrentDateInTimezone("UTC") // "2024-01-01"
 * getCurrentDateInTimezone("Pacific/Auckland") // "2024-01-01" (UTC+13, 14:00 local)
 * getCurrentDateInTimezone("America/Los_Angeles") // "2023-12-31" (UTC-8, 17:00 local)
 */
export function getCurrentDateInTimezone(timezone: string): string {
  return formatInTimeZone(new Date(), timezone, "yyyy-MM-dd");
}

/**
 * Gets a Date object representing the start of the current day (midnight)
 * in a specific timezone, correctly converted to UTC.
 *
 * This is critical for database queries that need to filter by "today"
 * in a park's local timezone. It handles timezone offset correctly to avoid
 * including data from previous/next day due to UTC conversion.
 *
 * @param timezone - IANA timezone (e.g., "America/New_York", "Europe/Paris")
 * @returns Date object representing midnight in the specified timezone
 *
 * @example
 * // At 2026-01-05 20:50 UTC (15:50 in New York, UTC-5):
 * const startOfDay = getStartOfDayInTimezone("America/New_York");
 * // Returns: Date object for 2026-01-05T05:00:00.000Z (2026-01-05 00:00 New York time)
 * // NOT 2026-01-05T00:00:00.000Z (which would be 2026-01-04 19:00 New York time)
 */
export function getStartOfDayInTimezone(timezone: string): Date {
  // Get current date in park timezone as string (e.g., "2026-01-05")
  const dateStr = getCurrentDateInTimezone(timezone);

  // Create a date at midnight in the target timezone
  // fromZonedTime takes a string/date that IS in the timezone and matches it to the UTC instant
  const zonedMidnight = fromZonedTime(`${dateStr}T00:00:00`, timezone);

  return zonedMidnight;
}

/**
 * Checks if two dates represent the same calendar day in a specific timezone.
 *
 * This is essential for comparing dates that may be stored with different
 * time components or in different timezones. Two dates might be different
 * days in UTC but the same day in a park's local timezone.
 *
 * @param date1 - First date to compare
 * @param date2 - Second date to compare
 * @param timezone - IANA timezone for comparison
 * @returns true if both dates are the same calendar day in the timezone
 *
 * @example
 * const utcMidnight = new Date("2024-01-01T00:00:00Z");
 * const utcAlmostNext = new Date("2024-01-01T23:59:00Z");
 * isSameDayInTimezone(utcMidnight, utcAlmostNext, "UTC") // true
 * isSameDayInTimezone(utcMidnight, utcAlmostNext, "Pacific/Auckland") // false (crosses day boundary)
 */
export function isSameDayInTimezone(
  date1: Date,
  date2: Date,
  timezone: string,
): boolean {
  const str1 = formatInTimeZone(date1, timezone, "yyyy-MM-dd");
  const str2 = formatInTimeZone(date2, timezone, "yyyy-MM-dd");
  return str1 === str2;
}

/**
 * Gets tomorrow's date as "YYYY-MM-DD" in a specific timezone.
 *
 * Use this when you need "tomorrow" in a park's local timezone.
 * This is critical for showing next-day predictions or schedules.
 *
 * @param timezone - IANA timezone (e.g., "Europe/Berlin", "America/New_York")
 * @returns Tomorrow's date string in the specified timezone (YYYY-MM-DD)
 *
 * @example
 * // At 2024-01-01 23:00 UTC:
 * getTomorrowDateInTimezone("UTC") // "2024-01-02"
 * getTomorrowDateInTimezone("America/Los_Angeles") // "2024-01-02" (UTC-8, still 15:00 on 2024-01-01 local)
 */
export function getTomorrowDateInTimezone(timezone: string): string {
  const todayStr = getCurrentDateInTimezone(timezone);
  const noonInTz = fromZonedTime(`${todayStr}T12:00:00`, timezone);
  const tomorrowInTz = addDays(noonInTz, 1);
  return formatInTimeZone(tomorrowInTz, timezone, "yyyy-MM-dd");
}

/**
 * Gets the current time as a Date object in a specific timezone.
 *
 * This returns a Date object that represents "now" but adjusted to show
 * the correct hour/minute/day in the specified timezone. Useful for
 * extracting the current hour/day-of-week in a park's local time.
 *
 * @param timezone - IANA timezone (e.g., "America/New_York", "Europe/Paris")
 * @returns Date object with time adjusted to the specified timezone
 *
 * @example
 * // At 2026-01-05 23:00 UTC (18:00 in New York, UTC-5):
 * const nyTime = getCurrentTimeInTimezone("America/New_York");
 * nyTime.getHours() // 18 (not 23)
 * nyTime.getDay() // 0 (Monday in NY, still Monday in UTC too in this case)
 */
export function getCurrentTimeInTimezone(timezone: string): Date {
  // Get current time formatted in the target timezone
  const formatted = formatInTimeZone(
    new Date(),
    timezone,
    "yyyy-MM-dd'T'HH:mm:ss",
  );

  // Parse it back as a Date object
  // This creates a new Date with the local time components matching the timezone
  return new Date(formatted);
}
