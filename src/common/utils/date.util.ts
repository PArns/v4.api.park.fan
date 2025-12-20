import { formatInTimeZone } from "date-fns-tz";

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
