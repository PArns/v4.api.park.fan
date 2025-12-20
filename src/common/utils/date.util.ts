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
