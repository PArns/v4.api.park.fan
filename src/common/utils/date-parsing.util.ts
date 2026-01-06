import { BadRequestException } from "@nestjs/common";
import { getCurrentDateInTimezone, formatInParkTimezone } from "./date.util";
import { fromZonedTime } from "date-fns-tz";

/**
 * Options for parsing date ranges from query parameters
 */
export interface DateRangeOptions {
  /**
   * IANA timezone to interpret dates in (e.g., "America/New_York", "Europe/Berlin")
   * CRITICAL: All date parsing must be timezone-aware to avoid off-by-one day errors
   */
  timezone: string;

  /**
   * Default number of days to go back from today if 'from' is not provided
   * @default 0 (today)
   */
  defaultFromDaysAgo?: number;

  /**
   * Default number of days ahead from 'from' date if 'to' is not provided
   * @default 30
   */
  defaultToDaysAhead?: number;
}

/**
 * Parse date range from query parameters with proper timezone handling
 *
 * This utility ensures that all date parsing is timezone-aware, which is critical
 * for parks in different timezones. Without proper timezone handling:
 * - Weather data can be off by one day
 * - Schedule queries can miss or duplicate days
 * - Predictions can use wrong date ranges
 *
 * @param from - Optional start date string (YYYY-MM-DD format)
 * @param to - Optional end date string (YYYY-MM-DD format)
 * @param options - Timezone and default options
 * @returns Object with fromDate and toDate as Date objects (midnight in park timezone)
 * @throws BadRequestException if date format is invalid
 *
 * @example
 * // Park in Los Angeles (UTC-8)
 * const { fromDate, toDate } = parseDateRange("2024-01-15", "2024-01-20", {
 *   timezone: "America/Los_Angeles"
 * });
 * // fromDate = 2024-01-15T08:00:00.000Z (midnight in LA)
 * // toDate = 2024-01-20T07:59:59.999Z (end of day in LA)
 */
export function parseDateRange(
  from: string | undefined,
  to: string | undefined,
  options: DateRangeOptions,
): { fromDate: Date; toDate: Date } {
  const { timezone, defaultFromDaysAgo = 0, defaultToDaysAhead = 30 } = options;

  // Parse 'from' date
  let fromDateStr: string;
  if (from) {
    // Validate format by attempting to parse
    const testDate = new Date(from);
    if (isNaN(testDate.getTime())) {
      throw new BadRequestException(
        'Invalid "from" date format. Use YYYY-MM-DD.',
      );
    }
    fromDateStr = from;
  } else {
    // Default: today in park timezone
    const today = getCurrentDateInTimezone(timezone);
    if (defaultFromDaysAgo > 0) {
      const d = new Date(today);
      d.setDate(d.getDate() - defaultFromDaysAgo);
      fromDateStr = formatInParkTimezone(d, timezone);
    } else {
      fromDateStr = today;
    }
  }

  // Parse 'to' date
  let toDateStr: string;
  if (to) {
    const testDate = new Date(to);
    if (isNaN(testDate.getTime())) {
      throw new BadRequestException(
        'Invalid "to" date format. Use YYYY-MM-DD.',
      );
    }
    toDateStr = to;
  } else {
    // Default: from + N days ahead
    const fromDate = new Date(fromDateStr);
    fromDate.setDate(fromDate.getDate() + defaultToDaysAhead);
    toDateStr = formatInParkTimezone(fromDate, timezone);
  }

  // Convert to Date objects representing midnight and end-of-day in park timezone
  // This ensures DB queries work correctly regardless of server timezone
  const fromDate = fromZonedTime(`${fromDateStr}T00:00:00`, timezone);
  const toDate = fromZonedTime(`${toDateStr}T23:59:59`, timezone);

  return { fromDate, toDate };
}

/**
 * Validate that a date range is within acceptable limits
 *
 * @param fromDate - Start date
 * @param toDate - End date
 * @param maxDays - Maximum number of days allowed in the range
 * @throws BadRequestException if range exceeds maxDays or dates are in wrong order
 */
export function validateDateRange(
  fromDate: Date,
  toDate: Date,
  maxDays: number,
): void {
  if (fromDate > toDate) {
    throw new BadRequestException(
      'Invalid date range: "from" must be before or equal to "to"',
    );
  }

  const daysDiff = Math.ceil(
    (toDate.getTime() - fromDate.getTime()) / (1000 * 60 * 60 * 24),
  );

  if (daysDiff > maxDays) {
    throw new BadRequestException(
      `Date range too large. Maximum allowed: ${maxDays} days.`,
    );
  }
}
