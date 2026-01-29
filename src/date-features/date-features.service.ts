import { Injectable, Logger } from "@nestjs/common";
import { HolidaysService } from "../holidays/holidays.service";
import { getWeekendDaysForCountry } from "./constants/weekend-rules.constant";
import { formatInParkTimezone } from "../common/utils/date.util";
import { formatInTimeZone } from "date-fns-tz";
import { getTimezoneForCountry } from "../common/utils/timezone.util";

/**
 * Date Features Service
 *
 * Provides region-specific date features for ML predictions:
 * - Weekend detection (varies by country)
 * - Holiday detection (via HolidaysService)
 * - Peak day identification (weekends + holidays)
 *
 * Used for correlating attendance patterns with calendar features.
 */
@Injectable()
export class DateFeaturesService {
  private readonly logger = new Logger(DateFeaturesService.name);

  constructor(private readonly holidaysService: HolidaysService) {}

  /**
   * Check if a date is a weekend in a specific country
   *
   * @param date The date to check
   * @param countryCode ISO 3166-1 alpha-2 country code (e.g., 'US', 'SA', 'AE')
   * @returns True if the date falls on a weekend day in that country
   *
   * @example
   * // Saturday in US (Sat+Sun weekend)
   * isWeekend(new Date('2025-11-22'), 'US') // true
   *
   * // Saturday in Saudi Arabia (Fri+Sat weekend)
   * isWeekend(new Date('2025-11-22'), 'SA') // true
   *
   * // Sunday in Saudi Arabia (Fri+Sat weekend)
   * isWeekend(new Date('2025-11-23'), 'SA') // false
   */
  isWeekend(
    date: Date,
    countryCode: string,
    timezone: string = "UTC",
  ): boolean {
    // formatInTimeZone with "i" returns ISO day-of-week: 1=Mon, 2=Tue, ..., 7=Sun
    // We need JavaScript day-of-week: 0=Sun, 1=Mon, ..., 6=Sat
    const isoDayOfWeek = Number(formatInTimeZone(date, timezone, "i"));
    const dayOfWeek = isoDayOfWeek === 7 ? 0 : isoDayOfWeek;

    const weekendDays = getWeekendDaysForCountry(countryCode);

    return weekendDays.includes(dayOfWeek);
  }

  /**
   * Check if a date is a holiday in a specific country
   *
   * @param date The date to check
   * @param countryCode ISO 3166-1 alpha-2 country code
   * @param region Optional region/state code (for regional holidays)
   * @returns True if the date is a public holiday
   *
   * @example
   * // Check if Christmas Day 2025 is a holiday in US
   * await isHoliday(new Date('2025-12-25'), 'US') // true
   */
  async isHoliday(
    date: Date,
    countryCode: string,
    region?: string,
    timezone: string = "UTC",
  ): Promise<boolean> {
    // Check if it's a holiday in the specific region (or national holiday)
    // If region is provided, strict filtering is applied (National OR Region-Specific).
    // If no region provided, it checks generally for the country.
    const isHolidayResult = await this.holidaysService.isHoliday(
      date,
      countryCode,
      region,
      timezone,
    );

    return isHolidayResult;
  }

  /**
   * Check if a date is a school holiday
   */
  async isSchoolHoliday(
    date: Date,
    countryCode: string,
    region?: string,
    timezone: string = "UTC",
  ): Promise<boolean> {
    return this.holidaysService.isEffectiveSchoolHoliday(
      date,
      countryCode,
      region,
      timezone,
    );
  }

  /**
   * Check if a date is a peak day (weekend OR holiday OR school holiday)
   *
   * Peak days typically have higher park attendance.
   *
   * @param date The date to check
   * @param countryCode ISO 3166-1 alpha-2 country code
   * @param region Optional region/state code
   * @param timezone Optional timezone
   * @returns True if the date is either a weekend or a holiday
   */
  async isPeakDay(
    date: Date,
    countryCode: string,
    region?: string,
    timezone: string = "UTC",
  ): Promise<boolean> {
    const weekend = this.isWeekend(date, countryCode, timezone);
    const holiday = await this.isHoliday(date, countryCode, region, timezone);
    const schoolHoliday = await this.isSchoolHoliday(
      date,
      countryCode,
      region,
      timezone,
    );

    return weekend || holiday || schoolHoliday;
  }

  /**
   * Get date features as an object (useful for ML feature engineering)
   *
   * @param date The date to analyze
   * @param countryCode ISO 3166-1 alpha-2 country code
   * @param region Optional region/state code
   * @param timezone Optional timezone
   * @returns Object with all date features
   */
  async getDateFeatures(
    date: Date,
    countryCode: string,
    region?: string,
    timezone: string = "UTC",
  ): Promise<{
    date: string;
    dayOfWeek: number;
    isWeekend: boolean;
    isHoliday: boolean;
    isSchoolHoliday: boolean;
    isBridgeDay: boolean;
    isPeakDay: boolean;
    countryCode: string;
    region?: string;
  }> {
    const effectiveTimezone =
      timezone && timezone !== "UTC"
        ? timezone
        : getTimezoneForCountry(countryCode) || "UTC";

    const isWeekend = this.isWeekend(date, countryCode, effectiveTimezone);
    const isHoliday = await this.isHoliday(
      date,
      countryCode,
      region,
      effectiveTimezone,
    );
    const isSchoolHoliday = await this.isSchoolHoliday(
      date,
      countryCode,
      region,
      effectiveTimezone,
    );
    const isBridgeDay =
      !isHoliday &&
      !isSchoolHoliday &&
      (await this.holidaysService.isBridgeDay(
        date,
        countryCode,
        region,
        effectiveTimezone,
      ));
    const isPeakDay = isWeekend || isHoliday || isSchoolHoliday || isBridgeDay;

    return {
      date: formatInParkTimezone(date, effectiveTimezone),
      dayOfWeek: Number(formatInTimeZone(date, effectiveTimezone, "i")),
      isWeekend,
      isHoliday,
      isSchoolHoliday,
      isBridgeDay,
      isPeakDay,
      countryCode,
      ...(region && { region }),
    };
  }
}
