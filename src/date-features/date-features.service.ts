import { Injectable, Logger } from "@nestjs/common";
import { HolidaysService } from "../holidays/holidays.service";
import { getWeekendDaysForCountry } from "./constants/weekend-rules.constant";

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
  isWeekend(date: Date, countryCode: string): boolean {
    const dayOfWeek = date.getDay(); // 0 = Sunday, 1 = Monday, ..., 6 = Saturday
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
  ): Promise<boolean> {
    // Normalize date to start of day (ignore time)
    const normalizedDate = new Date(
      date.getFullYear(),
      date.getMonth(),
      date.getDate(),
    );

    // Check if it's a holiday in the specific region (or national holiday)
    // If region is provided, strict filtering is applied (National OR Region-Specific).
    // If no region provided, it checks generally for the country.
    const isHolidayResult = await this.holidaysService.isHoliday(
      normalizedDate,
      countryCode,
      region,
    );

    return isHolidayResult;
  }

  /**
   * Check if a date is a peak day (weekend OR holiday)
   *
   * Peak days typically have higher park attendance.
   *
   * @param date The date to check
   * @param countryCode ISO 3166-1 alpha-2 country code
   * @param region Optional region/state code
   * @returns True if the date is either a weekend or a holiday
   *
   * @example
   * // Saturday in US
   * await isPeakDay(new Date('2025-11-22'), 'US') // true (weekend)
   *
   * // Thursday (Thanksgiving) in US
   * await isPeakDay(new Date('2025-11-27'), 'US') // true (holiday)
   *
   * // Regular Tuesday in US
   * await isPeakDay(new Date('2025-11-25'), 'US') // false
   */
  async isPeakDay(
    date: Date,
    countryCode: string,
    region?: string,
  ): Promise<boolean> {
    const weekend = this.isWeekend(date, countryCode);
    const holiday = await this.isHoliday(date, countryCode, region);

    return weekend || holiday;
  }

  /**
   * Get date features as an object (useful for ML feature engineering)
   *
   * @param date The date to analyze
   * @param countryCode ISO 3166-1 alpha-2 country code
   * @param region Optional region/state code
   * @returns Object with all date features
   *
   * @example
   * await getDateFeatures(new Date('2025-12-25'), 'US')
   * // {
   * //   date: '2025-12-25',
   * //   dayOfWeek: 4, // Thursday
   * //   isWeekend: false,
   * //   isHoliday: true, // Christmas
   * //   isPeakDay: true,
   * //   countryCode: 'US'
   * // }
   */
  async getDateFeatures(
    date: Date,
    countryCode: string,
    region?: string,
  ): Promise<{
    date: string;
    dayOfWeek: number;
    isWeekend: boolean;
    isHoliday: boolean;
    isPeakDay: boolean;
    countryCode: string;
    region?: string;
  }> {
    const isWeekend = this.isWeekend(date, countryCode);
    const isHoliday = await this.isHoliday(date, countryCode, region);
    const isPeakDay = isWeekend || isHoliday;

    return {
      date: date.toISOString().split("T")[0], // YYYY-MM-DD
      dayOfWeek: date.getDay(),
      isWeekend,
      isHoliday,
      isPeakDay,
      countryCode,
      ...(region && { region }),
    };
  }
}
