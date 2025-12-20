import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Holiday } from "./entities/holiday.entity";
import { NagerPublicHoliday } from "../external-apis/nager-date/nager-date.types";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { formatInParkTimezone } from "../common/utils/date.util";
import {
  parseDateInTimezone,
  getTimezoneForCountry,
} from "../common/utils/timezone.util";

/**
 * Holidays Service
 *
 * Manages holiday data for ML predictions.
 * Holidays significantly impact park attendance.
 */
@Injectable()
export class HolidaysService {
  private readonly logger = new Logger(HolidaysService.name);

  constructor(
    @InjectRepository(Holiday)
    private holidayRepository: Repository<Holiday>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Save holidays from Nager.Date API
   *
   * Uses upsert to avoid duplicates (based on externalId).
   *
   * IMPORTANT: Holidays are stored as UTC timestamps representing midnight
   * in the country's local timezone. For example, Christmas (2025-12-25) in
   * Germany (UTC+1) is stored as 2025-12-24T23:00:00.000Z.
   */
  async saveHolidaysFromApi(
    holidays: NagerPublicHoliday[],
    countryCode: string,
  ): Promise<number> {
    let savedCount = 0;

    // Get timezone for this country
    const timezone = getTimezoneForCountry(countryCode);
    if (!timezone) {
      this.logger.warn(
        `No timezone mapping found for country: ${countryCode}, using UTC`,
      );
    }

    for (const holiday of holidays) {
      try {
        const externalId = `nager:${countryCode}:${holiday.date}:${holiday.name}`;

        // Determine holiday type
        const holidayType = this.mapHolidayType(holiday.types);

        // Convert date string to Date object at midnight in country's timezone
        const holidayDate = timezone
          ? parseDateInTimezone(holiday.date, timezone)
          : new Date(holiday.date);

        // Create or update holiday
        await this.holidayRepository.upsert(
          {
            externalId,
            date: holidayDate,
            name: holiday.name,
            localName: holiday.localName || undefined,
            country: countryCode,
            region:
              holiday.counties && holiday.counties.length > 0
                ? holiday.counties[0]
                : undefined,
            holidayType,
            isNationwide: holiday.global,
          },
          ["externalId"],
        );

        savedCount++;
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.warn(
          `Failed to save holiday ${holiday.name}: ${errorMessage}`,
        );
      }
    }

    return savedCount;
  }

  /**
   * Map Nager.Date holiday types to our enum
   */
  private mapHolidayType(
    types: string[],
  ): "public" | "observance" | "school" | "bank" {
    if (types.includes("Public")) return "public";
    if (types.includes("Bank")) return "bank";
    if (types.includes("School")) return "school";
    return "observance";
  }

  /**
   * Get holidays for a specific country and date range
   */
  async getHolidays(
    countryCode: string,
    startDate: Date,
    endDate: Date,
  ): Promise<Holiday[]> {
    return this.holidayRepository
      .createQueryBuilder("holiday")
      .where("holiday.country = :countryCode", { countryCode })
      .andWhere("holiday.date >= :startDate", { startDate })
      .andWhere("holiday.date <= :endDate", { endDate })
      .orderBy("holiday.date", "ASC")
      .getMany();
  }

  /**
   * Get all holidays across all countries for a date range
   */
  async getAllHolidays(startDate: Date, endDate: Date): Promise<Holiday[]> {
    return this.holidayRepository
      .createQueryBuilder("holiday")
      .where("holiday.date >= :startDate", { startDate })
      .andWhere("holiday.date <= :endDate", { endDate })
      .orderBy("holiday.date", "ASC")
      .addOrderBy("holiday.country", "ASC")
      .getMany();
  }

  /**
   * Check if a date is a holiday in a specific country (Cached)
   */
  async isHoliday(
    date: Date,
    countryCode: string,
    regionCode?: string,
    timezone?: string,
  ): Promise<boolean> {
    const dateStr = timezone
      ? formatInParkTimezone(date, timezone)
      : date.toISOString().split("T")[0];
    const cacheKey = `holiday:check:${countryCode}:${regionCode || "national"}:${dateStr}`;

    const cached = await this.redis.get(cacheKey);
    if (cached) {
      return cached === "true";
    }

    const query = this.holidayRepository
      .createQueryBuilder("holiday")
      .where("holiday.country = :countryCode", { countryCode })
      .andWhere("holiday.date = :dateStr", { dateStr });

    if (regionCode) {
      // Check for National Holidays OR Regional Holidays for this specific region
      const fullRegionCode = `${countryCode}-${regionCode}`;
      query.andWhere(
        "(holiday.isNationwide = true OR holiday.region = :fullRegionCode)",
        { fullRegionCode },
      );
    } else {
      // If no region specified, we only count Nationwide holidays to avoid false positives
      // from regional holidays in other parts of the country.
      query.andWhere("holiday.isNationwide = true");
    }

    const count = await query.getCount();
    const isHoliday = count > 0;

    // Cache result (24 hours - holidays change rarely)
    await this.redis.set(cacheKey, String(isHoliday), "EX", 24 * 60 * 60);

    return isHoliday;
  }

  /**
   * Check if a date is a bridge day (Brückentag)
   *
   * Logic:
   * - If Friday AND Thursday is a holiday -> Bridge Day
   * - If Monday AND Tuesday is a holiday -> Bridge Day
   */
  async isBridgeDay(
    date: Date,
    countryCode: string,
    regionCode?: string,
    timezone?: string,
  ): Promise<boolean> {
    const dayOfWeek = date.getDay(); // 0 = Sun, 1 = Mon, ..., 5 = Fri, 6 = Sat

    // Case 1: Friday (5) bridging Thursday Holiday
    if (dayOfWeek === 5) {
      const thursday = new Date(date);
      thursday.setDate(date.getDate() - 1);
      const isThuHoliday = await this.isHoliday(
        thursday,
        countryCode,
        regionCode,
        timezone,
      );
      if (isThuHoliday) return true;
    }

    // Case 2: Monday (1) bridging Tuesday Holiday
    if (dayOfWeek === 1) {
      const tuesday = new Date(date);
      tuesday.setDate(date.getDate() + 1);
      const isTueHoliday = await this.isHoliday(
        tuesday,
        countryCode,
        regionCode,
        timezone,
      );
      if (isTueHoliday) return true;
    }

    return false;
  }

  /**
   * Get all unique countries in the database
   */
  async getUniqueCountries(): Promise<string[]> {
    const result = await this.holidayRepository
      .createQueryBuilder("holiday")
      .select("DISTINCT holiday.country", "country")
      .getRawMany();

    return result.map((r) => r.country);
  }

  /**
   * Delete holidays older than a certain date (cleanup)
   */
  async deleteOldHolidays(beforeDate: Date): Promise<number> {
    const result = await this.holidayRepository
      .createQueryBuilder()
      .delete()
      .where("date < :beforeDate", { beforeDate })
      .execute();

    return result.affected || 0;
  }
}
