import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Holiday } from "./entities/holiday.entity";
import { NagerPublicHoliday } from "../external-apis/nager-date/nager-date.types";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { formatInParkTimezone } from "../common/utils/date.util";

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
   * IMPORTANT: Holidays are stored as pure calendar dates (YYYY-MM-DD)
   * without timezone conversion. A holiday on "2025-12-25" is stored
   * as 2025-12-25 00:00:00 UTC, ensuring it matches December 25th
   * in any timezone when compared using date-only matching.
   */
  async saveHolidaysFromApi(
    holidays: NagerPublicHoliday[],
    countryCode: string,
  ): Promise<number> {
    let savedCount = 0;
    const holidaysToUpsert: any[] = [];
    for (const holiday of holidays) {
      const externalId = `nager:${countryCode}:${holiday.date}:${holiday.name}`;
      const holidayType = this.mapHolidayType(holiday.types);
      const holidayDate = new Date(holiday.date + "T00:00:00.000Z");

      holidaysToUpsert.push({
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
      });
    }

    // Deduplicate by externalId before batch upsert to avoid "ON CONFLICT" errors
    const uniqueHolidays = new Map<string, any>();
    for (const h of holidaysToUpsert) {
      uniqueHolidays.set(h.externalId, h);
    }
    const finalHolidays = Array.from(uniqueHolidays.values());

    if (finalHolidays.length > 0) {
      // Bulk upsert in batches of 500
      for (let i = 0; i < finalHolidays.length; i += 500) {
        const batch = finalHolidays.slice(i, i + 500);
        await this.holidayRepository.upsert(batch, ["externalId"]);
        savedCount += batch.length;
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
   * Generic upsert for any holiday (used by peak seasons sync)
   */
  async upsertHoliday(holiday: {
    externalId: string;
    date: Date;
    name: string;
    localName?: string;
    country: string;
    region?: string;
    holidayType: "public" | "observance" | "school" | "bank";
    isNationwide: boolean;
  }): Promise<void> {
    await this.holidayRepository.upsert(
      {
        externalId: holiday.externalId,
        date: holiday.date,
        name: holiday.name,
        localName: holiday.localName,
        country: holiday.country,
        region: holiday.region,
        holidayType: holiday.holidayType,
        isNationwide: holiday.isNationwide,
      },
      ["externalId"],
    );
  }

  /**
   * Save school holidays from OpenHolidays API
   *
   * Expands date ranges (startDate -> endDate) into individual daily entries.
   * Sets holidayType = 'school'.
   */
  async saveSchoolHolidaysFromApi(
    entries: import("../external-apis/open-holidays/open-holidays.types").OpenHolidaysEntry[],
    countryCode: string,
  ): Promise<number> {
    let savedCount = 0;
    const holidaysToUpsert: any[] = [];

    for (const entry of entries) {
      const start = new Date(entry.startDate);
      const end = new Date(entry.endDate);
      const current = new Date(start);

      while (current <= end) {
        try {
          const dateStr = current.toISOString().split("T")[0];
          const name =
            entry.name.find((n) => n.language.toUpperCase() === "EN")?.text ||
            entry.name.find(
              (n) => n.language.toUpperCase() === countryCode.toUpperCase(),
            )?.text ||
            entry.name[0]?.text ||
            "School Holiday";

          // Regions: Support both 'subdivisions' and 'groups' (BE uses groups)
          let regionsToCheck = entry.subdivisions?.map((s) => s.code) || [];
          if (regionsToCheck.length === 0 && entry.groups) {
            regionsToCheck = entry.groups.map((g) => g.code);
          }

          if (entry.regionalScope === "National" || entry.nationwide) {
            regionsToCheck = [null] as any;
          }

          if (
            regionsToCheck.length === 0 &&
            entry.regionalScope !== "National"
          ) {
            current.setDate(current.getDate() + 1);
            continue;
          }

          for (const regionCode of regionsToCheck) {
            const regionPart = regionCode || "national";
            const externalId = `openholidays:${countryCode}:${regionPart}:${dateStr}:${entry.id}`;

            const holidayDate = new Date(current);
            holidayDate.setUTCHours(0, 0, 0, 0);

            holidaysToUpsert.push({
              externalId,
              date: holidayDate,
              name,
              localName: name,
              country: countryCode,
              region: regionCode || undefined,
              holidayType: "school",
              isNationwide: !regionCode,
            });
          }
        } catch (error) {
          this.logger.warn(
            `Failed to process school holiday entry ${entry.id}: ${error}`,
          );
        }
        current.setDate(current.getDate() + 1);
      }
    }

    if (holidaysToUpsert.length > 0) {
      // Bulk upsert in batches of 500
      for (let i = 0; i < holidaysToUpsert.length; i += 500) {
        const batch = holidaysToUpsert.slice(i, i + 500);
        await this.holidayRepository.upsert(batch, ["externalId"]);
        savedCount += batch.length;
      }
    }

    return savedCount;
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
   *
   * Uses date-only string comparison (YYYY-MM-DD) to ensure timezone-safe matching.
   * A holiday stored as "2025-12-25" will match December 25th regardless of timezone.
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
      .andWhere("CAST(holiday.date AS DATE) = :dateStr", { dateStr });

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
   * Check explicitly if a date is a SCHOOL holiday
   */
  async isSchoolHoliday(
    date: Date,
    countryCode: string,
    regionCode?: string,
    timezone?: string,
  ): Promise<boolean> {
    const dateStr = timezone
      ? formatInParkTimezone(date, timezone)
      : date.toISOString().split("T")[0];
    const cacheKey = `holiday:school:${countryCode}:${regionCode || "national"}:${dateStr}`;

    const cached = await this.redis.get(cacheKey);
    if (cached) {
      return cached === "true";
    }

    const query = this.holidayRepository
      .createQueryBuilder("holiday")
      .where("holiday.country = :countryCode", { countryCode })
      .andWhere("CAST(holiday.date AS DATE) = :dateStr", { dateStr })
      .andWhere("holiday.holidayType = 'school'");

    if (regionCode) {
      const fullRegionCode = `${countryCode}-${regionCode}`;
      query.andWhere(
        "(holiday.isNationwide = true OR holiday.region = :fullRegionCode)",
        { fullRegionCode },
      );
    } else {
      query.andWhere("holiday.isNationwide = true");
    }

    const count = await query.getCount();
    const isSchool = count > 0;

    await this.redis.set(cacheKey, String(isSchool), "EX", 24 * 60 * 60);

    return isSchool;
  }

  /**
   * Check if any influencing region has a school holiday
   * Used for ML features to detect cross-border holiday effects
   */
  async isSchoolHolidayInInfluenceZone(
    date: Date,
    localCountryCode: string,
    localRegionCode: string | null,
    timezone: string,
    influencingRegions: {
      countryCode: string;
      regionCode: string | null;
    }[] = [],
  ): Promise<boolean> {
    // 1. Check local region first
    const isLocal = await this.isSchoolHoliday(
      date,
      localCountryCode,
      localRegionCode || undefined,
      timezone,
    );

    if (isLocal) return true;

    // 2. Check influencing regions
    if (!influencingRegions || influencingRegions.length === 0) {
      return false;
    }

    // Check all influencing regions in parallel
    const results = await Promise.all(
      influencingRegions.map((region) =>
        this.isSchoolHoliday(
          date,
          region.countryCode,
          region.regionCode || undefined,
          timezone, // Assuming influencing regions share similar timezone for "Is it holiday TODAY" check
        ),
      ),
    );

    return results.some((isHoliday) => isHoliday);
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

  /**
   * Save a batch of raw holiday objects (used for peak seasons and internal syncs)
   */
  async saveRawHolidays(holidays: any[]): Promise<number> {
    if (holidays.length === 0) return 0;

    // Deduplicate by externalId
    const uniqueHolidays = new Map<string, any>();
    for (const h of holidays) {
      uniqueHolidays.set(h.externalId, h);
    }
    const finalHolidays = Array.from(uniqueHolidays.values());

    for (let i = 0; i < finalHolidays.length; i += 500) {
      const batch = finalHolidays.slice(i, i + 500);
      await this.holidayRepository.upsert(batch, ["externalId"]);
    }

    return finalHolidays.length;
  }
}
