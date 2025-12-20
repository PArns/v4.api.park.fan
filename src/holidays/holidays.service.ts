import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Holiday } from "./entities/holiday.entity";
import { NagerPublicHoliday } from "../external-apis/nager-date/nager-date.types";

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
  ) {}

  /**
   * Save holidays from Nager.Date API
   *
   * Uses upsert to avoid duplicates (based on externalId).
   */
  async saveHolidaysFromApi(
    holidays: NagerPublicHoliday[],
    countryCode: string,
  ): Promise<number> {
    let savedCount = 0;

    for (const holiday of holidays) {
      try {
        const externalId = `nager:${countryCode}:${holiday.date}:${holiday.name}`;

        // Determine holiday type
        const holidayType = this.mapHolidayType(holiday.types);

        // Create or update holiday
        await this.holidayRepository.upsert(
          {
            externalId,
            date: new Date(holiday.date),
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
   * Check if a date is a holiday in a specific country
   */
  async isHoliday(
    date: Date,
    countryCode: string,
    regionCode?: string,
  ): Promise<boolean> {
    const query = this.holidayRepository
      .createQueryBuilder("holiday")
      .where("holiday.country = :countryCode", { countryCode })
      .andWhere("holiday.date = :date", { date });

    if (regionCode) {
      // Check for National Holidays OR Regional Holidays for this specific region
      // Holidays where region IS NULL are national.
      // Holidays where region matches regionCode are regional.
      // Holidays for OTHER regions should be excluded.

      // Nager.Date region format usually includes country code (e.g. "US-FL", "DE-BW")
      // Our database stores exactly what Nager returns.
      // But let's be robust: check exact match.

      // Since we store "US-FL" in DB, and pass "FL" or "US-FL" as regionCode...
      // We should ideally ensure consistent formatting.
      // Park entity has "FL". Nager returns "US-FL".
      // Let's assume input regionCode is just "FL" from Park entity,
      // so we might need to prefix it, OR match logic.

      // Actually, Nager returns "DE-BW" in countys.
      // Our Park entity stores "BW".
      // So we should construct the full code: `${countryCode}-${regionCode}`
      const fullRegionCode = `${countryCode}-${regionCode}`;

      query.andWhere(
        "(holiday.isNationwide = true OR holiday.region = :fullRegionCode)",
        { fullRegionCode },
      );
    } else {
      // If no region specified, assume we only care about National holidays?
      // Or include ANY holiday?
      // Previously we just checked count > 0, which meant ANY holiday in the country (even regional one elsewhere)
      // This was a bit inaccurate (Bayern holiday counting for Hamburg park).
      // Let's stick to National only if no region is provided, for strictness?
      // OR keep backward compatibility: "If it's a holiday somewhere in the country, it's a holiday"
      // The previous code was: .where("holiday.country ...").andWhere("date ...") -> checks ALL holidays.
      // Let's keep it broad if no region provided (backward compat),
      // but maybe strictly national is better?
      // User Analysis said: "Regional holidays < 10% impact".
      // So defaulting to National-Only might be cleaner if we want to avoid false positives.
      // But for now, let's keep it as is (Any Holiday) to not break existing behavior,
      // just adding region filter reduces false positives for specific regions.
    }

    const count = await query.getCount();
    return count > 0;
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
