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
  async isHoliday(date: Date, countryCode: string): Promise<boolean> {
    const count = await this.holidayRepository
      .createQueryBuilder("holiday")
      .where("holiday.country = :countryCode", { countryCode })
      .andWhere("holiday.date = :date", { date })
      .getCount();

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
