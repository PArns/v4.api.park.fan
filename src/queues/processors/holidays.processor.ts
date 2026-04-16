import { Processor, Process, InjectQueue } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job, Queue } from "bull";
import { HolidaysService } from "../../holidays/holidays.service";
import { NagerDateClient } from "../../external-apis/nager-date/nager-date.client";
import { OpenHolidaysClient } from "../../external-apis/open-holidays/open-holidays.client";
import { ParksService } from "../../parks/parks.service";
import { HolidayInput } from "../../common/types/holiday-input.type";

/**
 * Holidays Processor
 *
 * Fetches public holiday data from Nager.Date API.
 *
 * Strategy:
 * 1. Get unique countries from parks
 * 2. Fetch holidays for current year + next 2 years (for predictions)
 * 3. Fetch previous 2 years (for historical ML training)
 * 4. Save to database (upsert to avoid duplicates)
 *
 * Schedule: Monthly (holidays change rarely)
 */
@Processor("holidays")
export class HolidaysProcessor {
  private readonly logger = new Logger(HolidaysProcessor.name);

  constructor(
    private holidaysService: HolidaysService,
    private nagerDateClient: NagerDateClient,
    private openHolidaysClient: OpenHolidaysClient,
    private parksService: ParksService,
    @InjectQueue("park-metadata")
    private parkMetadataQueue: Queue,
  ) {}

  /**
   * Calculate Easter Sunday for a given year (Anonymous Gregorian algorithm).
   * Returns a UTC Date object at midnight.
   */
  private calculateEasterSunday(year: number): Date {
    const a = year % 19;
    const b = Math.floor(year / 100);
    const c = year % 100;
    const d = Math.floor(b / 4);
    const e = b % 4;
    const f = Math.floor((b + 8) / 25);
    const g = Math.floor((b - f + 1) / 3);
    const h = (19 * a + b - d - g + 15) % 30;
    const i = Math.floor(c / 4);
    const k = c % 4;
    const l = (32 + 2 * e + 2 * i - h - k) % 7;
    const m = Math.floor((a + 11 * h + 22 * l) / 451);
    const month = Math.floor((h + l - 7 * m + 114) / 31);
    const day = ((h + l - 7 * m + 114) % 31) + 1;
    return new Date(
      `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}T00:00:00.000Z`,
    );
  }

  @Process("fetch-holidays")
  async handleSyncHolidays(_job: Job): Promise<void> {
    this.logger.log("🎉 Starting holidays sync...");

    try {
      // Get relevant country codes (with parks OR as influencers)
      const countries = await this.parksService.getSyncCountryCodes();
      this.logger.log(
        `Found ${countries.length} relevant countries for holiday sync`,
      );

      if (countries.length === 0) {
        this.logger.warn("No countries found. Skipping holiday sync.");
        return;
      }

      // Fetch holidays for each country
      let totalHolidaysSaved = 0;
      const currentYear = new Date().getFullYear();
      const startYear = currentYear - 1;
      const endYear = currentYear + 2;

      this.logger.log(
        `Syncing holidays for period: ${startYear} to ${endYear}`,
      );

      for (const isoCode of countries) {
        try {
          // 1. Public Holidays (Nager.Date)
          try {
            const holidays = await this.nagerDateClient.getHolidaysForYears(
              isoCode,
              startYear,
              endYear,
            );

            const savedCount = await this.holidaysService.saveHolidaysFromApi(
              holidays,
              isoCode,
            );
            totalHolidaysSaved += savedCount;

            // 1.1 Long Weekends (Nager.Date)
            for (let year = startYear; year <= endYear; year++) {
              const longWeekends = await this.nagerDateClient.getLongWeekends(
                year,
                isoCode,
              );
              const savedBridges =
                await this.holidaysService.saveLongWeekendsFromApi(
                  longWeekends,
                  isoCode,
                );
              totalHolidaysSaved += savedBridges;
            }
          } catch (error) {
            this.logger.error(
              `Failed to fetch Nager holidays/weekends for ${isoCode}: ${error}`,
            );
          }

          // 2. School Holidays (OpenHolidays)
          try {
            let totalSchoolForCountry = 0;

            for (let year = startYear; year <= endYear; year++) {
              const yearStart = `${year}-01-01`;
              const yearEnd = `${year}-12-31`;

              const schoolHolidays =
                await this.openHolidaysClient.getSchoolHolidays(
                  isoCode,
                  yearStart,
                  yearEnd,
                );

              const savedSchoolCount =
                await this.holidaysService.saveSchoolHolidaysFromApi(
                  schoolHolidays,
                  isoCode,
                );
              totalSchoolForCountry += savedSchoolCount;
              totalHolidaysSaved += savedSchoolCount;
            }

            this.logger.log(
              `Fetched school holidays for ${isoCode}, expanded to ${totalSchoolForCountry} days.`,
            );
          } catch (error) {
            this.logger.error(
              `Failed to fetch school holidays for ${isoCode}: ${error}`,
            );
          }
        } catch (error) {
          this.logger.error(
            `Failed to fetch holidays for ${isoCode}: ${error}`,
          );
        }
      }

      this.logger.log(
        `✅ Holidays sync complete! Saved ${totalHolidaysSaved} holiday-days across ${countries.length} countries`,
      );

      // 4. Easter Sunday — Nager.Date only returns it for Brandenburg (DE-BB), not as nationwide.
      //    However, Easter Sunday is one of the highest-traffic days for all European theme parks.
      //    We add it programmatically as a nationwide public holiday for all relevant countries.
      const easterCountries = countries.filter((c) =>
        ["DE", "AT", "CH", "NL", "BE", "FR", "PL", "CZ"].includes(c),
      );
      if (easterCountries.length > 0) {
        const easterHolidays: HolidayInput[] = [];
        for (let year = startYear; year <= endYear; year++) {
          const easterDate = this.calculateEasterSunday(year);
          for (const isoCode of easterCountries) {
            const dateStr = easterDate.toISOString().split("T")[0];
            easterHolidays.push({
              externalId: `computed:${isoCode}:${dateStr}:easter-sunday`,
              date: easterDate,
              name: "Easter Sunday",
              localName:
                isoCode === "DE"
                  ? "Ostersonntag"
                  : isoCode === "AT" || isoCode === "CH"
                    ? "Ostersonntag"
                    : isoCode === "NL"
                      ? "Eerste Paasdag"
                      : isoCode === "FR"
                        ? "Dimanche de Pâques"
                        : "Easter Sunday",
              country: isoCode,
              region: undefined,
              holidayType: "public",
              isNationwide: true,
            });
          }
        }
        await this.holidaysService.saveRawHolidays(easterHolidays);
        this.logger.log(
          `✅ Synced ${easterHolidays.length} Easter Sunday entries (programmatic) for ${easterCountries.join(", ")}`,
        );
      }

      // Cleanup old holidays (older than 3 years)
      const cleanupDate = new Date();
      cleanupDate.setFullYear(cleanupDate.getFullYear() - 3);
      const deletedCount =
        await this.holidaysService.deleteOldHolidays(cleanupDate);

      if (deletedCount > 0) {
        this.logger.log(
          `🗑️  Cleaned up ${deletedCount} old holidays (before ${cleanupDate.getFullYear()})`,
        );
      }

      // After everything is synced, trigger global gap filling to apply new holidays to schedules
      this.logger.log("🔄 Triggering global schedule gap filling...");
      await this.parkMetadataQueue.add("fill-all-gaps", {}, { priority: 5 });
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`Holidays sync failed: ${errorMessage}`);
      throw error;
    }
  }
}
