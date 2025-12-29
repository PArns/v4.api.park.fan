import { Processor, Process, InjectQueue } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job, Queue } from "bull";
import { HolidaysService } from "../../holidays/holidays.service";
import { NagerDateClient } from "../../external-apis/nager-date/nager-date.client";
import { OpenHolidaysClient } from "../../external-apis/open-holidays/open-holidays.client";
import { ParksService } from "../../parks/parks.service";
import { getCountryISOCode } from "../../common/constants/country-codes.constant";
import {
  PEAK_SEASONS_BY_COUNTRY,
  getPeakSeasonHolidays,
} from "../../common/peak-seasons";

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

  @Process("fetch-holidays")
  async handleSyncHolidays(_job: Job): Promise<void> {
    this.logger.log("🎉 Starting holidays sync...");

    try {
      // Get unique countries from parks
      const countries = await this.parksService.getUniqueCountries();
      this.logger.log(`Found ${countries.length} unique countries with parks`);

      if (countries.length === 0) {
        this.logger.warn("No countries found. Skipping holiday sync.");
        return;
      }

      // Fetch holidays for each country
      let totalHolidaysSaved = 0;
      const currentYear = new Date().getFullYear();

      // Fetch 2 years back + current year + 2 years ahead = 5 years total
      const startYear = currentYear - 2;
      const endYear = currentYear + 2;

      for (const country of countries) {
        try {
          // Convert country name to ISO code
          const isoCode = getCountryISOCode(country);

          if (!isoCode) {
            this.logger.warn(
              `No ISO code mapping found for country: ${country}, skipping`,
            );
            continue;
          }

          // this.logger.verbose(
          //   `Fetching holidays for ${country} (${isoCode}) (${startYear}-${endYear})...`,
          // );

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
          } catch (error) {
            this.logger.error(
              `Failed to fetch Nager public holidays for ${country}: ${error}`,
            );
          }

          // 2. School Holidays (OpenHolidays)
          // OpenHolidays API rejects large date ranges (>1 year) with 400.
          // Fetch year-by-year to avoid this.
          try {
            let totalSchoolForCountry = 0;

            for (let year = startYear; year <= endYear; year++) {
              const yearStart = `${year}-01-01`;
              const yearEnd = `${year}-12-31`;

              // Language: Use country code (often matches, e.g. DE, FR)
              const schoolHolidays =
                await this.openHolidaysClient.getSchoolHolidays(
                  isoCode,
                  isoCode, // language
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
              `Fetched school holidays for ${country}, expanded to ${totalSchoolForCountry} days.`,
            );
          } catch (error) {
            this.logger.error(
              `Failed to fetch school holidays for ${country}: ${error}`,
            );
          }
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : String(error);
          this.logger.error(
            `Failed to fetch holidays for ${country}: ${errorMessage}`,
          );
        }
      }

      this.logger.log(
        `✅ Holidays sync complete! Saved ${totalHolidaysSaved} holidays across ${countries.length} countries`,
      );

      // 3. Peak Seasons (Hardcoded for countries without API coverage)
      // Sync peak seasons for US, UK, JP, CN, CA, KR, AU, DK
      this.logger.log("📅 Syncing hardcoded peak seasons...");
      let peakSeasonCount = 0;

      for (const country of countries) {
        const isoCode = getCountryISOCode(country);
        if (!isoCode || !PEAK_SEASONS_BY_COUNTRY[isoCode]) continue;

        const peakHolidays = getPeakSeasonHolidays(isoCode, startYear, endYear);
        for (const holiday of peakHolidays) {
          try {
            const dateStr = holiday.date.toISOString().split("T")[0];
            const externalId = `peak:${isoCode}:${dateStr}:${holiday.name.replace(/\s+/g, "-").toLowerCase()}`;

            await this.holidaysService.upsertHoliday({
              externalId,
              date: holiday.date,
              name: holiday.name,
              localName: holiday.name,
              country: isoCode,
              region: undefined, // Nationwide
              holidayType: "school",
              isNationwide: true,
            });
            peakSeasonCount++;
          } catch (_error) {
            // Silently skip duplicates
          }
        }
      }

      if (peakSeasonCount > 0) {
        this.logger.log(
          `📅 Synced ${peakSeasonCount} peak season days for countries without API coverage`,
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
