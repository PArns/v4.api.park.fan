import { Processor, Process, InjectQueue } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job, Queue } from "bull";
import { HolidaysService } from "../../holidays/holidays.service";
import { NagerDateClient } from "../../external-apis/nager-date/nager-date.client";
import { ParksService } from "../../parks/parks.service";
import { getCountryISOCode } from "../../common/constants/country-codes.constant";

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

          this.logger.log(
            `Fetching holidays for ${country} (${isoCode}) (${startYear}-${endYear})...`,
          );

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

          this.logger.log(
            `✅ Saved ${savedCount} holidays for ${country} (${isoCode})`,
          );
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : String(error);
          this.logger.error(
            `Failed to fetch holidays for ${country}: ${errorMessage}`,
          );
          // Continue with next country
        }
      }

      this.logger.log(
        `✅ Holidays sync complete! Saved ${totalHolidaysSaved} holidays across ${countries.length} countries`,
      );

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
