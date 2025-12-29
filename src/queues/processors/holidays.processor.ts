import { Processor, Process, InjectQueue } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job, Queue } from "bull";
import { HolidaysService } from "../../holidays/holidays.service";
import { NagerDateClient } from "../../external-apis/nager-date/nager-date.client";
import { OpenHolidaysClient } from "../../external-apis/open-holidays/open-holidays.client";
import { ParksService } from "../../parks/parks.service";
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
          } catch (error) {
            this.logger.error(
              `Failed to fetch Nager public holidays for ${isoCode}: ${error}`,
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

      // 3. Peak Seasons (Hardcoded for countries without API coverage)
      this.logger.log("📅 Syncing hardcoded peak seasons...");
      let totalPeakHolidaysSaved = 0;
      const peakHolidaysToUpsert: any[] = [];

      for (const isoCode of countries) {
        if (!PEAK_SEASONS_BY_COUNTRY[isoCode]) continue;

        const peakHolidays = getPeakSeasonHolidays(isoCode, startYear, endYear);
        for (const holiday of peakHolidays) {
          const dateStr = holiday.date.toISOString().split("T")[0];
          const externalId = `peak:${isoCode}:${dateStr}:${holiday.name.replace(/\s+/g, "-").toLowerCase()}`;

          peakHolidaysToUpsert.push({
            externalId,
            date: holiday.date,
            name: holiday.name,
            localName: holiday.name,
            country: isoCode,
            region: undefined,
            holidayType: "school",
            isNationwide: true,
          });
        }
      }

      if (peakHolidaysToUpsert.length > 0) {
        // Bulk upsert in batches of 500
        for (let i = 0; i < peakHolidaysToUpsert.length; i += 500) {
          const batch = peakHolidaysToUpsert.slice(i, i + 500);
          await this.holidaysService.saveRawHolidays(batch);
          totalPeakHolidaysSaved += batch.length;
        }
        this.logger.log(
          `✅ Synced ${totalPeakHolidaysSaved} peak season days for countries without API coverage`,
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
