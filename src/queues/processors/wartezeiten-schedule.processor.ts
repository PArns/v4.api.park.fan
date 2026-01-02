import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { WartezeitenClient } from "../../external-apis/wartezeiten/wartezeiten.client";
import { ParksService } from "../../parks/parks.service";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Park } from "../../parks/entities/park.entity";

/**
 * Wartezeiten Schedule Processor
 *
 * Fetches operating hours (opening times) from Wartezeiten.app once per day.
 * Separates this from the frequent wait times sync to reduce API calls.
 *
 * Scheduled: Daily at 6:00 AM
 * API Calls: ~30 per day (once for each Wartezeiten park)
 */
@Processor("wartezeiten-schedule")
export class WartezeitenScheduleProcessor {
  private readonly logger = new Logger(WartezeitenScheduleProcessor.name);

  constructor(
    private readonly wartezeitenClient: WartezeitenClient,
    private readonly parksService: ParksService,
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
  ) {}

  @Process("fetch-opening-times")
  async handleFetchOpeningTimes(_job: Job): Promise<void> {
    this.logger.log("🕐 Starting daily Wartezeiten opening times sync...");

    try {
      // Get all parks that have Wartezeiten data
      const allParks = await this.parkRepository
        .createQueryBuilder("park")
        .where("park.wartezeitenEntityId IS NOT NULL")
        .getMany();

      if (allParks.length === 0) {
        this.logger.warn("No parks with Wartezeiten data found.");
        return;
      }

      // Filter parks: Skip only if they have Wiki data AND already have schedule data for today
      // This allows Wartezeiten to fill gaps when Wiki doesn't provide schedules
      const parks: typeof allParks = [];
      const today = new Date();
      today.setHours(0, 0, 0, 0);

      for (const park of allParks) {
        if (park.wikiEntityId) {
          // Park has Wiki - check if today's schedule exists from Wiki
          const todaySchedule = await this.parksService.getTodaySchedule(
            park.id,
          );

          // If Wiki schedule exists for today, skip Wartezeiten (Wiki is more reliable)
          if (todaySchedule && todaySchedule.length > 0) {
            this.logger.verbose(
              `Skipping ${park.name} - Wiki schedule data exists`,
            );
            continue;
          }

          // No Wiki schedule found - allow Wartezeiten as fallback
          this.logger.verbose(
            `Including ${park.name} - No Wiki schedule, using Wartezeiten as fallback`,
          );
        }

        parks.push(park);
      }

      if (parks.length === 0) {
        this.logger.log(
          "All Wartezeiten parks already have Wiki schedule data.",
        );
        return;
      }

      this.logger.log(
        `Found ${parks.length} parks to sync from Wartezeiten (${allParks.length - parks.length} skipped due to existing Wiki data)`,
      );

      let successCount = 0;
      let skipCount = 0;

      for (const park of parks) {
        try {
          const openingTimes = await this.wartezeitenClient.getOpeningTimes(
            park.wartezeitenEntityId!,
          );

          if (openingTimes && openingTimes.length > 0) {
            const today = openingTimes[0];

            if (today.opened_today) {
              // Validate that closingTime is after openingTime
              const openingTime = new Date(today.open_from);
              const closingTime = new Date(today.closed_from);

              if (closingTime <= openingTime) {
                this.logger.warn(
                  `⚠️  Invalid schedule data for ${park.name}: closingTime (${today.closed_from}) is before or equal to openingTime (${today.open_from}). Skipping update.`,
                );
                skipCount++;
                continue;
              }

              // Persist to schedule table
              const scheduleUpdate = {
                date: today.open_from, // ISO string acts as date
                type: "OPERATING",
                openingTime: today.open_from,
                closingTime: today.closed_from,
                description: "Wartezeiten.app daily sync",
              };

              await this.parksService.saveScheduleData(park.id, [
                scheduleUpdate,
              ]);
              successCount++;

              this.logger.verbose(
                `✅ Updated schedule for ${park.name}: ${today.open_from} - ${today.closed_from}`,
              );
            } else {
              this.logger.verbose(`${park.name} is closed today`);
              skipCount++;
            }
          }
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : String(error);

          // Check for global rate limit block
          if (errorMessage.includes("Global Rate Limit")) {
            this.logger.warn(
              "⏳ Wartezeiten API is globally blocked. Skipping remaining parks.",
            );
            break; // Exit loop early
          }

          this.logger.warn(
            `Failed to fetch opening times for ${park.name}: ${errorMessage}`,
          );
        }
      }

      this.logger.log(
        `✅ Opening times sync complete! ${successCount} updated, ${skipCount} skipped (closed)`,
      );
    } catch (error) {
      this.logger.error("❌ Opening times sync failed", error);
      throw error; // Bull will retry
    }
  }
}
