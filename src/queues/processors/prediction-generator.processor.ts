import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { MLService } from "../../ml/ml.service";
import { ParksService } from "../../parks/parks.service";
import { CacheWarmupService } from "../services/cache-warmup.service";

/**
 * Prediction Generator Processor
 *
 * Generates and stores ML predictions for all attractions.
 *
 * Two types of predictions:
 * 1. Hourly: Next 24 hours (for today's planning)
 * 2. Daily: Next 30 days (for trip planning)
 *
 * Schedule:
 * - Hourly predictions: Every hour (fresh data)
 * - Daily predictions: Once per day at 1am (long-term forecast)
 *
 * Stored predictions are used for:
 * - Fast API responses (no ML service call needed)
 * - Accuracy tracking (feedback loop)
 */
@Processor("predictions")
export class PredictionGeneratorProcessor {
  private readonly logger = new Logger(PredictionGeneratorProcessor.name);

  constructor(
    private mlService: MLService,
    private parksService: ParksService,
    private cacheWarmupService: CacheWarmupService,
  ) {}

  @Process("generate-hourly")
  async handleGenerateHourly(_job: Job): Promise<void> {
    this.logger.log("⏰ Generating hourly predictions for all parks...");

    try {
      const allParks = await this.parksService.findAll();

      // OPTIMIZATION: Only generate hourly predictions for OPERATING parks
      // or parks opening soon / without schedule data (for morning planning)
      const parkIds = allParks.map((p) => p.id);
      const statusMap = await this.parksService.getBatchParkStatus(parkIds);

      // Filter to parks that are operating OR might operate soon
      // Note: Parks without schedules default to true via isParkOperatingToday
      const parks = [];
      for (const park of allParks) {
        const isOperating = statusMap.get(park.id) === "OPERATING";

        if (isOperating) {
          parks.push(park);
        } else {
          // Check if opens soon OR has no schedule (returns true by default)
          const shouldInclude = await this.parksService.isParkOperatingToday(
            park.id,
          );
          if (shouldInclude) {
            parks.push(park);
          }
        }
      }

      this.logger.log(
        `Filtered to ${parks.length}/${allParks.length} parks for hourly predictions`,
      );

      let totalPredictions = 0;
      let successParks = 0;
      let failedParks = 0;

      for (const park of parks) {
        try {
          // Get hourly predictions for next 24h
          const response = await this.mlService.getParkPredictions(
            park.id,
            "hourly",
          );

          if (response.predictions.length > 0) {
            // OPTIMIZATION: Delete old hourly predictions for this park before storing new ones
            // This prevents duplicate predictions for the same time slots
            await this.mlService.deduplicatePredictions(park.id, "hourly");

            // Store predictions in database
            await this.mlService.storePredictions(response.predictions);
            totalPredictions += response.predictions.length;
            successParks++;
          } else {
            this.logger.warn(
              `No hourly predictions returned for park: ${park.name}`,
            );
          }
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : String(error);
          this.logger.warn(
            `Failed to generate hourly predictions for park ${park.name}: ${errorMessage}`,
          );
          failedParks++;
        }
      }

      this.logger.log(
        `✅ Hourly predictions complete: ${totalPredictions} predictions for ${successParks}/${parks.length} parks`,
      );
      if (failedParks > 0) {
        this.logger.warn(
          `⚠️  ${failedParks} parks failed to generate predictions`,
        );
      }

      // Cache Warmup: Prepopulate cache for parks opening in next 12h
      // Important for trip planning - users look at tomorrow's data
      this.logger.log("🔥 Starting cache warmup for upcoming parks...");
      try {
        await this.cacheWarmupService.warmupUpcomingParks();
      } catch (error: unknown) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.warn(`Cache warmup failed: ${errorMessage}`);
        // Don't throw - warmup failure shouldn't fail the entire generation
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`Hourly prediction generation failed: ${errorMessage}`);
      throw error;
    }
  }

  @Process("generate-daily")
  async handleGenerateDaily(_job: Job): Promise<void> {
    this.logger.log("📅 Generating daily predictions for all parks...");

    try {
      const parks = await this.parksService.findAll();
      let totalPredictions = 0;
      let successParks = 0;
      let failedParks = 0;

      for (const park of parks) {
        try {
          // Get daily predictions for next 30 days
          const response = await this.mlService.getParkPredictions(
            park.id,
            "daily",
          );

          if (response.predictions.length > 0) {
            // OPTIMIZATION: Delete old daily predictions for this park before storing new ones
            await this.mlService.deduplicatePredictions(park.id, "daily");

            // Store predictions in database
            await this.mlService.storePredictions(response.predictions);
            totalPredictions += response.predictions.length;
            successParks++;
          } else {
            this.logger.warn(
              `No daily predictions returned for park: ${park.name}`,
            );
          }
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : String(error);
          this.logger.warn(
            `Failed to generate daily predictions for park ${park.name}: ${errorMessage}`,
          );
          failedParks++;
        }
      }

      this.logger.log(
        `✅ Daily predictions complete: ${totalPredictions} predictions for ${successParks}/${parks.length} parks`,
      );
      if (failedParks > 0) {
        this.logger.warn(
          `⚠️  ${failedParks} parks failed to generate predictions`,
        );
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`Daily prediction generation failed: ${errorMessage}`);
      throw error;
    }
  }

  @Process("cleanup-old")
  async handleCleanupOld(_job: Job): Promise<void> {
    this.logger.log("🧹 Cleaning up old predictions...");

    try {
      const now = new Date();

      // Delete hourly predictions older than 7 days
      const hourlyCutoff = new Date(now);
      hourlyCutoff.setDate(hourlyCutoff.getDate() - 7);

      const deletedHourly = await this.mlService.deleteOldPredictions(
        "hourly",
        hourlyCutoff,
      );

      // Delete daily predictions older than 90 days
      const dailyCutoff = new Date(now);
      dailyCutoff.setDate(dailyCutoff.getDate() - 90);

      const deletedDaily = await this.mlService.deleteOldPredictions(
        "daily",
        dailyCutoff,
      );

      const totalDeleted = deletedHourly + deletedDaily;

      this.logger.log(
        `✅ Cleanup complete: Removed ${totalDeleted.toLocaleString()} old predictions (hourly: ${deletedHourly.toLocaleString()}, daily: ${deletedDaily.toLocaleString()})`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`Prediction cleanup failed: ${errorMessage}`);
      throw error;
    }
  }
}
