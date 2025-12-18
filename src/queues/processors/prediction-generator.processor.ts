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
      const parks = await this.parksService.findAll();
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
}
