import { Processor, Process } from "@nestjs/bull";
import { Logger, OnModuleInit } from "@nestjs/common";
import { Job } from "bull";
import { MLService } from "../../ml/ml.service";
import { ParksService } from "../../parks/parks.service";
import { CacheWarmupService } from "../services/cache-warmup.service";

/**
 * Prediction Generator Processor
 *
 * Generates and stores ML predictions for all attractions.
 * With BATCH processing to ensure stability.
 */
@Processor("predictions")
export class PredictionGeneratorProcessor implements OnModuleInit {
  private readonly logger = new Logger(PredictionGeneratorProcessor.name);

  constructor(
    private mlService: MLService,
    private parksService: ParksService,
    private cacheWarmupService: CacheWarmupService,
  ) {
    this.logger.log("🔧 PredictionGeneratorProcessor CONSTRUCTED");
  }

  onModuleInit() {
    this.logger.log("🔧 PredictionGeneratorProcessor INITIALIZED");
  }

  @Process("generate-hourly")
  async handleGenerateHourly(_job: Job): Promise<void> {
    this.logger.log(
      "⏰ Generating hourly predictions for all parks (BATCHED)...",
    );

    try {
      const allParks = await this.parksService.findAll();

      // OPTIMIZATION: Only generate hourly predictions for OPERATING parks
      // or parks opening soon / without schedule data (for morning planning)
      const parkIds = allParks.map((p) => p.id);
      const statusMap = await this.parksService.getBatchParkStatus(parkIds);

      // Filter to parks that are operating OR might operate soon
      const parks = [];
      for (const park of allParks) {
        const status = statusMap.get(park.id);
        const isOperating = status === "OPERATING";

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

      // BATCHED PROCESSING (Size 5)
      // Process in small batches to prevent 429s (Weather API) and Timeouts (ML Service)
      const BATCH_SIZE = 5;

      for (let i = 0; i < parks.length; i += BATCH_SIZE) {
        const batch = parks.slice(i, i + BATCH_SIZE);
        const batchNum = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(parks.length / BATCH_SIZE);

        this.logger.log(
          `Processing batch ${batchNum}/${totalBatches} (${batch.length} parks)...`,
        );

        // Process batch in parallel
        await Promise.all(
          batch.map(async (park) => {
            try {
              // Get hourly predictions for next 24h
              const response = await this.mlService.getParkPredictions(
                park.id,
                "hourly",
              );

              if (response.predictions.length > 0) {
                // OPTIMIZATION: Delete old hourly predictions for this park before storing new ones
                await this.mlService.deduplicatePredictions(park.id, "hourly");

                // Store predictions in database
                await this.mlService.storePredictions(response.predictions);
                totalPredictions += response.predictions.length;
                successParks++;
              } else {
                this.logger.debug(
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
          }),
        );

        // Small delay between batches to be nice to external APIs
        if (i + BATCH_SIZE < parks.length) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
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
      this.logger.log("🔥 Starting cache warmup for upcoming parks...");
      try {
        await this.cacheWarmupService.warmupUpcomingParks();
      } catch (error: unknown) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.warn(`Cache warmup failed: ${errorMessage}`);
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
    this.logger.log(
      "📅 Generating daily predictions for all parks (BATCHED)...",
    );

    try {
      const parks = await this.parksService.findAll();
      let totalPredictions = 0;
      let successParks = 0;
      let failedParks = 0;

      // BATCHED PROCESSING (Size 5)
      const BATCH_SIZE = 5;

      for (let i = 0; i < parks.length; i += BATCH_SIZE) {
        const batch = parks.slice(i, i + BATCH_SIZE);
        this.logger.log(
          `Processing daily batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(parks.length / BATCH_SIZE)}...`,
        );

        await Promise.all(
          batch.map(async (park) => {
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
                this.logger.debug(
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
          }),
        );

        if (i + BATCH_SIZE < parks.length) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
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
