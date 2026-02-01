import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { AnalyticsService } from "../../analytics/analytics.service";
import { ParksService } from "../../parks/parks.service";

/**
 * P50 Baseline Processor
 *
 * Calculates and stores P50 (median) baselines for parks and attractions.
 *
 * Strategy:
 * - Runs daily at 3am (after percentile calculation at 2am)
 * - Identifies headliner attractions using 3-tier adaptive system
 * - Calculates P50 baseline from headliners only (548-day window)
 * - Stores in database and caches in Redis
 *
 * Benefits:
 * - More intuitive crowd levels (P50 = expected/typical day)
 * - Filters out low-demand attractions
 * - Adapts to parks of all sizes (major, medium, small)
 *
 * Schedule: Daily at 3am (after queue-data-aggregates update)
 */
@Processor("p50-baseline")
export class P50BaselineProcessor {
  private readonly logger = new Logger(P50BaselineProcessor.name);

  constructor(
    private readonly analyticsService: AnalyticsService,
    private readonly parksService: ParksService,
  ) {}

  /**
   * Daily job: Calculate P50 baselines for all parks
   */
  @Process("calculate-park-baselines")
  async handleCalculateParkBaselines(_job: Job): Promise<void> {
    this.logger.log("🎯 Calculating P50 baselines for all parks...");

    try {
      const parks = await this.parksService.findAll();
      let successCount = 0;
      let failureCount = 0;

      for (const park of parks) {
        try {
          // Step 1: Identify headliners using 3-tier system
          const headliners = await this.analyticsService.identifyHeadliners(
            park.id,
          );

          if (headliners.length === 0) {
            this.logger.warn(
              `No headliners identified for park ${park.name} (${park.id})`,
            );
            failureCount++;
            continue;
          }

          // Step 2: Calculate P50 baseline from headliners
          const baseline = await this.analyticsService.calculateP50Baseline(
            park.id,
            headliners,
          );

          if (baseline.p50 === 0) {
            this.logger.warn(
              `P50 baseline is 0 for park ${park.name} (${park.id}) - insufficient data`,
            );
            failureCount++;
            continue;
          }

          // Step 3: Save to database and cache
          await this.analyticsService.saveP50Baselines(
            park.id,
            baseline,
            headliners,
          );

          this.logger.log(
            `✅ ${park.name}: ${baseline.p50}min (${headliners.length} headliners, tier: ${baseline.tier}, confidence: ${baseline.confidence})`,
          );
          successCount++;
        } catch (error) {
          this.logger.error(
            `Failed to calculate P50 baseline for park ${park.name} (${park.id})`,
            error instanceof Error ? error.stack : String(error),
          );
          failureCount++;
        }
      }

      this.logger.log(
        `✅ Park P50 baseline calculation complete: ${successCount} succeeded, ${failureCount} failed`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to calculate park P50 baselines: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }

  /**
   * Daily job: Calculate P50 baselines for all attractions
   */
  @Process("calculate-attraction-baselines")
  async handleCalculateAttractionBaselines(_job: Job): Promise<void> {
    this.logger.log("🎯 Calculating P50 baselines for all attractions...");

    try {
      const parks = await this.parksService.findAll();
      let successCount = 0;
      let failureCount = 0;

      for (const park of parks) {
        // Get all attractions for this park
        const attractions = park.attractions || [];

        for (const attraction of attractions) {
          try {
            // Calculate P50 baseline for this attraction
            const baseline = await this.analyticsService.calculateAttractionP50(
              attraction.id,
            );

            if (baseline.p50 === 0) {
              this.logger.warn(
                `P50 baseline is 0 for attraction ${attraction.name} (${attraction.id}) - insufficient data`,
              );
              failureCount++;
              continue;
            }

            // Save to database and cache
            await this.analyticsService.saveAttractionP50Baseline(
              attraction.id,
              park.id,
              baseline,
            );

            this.logger.debug(
              `✅ ${attraction.name}: ${baseline.p50}min (headliner: ${baseline.isHeadliner}, confidence: ${baseline.confidence})`,
            );
            successCount++;
          } catch (error) {
            this.logger.error(
              `Failed to calculate P50 baseline for attraction ${attraction.name} (${attraction.id})`,
              error instanceof Error ? error.stack : String(error),
            );
            failureCount++;
          }
        }
      }

      this.logger.log(
        `✅ Attraction P50 baseline calculation complete: ${successCount} succeeded, ${failureCount} failed`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to calculate attraction P50 baselines: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }

  /**
   * Manual job: Backfill P50 baselines for specific park
   */
  @Process("backfill-park-baseline")
  async handleBackfillParkBaseline(
    job: Job<{ parkId: string }>,
  ): Promise<void> {
    const { parkId } = job.data;
    this.logger.log(`🔄 Backfilling P50 baseline for park ${parkId}...`);

    try {
      // Identify headliners
      const headliners = await this.analyticsService.identifyHeadliners(parkId);

      if (headliners.length === 0) {
        this.logger.warn(`No headliners identified for park ${parkId}`);
        return;
      }

      // Calculate P50 baseline
      const baseline = await this.analyticsService.calculateP50Baseline(
        parkId,
        headliners,
      );

      if (baseline.p50 === 0) {
        this.logger.warn(
          `P50 baseline is 0 for park ${parkId} - insufficient data`,
        );
        return;
      }

      // Save to database and cache
      await this.analyticsService.saveP50Baselines(
        parkId,
        baseline,
        headliners,
      );

      this.logger.log(
        `✅ Backfilled P50 baseline for park ${parkId}: ${baseline.p50}min (${headliners.length} headliners)`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to backfill P50 baseline for park ${parkId}: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }
}
