import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { AnalyticsService } from "../../analytics/analytics.service";
import { ParksService } from "../../parks/parks.service";
import { AttractionsService } from "../../attractions/attractions.service";

/**
 * P50 + P90 Baseline Processor
 *
 * Calculates and stores both the median (P50) and the peak (P90) wait-time
 * baselines for parks and attractions. PostgreSQL produces both percentiles
 * from a single PERCENTILE_CONT sort, so the additional P90 row is free
 * on top of the existing P50 job.
 *
 * Strategy:
 * - Runs daily — parks at 3 AM, attractions at 4 AM (after the
 *   percentile-aggregates job at 2 AM).
 * - Identifies headliner attractions via the 3-tier adaptive system.
 * - Calculates per-headliner P50 / P90 over the 548-day sliding window.
 * - Park baselines = avg-of-per-headliner-{P50,P90} (consistent with the
 *   peak-vs-peak and avg-vs-avg comparisons the API surfaces).
 * - Writes `park_p50_baselines` + `park_p90_baselines` for parks and the
 *   matching pair for attractions; primes Redis cache for each.
 *
 * Why both percentiles: the API surfaces crowd levels as peak-vs-peak
 * (P90 ÷ P90 baseline). P50 is kept for legacy avg-shaped consumers and
 * as a graceful fallback when a P90 row hasn't been calculated yet.
 */
@Processor("p50-baseline")
export class P50BaselineProcessor {
  private readonly logger = new Logger(P50BaselineProcessor.name);

  constructor(
    private readonly analyticsService: AnalyticsService,
    private readonly parksService: ParksService,
    private readonly attractionsService: AttractionsService,
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

      const WINDOW_DAYS = 548;
      for (const park of parks) {
        try {
          // Skip parks with no queue_data in window (reduces log noise; headliner logic would fail anyway)
          const hasData = await this.analyticsService.parkHasQueueDataInWindow(
            park.id,
            WINDOW_DAYS,
          );
          if (!hasData) {
            this.logger.debug(
              `Skipping ${park.name}: no queue_data (STANDBY, OPERATING) in last ${WINDOW_DAYS} days`,
            );
            continue;
          }

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

      // One GROUP BY query per *park* (returning P50/P90 per attraction)
      // replaces the previous per-attraction PERCENTILE_CONT scan. The
      // 548-day window scan still happens, but PostgreSQL can produce
      // results for an entire park's attractions from a single sort. A
      // 200-park / 10k-attraction system used to fire ~10k heavy queries
      // here every night; now it fires ~200, all of which are I/O-bound
      // on the same hot index.
      for (const park of parks) {
        try {
          const perAttraction =
            await this.analyticsService.calculateAttractionP50P90ForPark(
              park.id,
              park.timezone || "UTC",
            );

          const rows = Array.from(perAttraction.entries()).map(
            ([attractionId, baseline]) => ({
              attractionId,
              ...baseline,
            }),
          );

          if (rows.length === 0) {
            this.logger.debug(
              `Skipping ${park.name} — no attractions with qualifying data`,
            );
            continue;
          }

          const { p50Saved } =
            await this.analyticsService.saveAttractionP50P90BaselinesBatch(
              park.id,
              rows,
            );
          successCount += p50Saved;
          failureCount += rows.length - p50Saved;

          this.logger.debug(
            `✅ ${park.name}: ${p50Saved}/${rows.length} attraction baselines saved`,
          );
        } catch (error) {
          this.logger.error(
            `Failed to calculate P50/P90 baselines for park ${park.name} (${park.id})`,
            error instanceof Error ? error.stack : String(error),
          );
          // Count every attraction in the park as failed so the summary
          // log reflects scope, not the single park-level error.
          failureCount += 1;
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
    const WINDOW_DAYS = 548;
    this.logger.log(`🔄 Backfilling P50 baseline for park ${parkId}...`);

    try {
      const hasData = await this.analyticsService.parkHasQueueDataInWindow(
        parkId,
        WINDOW_DAYS,
      );
      if (!hasData) {
        this.logger.warn(
          `Skipping backfill: no queue_data (STANDBY, OPERATING) in last ${WINDOW_DAYS} days for park ${parkId}`,
        );
        return;
      }

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
