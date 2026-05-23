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
 * - Park baselines = avg-of-per-headliner-{P50,P90}, plus the
 *   **typical-day-peak** (median over operating days of the AVG-of-per-
 *   headliner daily P90) computed in the same pass and stored atomically.
 * - Writes `park_p50_baselines` (incl. the `typicalDayPeak` column) +
 *   `park_p90_baselines` for parks and the matching pair for attractions;
 *   primes Redis cache for each.
 *
 * Crowd-level regimes the API surfaces:
 * - Calendar/daily: a day's AVG-of-per-headliner-P90 ÷ the **typical-day-
 *   peak** baseline (100% = a typical day = moderate). See
 *   docs/analytics/crowd-level-typical-day-peak.md.
 * - Live overview / getCurrentOccupancy and hourly predictions: ratio-vs-P50
 *   (current peak ÷ P50 baseline). P50 also feeds the ML occupancy feature.
 * P90 is computed for free (carries confidence/metadata) but is no longer
 * the calendar reference.
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

      // Process parks in batches of 5 — `identifyHeadliners` is a
      // heavy 548-day PERCENTILE_CONT scan per park, so we can't
      // fan out unbounded without saturating the DB. BATCH_SIZE=5
      // mirrors the wait-times processor pattern (worked there too).
      // Wall-time drops from sum(parks) sequential to
      // ceil(parks/5) parallel batches; per-park error isolation
      // preserved via the inner try/catch.
      const BATCH_SIZE = 5;
      for (let i = 0; i < parks.length; i += BATCH_SIZE) {
        const batch = parks.slice(i, i + BATCH_SIZE);
        const results = await Promise.all(
          batch.map(async (park) => {
            try {
              const hasData =
                await this.analyticsService.parkHasQueueDataInWindow(
                  park.id,
                  WINDOW_DAYS,
                );
              if (!hasData) {
                this.logger.debug(
                  `Skipping ${park.name}: no queue_data (STANDBY, OPERATING) in last ${WINDOW_DAYS} days`,
                );
                return "skipped" as const;
              }

              const headliners = await this.analyticsService.identifyHeadliners(
                park.id,
              );

              if (headliners.length === 0) {
                this.logger.warn(
                  `No headliners identified for park ${park.name} (${park.id})`,
                );
                return "failed" as const;
              }

              const baseline = await this.analyticsService.calculateP50Baseline(
                park.id,
                headliners,
              );

              if (baseline.p50 === 0) {
                this.logger.warn(
                  `P50 baseline is 0 for park ${park.name} (${park.id}) - insufficient data`,
                );
                return "failed" as const;
              }

              await this.analyticsService.saveP50Baselines(
                park.id,
                baseline,
                headliners,
              );

              this.logger.log(
                `✅ ${park.name}: P50=${baseline.p50}min typical-day-peak=${baseline.typicalDayPeak}min (${headliners.length} headliners, tier: ${baseline.tier}, confidence: ${baseline.confidence})`,
              );
              return "success" as const;
            } catch (error) {
              this.logger.error(
                `Failed to calculate P50 baseline for park ${park.name} (${park.id})`,
                error instanceof Error ? error.stack : String(error),
              );
              return "failed" as const;
            }
          }),
        );

        for (const outcome of results) {
          if (outcome === "success") successCount++;
          else if (outcome === "failed") failureCount++;
          // "skipped" doesn't count toward either bucket.
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

      // Save to database and cache (P50/P90 + typical-day-peak atomically)
      await this.analyticsService.saveP50Baselines(
        parkId,
        baseline,
        headliners,
      );

      this.logger.log(
        `✅ Backfilled baseline for park ${parkId}: P50=${baseline.p50}min typical-day-peak=${baseline.typicalDayPeak}min (${headliners.length} headliners)`,
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
