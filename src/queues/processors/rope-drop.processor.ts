import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { AnalyticsService } from "../../analytics/analytics.service";
import { ParksService } from "../../parks/parks.service";

/**
 * Rope-Drop Processor
 *
 * Computes the "is it worth rope-dropping this headliner?" recommendation for
 * every park's tier1/tier2 headliners and stores it in `attraction_rope_drop`
 * (+ Redis). Runs daily at 5:15 AM, after `attraction-hourly-history` (4:30 AM)
 * has written yesterday's slots.
 *
 * Two-layer model (see plan / docs/analytics): shape (opening-relative ratio
 * curve, pooled over history) + levels (absolute minutes on a trailing window,
 * weekend/weekday buckets). Daily recompute = the "window adapts to the current
 * season" requirement — `worth` flips across the year by design.
 *
 * Source data is the precomputed hourly-history slots + schedule opening times,
 * so this is one GROUP/LATERAL query per park, not a fresh PERCENTILE scan.
 * Parks in batches of 5 (mirrors p50-baseline) to avoid DB saturation.
 */
@Processor("rope-drop")
export class RopeDropProcessor {
  private readonly logger = new Logger(RopeDropProcessor.name);

  constructor(
    private readonly analyticsService: AnalyticsService,
    private readonly parksService: ParksService,
  ) {}

  @Process("calculate-rope-drop")
  async handleCalculateRopeDrop(_job: Job): Promise<void> {
    this.logger.log(
      "🏃 Calculating rope-drop recommendations for all parks...",
    );

    try {
      const parks = await this.parksService.findAll();
      let parksWithData = 0;
      let totalStored = 0;
      let totalWorth = 0;
      let failureCount = 0;

      const BATCH_SIZE = 5;
      for (let i = 0; i < parks.length; i += BATCH_SIZE) {
        const batch = parks.slice(i, i + BATCH_SIZE);
        const results = await Promise.all(
          batch.map(async (park) => {
            try {
              const computed =
                await this.analyticsService.computeRopeDropForPark(
                  park.id,
                  park.timezone || "UTC",
                );
              if (computed.size === 0) return { stored: 0, worth: 0 };

              const worth = Array.from(computed.values()).filter(
                (r) => r.worth,
              ).length;
              const stored = await this.analyticsService.saveRopeDropBatch(
                park.id,
                computed,
              );
              return { stored, worth };
            } catch (error) {
              this.logger.error(
                `Failed to compute rope-drop for park ${park.name} (${park.id})`,
                error instanceof Error ? error.stack : String(error),
              );
              return null;
            }
          }),
        );

        for (const outcome of results) {
          if (outcome === null) {
            failureCount++;
            continue;
          }
          if (outcome.stored > 0) {
            parksWithData++;
            totalStored += outcome.stored;
            totalWorth += outcome.worth;
          }
        }
      }

      this.logger.log(
        `✅ Rope-drop complete: ${totalStored} headliners across ${parksWithData} parks (${totalWorth} worth rope-dropping, ${failureCount} parks failed)`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to calculate rope-drop recommendations: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }
}
