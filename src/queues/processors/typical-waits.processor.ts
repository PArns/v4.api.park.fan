import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { AnalyticsService } from "../../analytics/analytics.service";
import { ParksService } from "../../parks/parks.service";

/**
 * Typical-Waits Processor
 *
 * Precomputes the typical (P50) vs busy (P90) peak-wait stats for every park's
 * tier1/tier2 headliners and stores them in `attraction_typical_waits`, so the
 * park response — and thus the statically-prerendered ride-page shell — can
 * serve them without a per-request percentile scan. Mirrors the rope-drop
 * processor (daily, parks in batches of 5). Reuses the 24h-cached per-attraction
 * aggregate, so a run is cheap after the daily percentile rebuild.
 */
@Processor("typical-waits")
export class TypicalWaitsProcessor {
  private readonly logger = new Logger(TypicalWaitsProcessor.name);

  constructor(
    private readonly analyticsService: AnalyticsService,
    private readonly parksService: ParksService,
  ) {}

  @Process("calculate-typical-waits")
  async handleCalculateTypicalWaits(_job: Job): Promise<void> {
    this.logger.log(
      "⏳ Calculating typical-waits for all parks' headliners...",
    );

    try {
      const parks = await this.parksService.findAll();
      let parksWithData = 0;
      let totalStored = 0;
      let failureCount = 0;

      const BATCH_SIZE = 5;
      for (let i = 0; i < parks.length; i += BATCH_SIZE) {
        const batch = parks.slice(i, i + BATCH_SIZE);
        const results = await Promise.all(
          batch.map(async (park) => {
            try {
              const computed =
                await this.analyticsService.computeTypicalWaitsForPark(
                  park.id,
                  park.timezone || "UTC",
                  park.countryCode || "",
                );
              const stored = await this.analyticsService.saveTypicalWaitsBatch(
                park.id,
                computed,
              );
              return { stored };
            } catch (error) {
              this.logger.error(
                `Failed to compute typical-waits for park ${park.name} (${park.id})`,
                error instanceof Error ? error.stack : String(error),
              );
              return null;
            }
          }),
        );

        for (const r of results) {
          if (r === null) {
            failureCount++;
            continue;
          }
          if (r.stored > 0) parksWithData++;
          totalStored += r.stored;
        }
      }

      this.logger.log(
        `⏳ Typical-waits done: ${totalStored} rows across ${parksWithData} parks (${failureCount} failures)`,
      );
    } catch (error) {
      this.logger.error(
        "Typical-waits calculation failed",
        error instanceof Error ? error.stack : String(error),
      );
    }
  }
}
