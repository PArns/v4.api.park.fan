import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import {
  TRIP_METRICS_RETENTION_DAYS,
  TripRequestMetricsService,
} from "../../trip-regions/metrics/trip-request-metrics.service";

/**
 * The nightly retention of the trip-region routes' metric rows
 * (docs/frontend/trip-regions.md#what-the-server-records): the rows past
 * {@link TRIP_METRICS_RETENTION_DAYS} days are deleted in batches. A failure
 * is rethrown, so Bull retries it and the failing-jobs sweep reports it.
 */
@Processor("trip-metrics")
export class TripMetricsMaintenanceProcessor {
  private readonly logger = new Logger(TripMetricsMaintenanceProcessor.name);

  constructor(private readonly metrics: TripRequestMetricsService) {}

  @Process("prune")
  async handlePrune(_job: Job): Promise<number> {
    try {
      const removed = await this.metrics.prune();
      // Quiet on zero, which is every night until the table is older than its retention.
      if (removed > 0) {
        this.logger.log(
          `🧹 Trip request metrics: deleted ${removed} row(s) past ${TRIP_METRICS_RETENTION_DAYS} days`,
        );
      }
      return removed;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`Trip request metrics prune failed: ${message}`);
      throw error;
    }
  }
}
