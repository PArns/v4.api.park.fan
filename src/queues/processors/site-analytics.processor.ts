import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { SiteAnalyticsCollectService } from "../../site-analytics/site-analytics-collect.service";

/**
 * The site statistic's two daily jobs (docs/frontend/site-analytics.md).
 *
 * `snapshot-visitors` writes the last two finished UTC days' unique visitors
 * from their Redis HyperLogLogs to `site_visitor_days`; a HyperLogLog lives
 * three days past its own, so a missed night is caught up the next.
 * `prune-old-days` deletes rows older than 400 days from the four tables.
 */
@Processor("site-analytics")
export class SiteAnalyticsProcessor {
  private readonly logger = new Logger(SiteAnalyticsProcessor.name);

  constructor(private readonly collector: SiteAnalyticsCollectService) {}

  @Process("snapshot-visitors")
  async handleSnapshotVisitors(_job: Job): Promise<void> {
    const written = await this.collector.snapshotVisitors();
    // A day of traffic writes seven rows; none means Redis lost the keys.
    if (written === 0) {
      this.logger.warn("No visitor HyperLogLog found for the last two days");
    }
  }

  @Process("prune-old-days")
  async handlePrune(_job: Job): Promise<void> {
    const deleted = await this.collector.prune();
    const total = Object.values(deleted).reduce((a, b) => a + b, 0);
    if (total > 0) {
      this.logger.log(
        `🧹 Pruned site statistic rows: ${JSON.stringify(deleted)}`,
      );
    }
  }
}
