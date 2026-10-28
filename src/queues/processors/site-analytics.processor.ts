import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { SiteAnalyticsStorageService } from "../../site-analytics/site-analytics-storage.service";

/**
 * The site statistic's jobs (docs/frontend/site-analytics.md), all of them
 * insert-only or whole-chunk:
 *
 * - `flush`, every five minutes: the finished Redis windows into
 *   `site_pageviews` and `site_events`;
 * - `snapshot-visitors`, nightly: the last two finished UTC days' unique
 *   visitors from their HyperLogLogs into `site_visitor_days`, once;
 * - `drop-old-chunks`, nightly: chunks past 400 days, dropped whole.
 */
@Processor("site-analytics")
export class SiteAnalyticsProcessor {
  private readonly logger = new Logger(SiteAnalyticsProcessor.name);

  constructor(private readonly storage: SiteAnalyticsStorageService) {}

  @Process("flush")
  async handleFlush(_job: Job): Promise<void> {
    const started = Date.now();
    const written = await this.storage.flush();
    if (written.windows > 0) {
      this.logger.debug(
        `Flushed ${written.windows} window(s): ${written.pageviewRows} page-view and ${written.eventRows} event rows in ${Date.now() - started} ms`,
      );
    }
  }

  @Process("snapshot-visitors")
  async handleSnapshotVisitors(_job: Job): Promise<void> {
    const { found, written } = await this.storage.snapshotVisitors();
    // A day of traffic leaves seven HyperLogLogs; none means Redis lost them.
    if (found === 0) {
      this.logger.warn("No visitor HyperLogLog found for the last two days");
    } else if (written > 0) {
      this.logger.log(`Wrote ${written} daily visitor counts`);
    }
  }

  @Process("drop-old-chunks")
  async handleDropOldChunks(_job: Job): Promise<void> {
    const dropped = await this.storage.dropOldChunks();
    const total = Object.values(dropped).reduce((a, b) => a + b, 0);
    if (total > 0) {
      this.logger.log(
        `🧹 Dropped site statistic chunks: ${JSON.stringify(dropped)}`,
      );
    }
  }
}
