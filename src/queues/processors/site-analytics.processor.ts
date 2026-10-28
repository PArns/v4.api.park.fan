import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { SiteAnalyticsStorageService } from "../../site-analytics/site-analytics-storage.service";

/**
 * The site statistic's jobs (docs/frontend/site-analytics.md), all of them
 * insert-only or whole-chunk, but for the last step of `drop-old-chunks`:
 *
 * - `flush`, every five minutes: first the visits idle for 30 minutes are
 *   ended with an exit each, then the finished Redis windows go into
 *   `site_pageviews` and `site_events`, each window on its own, so one that
 *   fails does not hold up the rest;
 * - `snapshot-visitors`, four times in the hour after midnight: yesterday's
 *   unique visitors, and each hour's, from their HyperLogLogs into
 *   `site_visitor_days` and `site_visitor_hours`, once;
 * - `rollup-days`, four times a day: the finished days summed into the daily
 *   tables the long reports read, each cuboid of each day once;
 * - `drop-old-chunks`, nightly: the hourly chunks past 90 days, once all their
 *   days are rolled up, and the daily ones past 400, dropped whole; then the
 *   small tables (visitor counts, roll-up markers) lose their old rows.
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
        `Flushed ${written.windows} window(s): ${written.pageviewRows} page-view and ${written.eventRows} event rows, ${written.visitsClosed} visit(s) ended, in ${Date.now() - started} ms`,
      );
    }
    if (written.failed > 0) {
      this.logger.warn(
        `${written.failed} window(s) not written, retried next run`,
      );
    }
  }

  @Process("snapshot-visitors")
  async handleSnapshotVisitors(_job: Job): Promise<void> {
    const { found, written, hoursWritten } =
      await this.storage.snapshotVisitors();
    // A day of traffic leaves seven HyperLogLogs; none means Redis lost them.
    if (found === 0) {
      this.logger.warn("No visitor HyperLogLog found for yesterday");
    } else if (written > 0 || hoursWritten > 0) {
      this.logger.log(
        `Wrote ${written} daily and ${hoursWritten} hourly visitor counts`,
      );
    }
  }

  @Process("rollup-days")
  async handleRollupDays(_job: Job): Promise<void> {
    const started = Date.now();
    const rolled = await this.storage.rollupDays();
    if (rolled.days > 0) {
      this.logger.log(
        `Rolled up ${rolled.days} day(s): ${rolled.cuboids} cuboid(s), ${rolled.rows} rows, in ${Date.now() - started} ms`,
      );
    }
  }

  @Process("drop-old-chunks")
  async handleDropOldChunks(_job: Job): Promise<void> {
    const { dropped, skipped, pruned } = await this.storage.dropOldChunks();
    const total =
      Object.values(dropped).reduce((a, b) => a + b, 0) +
      Object.values(pruned).reduce((a, b) => a + b, 0);
    if (total > 0) {
      this.logger.log(
        `🧹 Dropped site statistic chunks ${JSON.stringify(dropped)}, deleted rows ${JSON.stringify(pruned)}`,
      );
    }
    // The safe chunks are gone by now; failing the job is what puts the rest
    // in front of the admin (the failing-jobs report), as a day that does
    // not add up does for `rollup-days`.
    if (skipped.length > 0) {
      const days = skipped.flatMap((chunk) => chunk.days).sort();
      throw new Error(
        `${skipped.length} hourly chunk(s) kept, ${days.length} day(s) without a complete roll-up ` +
          `(${days.slice(0, 5).join(", ")}${days.length > 5 ? ", …" : ""}): ` +
          "the chunks go once `rollup-days` has rolled those days up",
      );
    }
  }
}
