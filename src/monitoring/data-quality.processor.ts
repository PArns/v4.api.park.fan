import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { DataQualityMonitorService } from "./data-quality-monitor.service";

/** Bull hands a processor whatever was thrown; only the message is useful here. */
const asMessage = (e: unknown): string =>
  e instanceof Error ? e.message : String(e);

/**
 * Daily sweep for the silent-failure modes described in
 * DataQualityMonitorService.
 *
 * Logs a WARN naming what it found and a single quiet line when clean, the same
 * shape as the ride-profile term audit — a job that is red every night is a job
 * people learn to ignore.
 */
@Processor("analytics")
export class DataQualityProcessor {
  private readonly logger = new Logger(DataQualityProcessor.name);

  constructor(private readonly monitor: DataQualityMonitorService) {}

  @Process("monitor-data-quality")
  async handleMonitorDataQuality(_job: Job): Promise<void> {
    // Each detector catches its own throw. They are independent findings, and a
    // Promise.all that rejects takes the other two down with it — which is the
    // failure mode the module was written for: detect-seasonal threw on every
    // run for 73 days and nothing said so.
    const [clusters, silentParks, failing] = await Promise.all([
      this.monitor.findSilencedClusters().catch((e) => {
        this.logger.error(`Silenced-cluster check failed: ${asMessage(e)}`);
        return [];
      }),
      this.monitor.findScheduledButSilentParks().catch((e) => {
        this.logger.error(`Scheduled-but-silent check failed: ${asMessage(e)}`);
        return [];
      }),
      this.monitor.findFailingJobs().catch((e) => {
        this.logger.error(`Failing-job check failed: ${asMessage(e)}`);
        return [];
      }),
    ]);

    for (const c of clusters) {
      this.logger.warn(
        `🔇 ${c.parkName}: ${c.attractionCount} attractions stopped reporting on ${c.lastOperating} ` +
          `(e.g. ${c.sampleNames.slice(0, 3).join(", ")}). Dropped feed, or a section closed for the season?`,
      );
    }

    for (const p of silentParks) {
      const seen = p.lastReading
        ? `no reading since ${p.lastReading}`
        : "never read at all";
      this.logger.warn(
        `📵 ${p.parkName}: ${p.attractionCount} attractions, ${seen}, yet ` +
          `${p.operatingDaysAhead} operating day(s) scheduled in the next week ` +
          `and a calendar running to ${p.lastScheduledDay}. ` +
          `Feed dropped, or a schedule nobody can confirm?`,
      );
    }

    for (const f of failing) {
      this.logger.warn(
        `💥 ${f.queue}/${f.jobName}: ${f.failures} failed run(s), last ${f.lastFailedAt ?? "unknown"} — ${f.lastReason}`,
      );
    }

    if (
      clusters.length === 0 &&
      silentParks.length === 0 &&
      failing.length === 0
    ) {
      this.logger.log(
        "✅ Data quality clean: no silenced clusters, no silent scheduled parks, no failing jobs",
      );
    }
  }
}
