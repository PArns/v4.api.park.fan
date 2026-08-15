import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { DataQualityMonitorService } from "./data-quality-monitor.service";

/**
 * Daily sweep for the two silent-failure modes described in
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
    const [clusters, failing] = await Promise.all([
      this.monitor.findSilencedClusters(),
      this.monitor.findFailingJobs(),
    ]);

    for (const c of clusters) {
      this.logger.warn(
        `🔇 ${c.parkName}: ${c.attractionCount} attractions stopped reporting on ${c.lastOperating} ` +
          `(e.g. ${c.sampleNames.slice(0, 3).join(", ")}). Dropped feed, or a section closed for the season?`,
      );
    }

    for (const f of failing) {
      this.logger.warn(
        `💥 ${f.queue}/${f.jobName}: ${f.failures} failed run(s), last ${f.lastFailedAt ?? "unknown"} — ${f.lastReason}`,
      );
    }

    if (clusters.length === 0 && failing.length === 0) {
      this.logger.log(
        "✅ Data quality clean: no silenced clusters, no failing jobs",
      );
    }
  }
}
