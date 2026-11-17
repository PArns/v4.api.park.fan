import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import type { Job } from "bull";
import { DisplayConsistencyService } from "./display-consistency.service";

/** The job's name on the `analytics` queue. */
export const CHECK_DISPLAY_CONSISTENCY_JOB = "check-display-consistency";

/**
 * The daily display-consistency check over the parks in a running season and
 * the most read park pages. One WARN naming the parks whose surfaces disagree,
 * one quiet line when none do.
 */
@Processor("analytics")
export class DisplayConsistencyProcessor {
  private readonly logger = new Logger(DisplayConsistencyProcessor.name);

  constructor(private readonly displayConsistency: DisplayConsistencyService) {}

  @Process(CHECK_DISPLAY_CONSISTENCY_JOB)
  async handle(_job: Job): Promise<void> {
    const startedAt = Date.now();
    const summary = await this.displayConsistency.runDaily();
    const seconds = Math.round((Date.now() - startedAt) / 1000);
    const flagged = summary.parks.filter((park) => park.mismatches > 0);
    if (flagged.length === 0) {
      this.logger.log(
        `✅ Display consistency: ${summary.parks.length} parks agree on every must-be-equal pair (${seconds}s)`,
      );
      return;
    }
    this.logger.warn(
      `⚠️ Display consistency: ${summary.mismatches} mismatches in ${flagged.length} of ${summary.parks.length} parks (${seconds}s): ` +
        flagged
          .slice(0, 10)
          .map((park) => `${park.name} ${park.mismatches}`)
          .join(", ") +
        ". Details: GET /v1/admin/display-consistency",
    );
  }
}
