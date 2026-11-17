import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import type { Job } from "bull";
import { DisplayConsistencyService } from "./display-consistency.service";

/** The job's name on the `analytics` queue. */
export const CHECK_DISPLAY_CONSISTENCY_JOB = "check-display-consistency";

/** Parks the WARN names; the rest are in the admin's list. */
const NAMED_PARKS = 10;

/**
 * The display-consistency check over the parks in a running season and the
 * most read park pages, twice a day. One WARN naming the parks with a mismatch
 * or a surface it could not read, or saying no park was checked; the quiet
 * line only when every park was read whole and agrees. A surface that cannot
 * be read compares nothing, so counting mismatches alone would let a run in
 * which every read failed pass as clean.
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
    const checked = summary.parks.length;
    if (checked === 0) {
      this.logger.warn(
        `⚠️ Display consistency: no park checked (${seconds}s); neither a running season nor the most read parks named one`,
      );
      return;
    }
    const flagged = summary.parks.filter(
      (park) => park.mismatches > 0 || park.failures > 0,
    );
    if (flagged.length === 0) {
      this.logger.log(
        `✅ Display consistency: ${checked} parks read on every surface, and they agree on every must-be-equal pair (${seconds}s)`,
      );
      return;
    }
    const count = (key: "mismatches" | "failures") =>
      flagged.filter((park) => park[key] > 0).length;
    const named = flagged
      .slice(0, NAMED_PARKS)
      .map((park) =>
        [
          park.name,
          park.mismatches > 0 && `${park.mismatches} mismatches`,
          park.failures > 0 && `${park.failures} unreadable`,
        ]
          .filter(Boolean)
          .join(" "),
      );
    this.logger.warn(
      `⚠️ Display consistency: ${summary.mismatches} mismatches in ${count("mismatches")} and ` +
        `${summary.failures} unreadable surfaces in ${count("failures")} of ${checked} parks ` +
        `(${seconds}s): ${named.join(", ")}. Details: GET /v1/admin/display-consistency`,
    );
  }
}
