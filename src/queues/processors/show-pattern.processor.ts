import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { ShowsService } from "../../shows/shows.service";

/**
 * The nightly rebuild of "what does this show's day usually look like".
 *
 * It exists because no source publishes showtimes ahead of the current day —
 * checked at ThemeParks.wiki, which answers with today's times and a tail of
 * entries it never cleared, some from 2022 — so a planner asking about October
 * has nothing to render unless we project from what we have seen.
 *
 * Nightly rather than on demand: the aggregation reads an eight-week window of a
 * hypertable (350,000 showtime entries for one park alone) and takes 5.7 s
 * across every park. That is nothing once a day and unthinkable on a request,
 * which is the whole reason the pattern lives in its own small table.
 *
 * At 04:15, before the 04:30 hourly-history rollup and after the night's feeds
 * have settled — the window is eight weeks long, so the exact minute does not
 * matter; not colliding with the heavier jobs does.
 */
@Processor("show-patterns")
export class ShowPatternProcessor {
  private readonly logger = new Logger(ShowPatternProcessor.name);

  constructor(private readonly showsService: ShowsService) {}

  @Process("rebuild-patterns")
  async handleRebuild(_job: Job): Promise<void> {
    const started = Date.now();
    try {
      const { patterns, shows } =
        await this.showsService.rebuildSchedulePatterns();
      this.logger.log(
        `✅ Show patterns: ${patterns} pattern(s) for ${shows} show(s) in ${Date.now() - started}ms`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`Show pattern rebuild failed: ${message}`);
      throw error;
    }
  }
}
