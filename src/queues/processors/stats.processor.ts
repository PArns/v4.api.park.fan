import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { StatsService } from "../../stats/stats.service";
import {
  getCurrentDateInTimezone,
  getYesterdayDateInTimezone,
} from "../../common/utils/date.util";
import { ParksService } from "../../parks/parks.service";
import { format, subDays, parseISO, isValid } from "date-fns";

@Processor("stats")
export class StatsProcessor {
  private readonly logger = new Logger(StatsProcessor.name);

  constructor(
    private readonly statsService: StatsService,
    private readonly parksService: ParksService,
  ) {}

  @Process("update-today-stats")
  async handleUpdateTodayStats(_job: Job): Promise<void> {
    this.logger.debug("Starting update-today-stats job...");
    try {
      const parks = await this.parksService.findAll();

      for (const park of parks) {
        const today = getCurrentDateInTimezone(park.timezone);
        await this.statsService.calculateAndStoreDailyStats(park.id, today);
      }

      this.logger.debug(`Updated stats for ${parks.length} parks.`);
    } catch (error) {
      this.logger.error("Failed to update today's stats", error);
      throw error;
    }
  }

  @Process("finalize-yesterday-stats")
  async handleFinalizeYesterdayStats(_job: Job): Promise<void> {
    this.logger.debug("Starting finalize-yesterday-stats job...");
    try {
      const parks = await this.parksService.findAll();

      for (const park of parks) {
        // Calculate "yesterday" in park's timezone
        const yesterday = getYesterdayDateInTimezone(park.timezone);

        await this.statsService.calculateAndStoreDailyStats(park.id, yesterday);
      }

      this.logger.log(`Finalized yesterday's stats for ${parks.length} parks.`);
    } catch (error) {
      this.logger.error("Failed to finalize yesterday's stats", error);
      throw error;
    }
  }

  @Process("backfill-stats")
  async handleBackfillStats(
    job: Job<{ days?: number; parkId?: string; startDate?: string }>,
  ): Promise<void> {
    const { days = 30, parkId, startDate } = job.data;
    this.logger.log(
      `🔄 Starting stats backfill: days=${days}, parkId=${parkId || "ALL"}, startDate=${startDate || "today"}`,
    );

    try {
      const parks = parkId
        ? [await this.parksService.findById(parkId)]
        : await this.parksService.findAll();

      let processedCount = 0;
      const baseDate = startDate ? parseISO(startDate) : new Date();

      if (!isValid(baseDate)) {
        throw new Error(`Invalid startDate provided: ${startDate}`);
      }

      const totalSteps = parks.length * days;

      for (const park of parks) {
        if (!park) continue;
        this.logger.log(`[Backfill] Starting park: ${park.name} (${park.id})`);

        for (let i = 0; i < days; i++) {
          const targetDate = subDays(baseDate, i);
          const dateStr = format(targetDate, "yyyy-MM-dd");

          this.logger.debug(
            `[Backfill] Processing ${park.name} - ${dateStr} (${i + 1}/${days})`,
          );
          await this.statsService.calculateAndStoreDailyStats(park.id, dateStr);
          processedCount++;

          if (processedCount % 5 === 0 || processedCount === totalSteps) {
            const progress = Math.round((processedCount / totalSteps) * 100);
            await job.progress(progress);
            this.logger.log(
              `[Backfill] Progress: ${progress}% (${processedCount}/${totalSteps} records total)`,
            );
          }
        }
        this.logger.log(`[Backfill] Finished park: ${park.name}`);
      }

      this.logger.log(
        `✅ Completed backfill: ${processedCount} daily stats records recalculated across ${parks.length} parks.`,
      );
    } catch (error) {
      this.logger.error("Failed to complete stats backfill", error);
      throw error;
    }
  }
}
