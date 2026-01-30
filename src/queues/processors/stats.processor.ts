import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { StatsService } from "../../stats/stats.service";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";
import { ParksService } from "../../parks/parks.service";

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
        const date = new Date();
        date.setDate(date.getDate() - 1);
        const yesterday = formatInParkTimezone(date, park.timezone);

        await this.statsService.calculateAndStoreDailyStats(park.id, yesterday);
      }

      this.logger.log(`Finalized yesterday's stats for ${parks.length} parks.`);
    } catch (error) {
      this.logger.error("Failed to finalize yesterday's stats", error);
      throw error;
    }
  }
}
