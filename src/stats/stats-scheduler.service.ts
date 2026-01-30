import { Injectable, Logger } from "@nestjs/common";
import { Cron, CronExpression } from "@nestjs/schedule";
import { StatsService } from "./stats.service";
import { InjectRepository } from "@nestjs/typeorm";
import { Park } from "../parks/entities/park.entity";
import { Repository } from "typeorm";

@Injectable()
export class StatsSchedulerService {
  private readonly logger = new Logger(StatsSchedulerService.name);

  constructor(
    private readonly statsService: StatsService,
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
  ) {}

  /**
   * Update stats for "Today" every hour
   * This ensures the P90 value for the current day evolves as new data comes in.
   */
  @Cron(CronExpression.EVERY_HOUR)
  async updateTodayStats() {
    this.logger.log("Starting hourly stats update for TODAY...");
    const parks = await this.parkRepository.find({ select: ["id"] });
    const today = new Date();

    for (const park of parks) {
      await this.statsService.calculateAndStoreDailyStats(park.id, today);
    }
    this.logger.log(`Updated hourly stats for ${parks.length} parks.`);
  }

  /**
   * Finalize stats for "Yesterday" every day at 01:00 AM
   * This ensures we have a final, complete record for the previous day.
   */
  @Cron("0 1 * * *") // 01:00 AM
  async finalizeYesterdayStats() {
    this.logger.log("Starting final stats update for YESTERDAY...");
    const parks = await this.parkRepository.find({ select: ["id"] });
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    for (const park of parks) {
      await this.statsService.calculateAndStoreDailyStats(park.id, yesterday);
    }
    this.logger.log(`Finalized stats for ${parks.length} parks.`);
  }
}
