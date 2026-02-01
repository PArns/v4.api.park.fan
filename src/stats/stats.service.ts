import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between } from "typeorm";
import { ParkDailyStats } from "./entities/park-daily-stats.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { QueueType } from "../external-apis/themeparks/themeparks.types";
import { formatInParkTimezone } from "../common/utils/date.util";
import { Park } from "../parks/entities/park.entity";

@Injectable()
export class StatsService {
  private readonly logger = new Logger(StatsService.name);

  constructor(
    @InjectRepository(ParkDailyStats)
    private readonly statsRepository: Repository<ParkDailyStats>,
    @InjectRepository(QueueData)
    private readonly queueDataRepository: Repository<QueueData>,
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
  ) {}

  /**
   * Get cached daily stats for a date range
   */
  async getDailyStats(
    parkId: string,
    startDate: string,
    endDate: string,
  ): Promise<ParkDailyStats[]> {
    return this.statsRepository.find({
      where: {
        parkId,
        date: Between(startDate, endDate),
      },
      order: { date: "ASC" },
    });
  }

  /**
   * Calculate and store stats for a specific day
   * This is an expensive operation and should be backgrounded
   */
  async calculateAndStoreDailyStats(
    parkId: string,
    date: Date | string,
  ): Promise<ParkDailyStats | null> {
    try {
      const park = await this.parkRepository.findOne({ where: { id: parkId } });
      if (!park) throw new Error("Park not found");

      let dateStr: string;
      if (typeof date === "string") {
        dateStr = date;
      } else {
        dateStr = formatInParkTimezone(date, park.timezone);
      }

      // Define day boundaries in UTC based on park timezone
      // NOTE: This logic mimics CalendarService queue retrieval
      // Ideally we should use a shared utility for this date range construction
      // For now, we will query by string date matching if possible, or broad range

      // Strategy: Fetch all queue data for the park in a broad 48h window around the date
      // then filter by converted timestamp. This is safer for timezones.
      const queryStart = new Date(dateStr);
      queryStart.setDate(queryStart.getDate() - 1);
      const queryEnd = new Date(dateStr);
      queryEnd.setDate(queryEnd.getDate() + 2);

      const queueData = await this.queueDataRepository
        .createQueryBuilder("q")
        .select(["q.waitTime", "q.timestamp"])
        .innerJoin("q.attraction", "a")
        .where("a.parkId = :parkId", { parkId })
        .andWhere("q.timestamp >= :queryStart", { queryStart })
        .andWhere("q.timestamp < :queryEnd", { queryEnd })
        .andWhere("q.queueType = :qt", { qt: QueueType.STANDBY })
        .andWhere("q.waitTime IS NOT NULL")
        .andWhere("q.waitTime > 0")
        .getMany();

      // Filter for the specific date in park timezone
      const dayQueueData = queueData.filter(
        (q) => formatInParkTimezone(q.timestamp, park.timezone) === dateStr,
      );

      if (dayQueueData.length === 0) {
        // No data for this day
        // We still might want to store a record with nulls to prevent re-calc
        return this.upsertStats(parkId, dateStr, null, null, null);
      }

      // Calculate P90 and Max; cap max to avoid single bad queue_data poisoning ParkDailyStats/cache
      const waitTimes = dayQueueData.map((q) => q.waitTime!);
      const p90 = this.calculateP90(waitTimes);
      const rawMax = Math.max(...waitTimes);
      // Outlier protection: one bad row (e.g. 450 instead of 45) must not feed into cache.
      // Cap max at 3× P90 (typical peak vs typical level) or 120 min, whichever is higher but sane.
      const max =
        p90 > 0
          ? Math.min(rawMax, Math.max(120, p90 * 3))
          : Math.min(rawMax, 120);

      return this.upsertStats(parkId, dateStr, p90, max, dayQueueData.length);
    } catch (error) {
      this.logger.error(
        `Failed to calculate stats for ${parkId} on ${date}: ${error}`,
      );
      return null;
    }
  }

  private async upsertStats(
    parkId: string,
    date: string,
    p90: number | null,
    max: number | null,
    sampleSize: number | null,
  ): Promise<ParkDailyStats> {
    const existing = await this.statsRepository.findOne({
      where: { parkId, date },
    });

    if (existing) {
      existing.p90WaitTime = p90;
      existing.maxWaitTime = max;
      existing.metadata = {
        ...existing.metadata,
        sampleSize,
        lastUpdated: new Date(),
      };
      return this.statsRepository.save(existing);
    }

    const newItem = this.statsRepository.create({
      parkId,
      date,
      p90WaitTime: p90,
      maxWaitTime: max,
      metadata: { sampleSize, lastUpdated: new Date() },
    });
    return this.statsRepository.save(newItem);
  }

  private calculateP90(waitTimes: number[]): number {
    if (waitTimes.length === 0) return 0;
    const sorted = waitTimes.sort((a, b) => a - b);
    const index = Math.ceil(sorted.length * 0.9) - 1;
    return sorted[index];
  }
}
