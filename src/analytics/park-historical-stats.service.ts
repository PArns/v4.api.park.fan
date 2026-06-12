import { Injectable, Inject, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { subDays, subYears } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { ParkDailyStats } from "../stats/entities/park-daily-stats.entity";
import { safeJsonParse } from "../common/utils/json.util";
import { QueueDataAggregate } from "./entities/queue-data-aggregate.entity";
import { Park } from "../parks/entities/park.entity";
import {
  ParkHistoricalStatsDto,
  MonthStatDto,
  DayOfWeekStatDto,
  TopAttractionStatDto,
} from "./dto/park-historical-stats.dto";
import { CrowdLevel } from "../common/types/crowd-level.type";
import { determineCrowdLevel } from "../common/utils/crowd-level.util";

/** Response schema version — bump when the contract changes (see DTO). */
const SCHEMA_VERSION = 2;
/** Default minimum sample days before the section is considered displayable. */
const DEFAULT_MIN_SAMPLE_DAYS = 30;

@Injectable()
export class ParkHistoricalStatsService {
  private readonly logger = new Logger(ParkHistoricalStatsService.name);
  private readonly CACHE_TTL = 24 * 60 * 60; // 24 hours

  constructor(
    @InjectRepository(ParkDailyStats)
    private readonly dailyStatsRepo: Repository<ParkDailyStats>,
    @InjectRepository(QueueDataAggregate)
    private readonly aggregateRepo: Repository<QueueDataAggregate>,
    @Inject(REDIS_CLIENT)
    private readonly redis: Redis,
  ) {}

  async getParkHistoricalStats(
    park: Park,
    years: number,
    topN = 10,
    minSampleDays = DEFAULT_MIN_SAMPLE_DAYS,
  ): Promise<ParkHistoricalStatsDto> {
    // v2: occupancy-relative avgCrowdLevel + additive meta fields. The topN /
    // minSampleDays inputs change the payload, so they're part of the key.
    const cacheKey = `park:historical-stats:v2:${park.id}:${years}:${topN}:${minSampleDays}`;
    const cached = safeJsonParse<ParkHistoricalStatsDto>(
      await this.redis.get(cacheKey),
    );
    if (cached) return cached;

    const result = await this.compute(park, years, topN, minSampleDays);
    await this.redis.set(
      cacheKey,
      JSON.stringify(result),
      "EX",
      this.CACHE_TTL,
    );
    return result;
  }

  private async compute(
    park: Park,
    years: number,
    topN: number,
    minSampleDays: number,
  ): Promise<ParkHistoricalStatsDto> {
    // Use park timezone so "yesterday" and the start boundary are correct
    // for parks in any UTC offset. date-fns subDays operates in wall-clock
    // time, then formatInTimeZone renders the result in the park's calendar.
    const now = new Date();
    const endStr = formatInTimeZone(
      subDays(now, 1),
      park.timezone,
      "yyyy-MM-dd",
    );
    const startStr = formatInTimeZone(
      subYears(subDays(now, 1), years),
      park.timezone,
      "yyyy-MM-dd",
    );

    const [byMonthRaw, byDowRaw, topAttrRaw, typicalDayPeak] =
      await Promise.all([
        this.queryByMonth(park.id, startStr, endStr),
        this.queryByDayOfWeek(park.id, startStr, endStr),
        this.queryTopAttractions(park.id, startStr, endStr, topN),
        this.queryTypicalDayPeak(park.id, startStr, endStr),
      ]);

    const byMonth: MonthStatDto[] = byMonthRaw.map((r) => ({
      month: Number(r.month),
      avgCrowdScore: this.toCrowdScore(Number(r.avg_wait_p50)),
      avgCrowdLevel: this.toCrowdLevel(Number(r.avg_wait_p90), typicalDayPeak),
      avgWaitP50: Math.round(Number(r.avg_wait_p50)),
      avgWaitP90: Math.round(Number(r.avg_wait_p90)),
      sampleDays: Number(r.sample_days),
    }));

    const byDayOfWeek: DayOfWeekStatDto[] = byDowRaw.map((r) => ({
      dayOfWeek: Number(r.day_of_week),
      avgCrowdScore: this.toCrowdScore(Number(r.avg_wait_p50)),
      avgCrowdLevel: this.toCrowdLevel(Number(r.avg_wait_p90), typicalDayPeak),
      avgWaitP50: Math.round(Number(r.avg_wait_p50)),
      avgWaitP90: Math.round(Number(r.avg_wait_p90)),
      sampleDays: Number(r.sample_days),
    }));

    const topAttractions: TopAttractionStatDto[] = topAttrRaw.map((r, i) => ({
      attractionSlug: r.slug as string,
      attractionName: r.name as string,
      avgWaitP50: Math.round(Number(r.avg_p50)),
      avgWaitP90: Math.round(Number(r.avg_p90)),
      sampleDays: Number(r.sample_days),
      rank: i + 1,
    }));

    const totalSampleDays = byMonth.reduce((sum, m) => sum + m.sampleDays, 0);

    return {
      byMonth,
      byDayOfWeek,
      topAttractions,
      meta: {
        parkSlug: park.slug,
        dataFrom: startStr,
        dataTo: endStr,
        totalSampleDays,
        windowYears: years,
        displayable: totalSampleDays >= minSampleDays,
        generatedAt: new Date().toISOString(),
        schemaVersion: SCHEMA_VERSION,
      },
    };
  }

  private async queryByMonth(
    parkId: string,
    startDate: string,
    endDate: string,
  ): Promise<Record<string, unknown>[]> {
    // Column names are camelCase (TypeORM default, no naming strategy) — must be quoted.
    return this.dailyStatsRepo.manager.query(
      `SELECT
         EXTRACT(MONTH FROM date::date)::int   AS month,
         AVG("p50WaitTime")                    AS avg_wait_p50,
         AVG("p90WaitTime")                    AS avg_wait_p90,
         COUNT(*)::int                          AS sample_days
       FROM park_daily_stats
       WHERE "parkId" = $1::uuid
         AND date BETWEEN $2 AND $3
         AND "p50WaitTime" IS NOT NULL
         AND "p90WaitTime" IS NOT NULL
       GROUP BY month
       ORDER BY month`,
      [parkId, startDate, endDate],
    );
  }

  private async queryByDayOfWeek(
    parkId: string,
    startDate: string,
    endDate: string,
  ): Promise<Record<string, unknown>[]> {
    return this.dailyStatsRepo.manager.query(
      `SELECT
         EXTRACT(DOW FROM date::date)::int     AS day_of_week,
         AVG("p50WaitTime")                    AS avg_wait_p50,
         AVG("p90WaitTime")                    AS avg_wait_p90,
         COUNT(*)::int                          AS sample_days
       FROM park_daily_stats
       WHERE "parkId" = $1::uuid
         AND date BETWEEN $2 AND $3
         AND "p50WaitTime" IS NOT NULL
         AND "p90WaitTime" IS NOT NULL
       GROUP BY day_of_week
       ORDER BY day_of_week`,
      [parkId, startDate, endDate],
    );
  }

  private async queryTopAttractions(
    parkId: string,
    startDate: string,
    endDate: string,
    topN: number,
  ): Promise<Record<string, unknown>[]> {
    // queue_data_aggregates uses camelCase columns: "parkId", "attractionId", p50, p90.
    // p50/p90 are lowercase (single-word) in the entity, so no quoting needed there.
    return this.aggregateRepo.manager.query(
      `SELECT
         a.slug,
         a.name,
         AVG(qda.p50)                               AS avg_p50,
         AVG(qda.p90)                               AS avg_p90,
         COUNT(DISTINCT DATE(qda.hour))::int         AS sample_days
       FROM queue_data_aggregates qda
       JOIN attractions a ON a.id::text = qda."attractionId"
       WHERE qda."parkId" = $1
         AND qda.hour >= $2::date
         AND qda.hour <  ($3::date + INTERVAL '1 day')
       GROUP BY a.id, a.slug, a.name
       ORDER BY avg_p90 DESC
       LIMIT $4`,
      [parkId, startDate, endDate, topN],
    );
  }

  /**
   * Park's typical-day-peak baseline = the median over operating days of the
   * daily peak (P90) wait. This is the same denominator the calendar/daily
   * crowd level uses (see crowd-level.type.ts), so historical and live
   * classifications stay on one scale.
   */
  private async queryTypicalDayPeak(
    parkId: string,
    startDate: string,
    endDate: string,
  ): Promise<number> {
    const rows: Record<string, unknown>[] =
      await this.dailyStatsRepo.manager.query(
        `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY "p90WaitTime")
                AS typical_day_peak
         FROM park_daily_stats
         WHERE "parkId" = $1::uuid
           AND date BETWEEN $2 AND $3
           AND "p90WaitTime" IS NOT NULL`,
        [parkId, startDate, endDate],
      );
    const raw = rows[0]?.typical_day_peak;
    return raw == null ? 0 : Number(raw);
  }

  /**
   * Maps average P50 wait time to a 1.0–5.0 crowd score.
   * 10 min → 1.0, 50 min → 5.0 (linear). Clamped to [1.0, 5.0].
   *
   * Kept for backwards compatibility (sorting/tooltips). Prefer avgCrowdLevel
   * for display — it is occupancy-relative and consistent across endpoints.
   */
  private toCrowdScore(avgWaitP50: number): number {
    const raw = avgWaitP50 / 10;
    return Math.round(Math.min(Math.max(raw, 1.0), 5.0) * 10) / 10;
  }

  /**
   * Maps a period's average daily-peak wait to a CrowdLevel, occupancy-relative
   * to the park's own typical-day-peak baseline (100% = a statistically typical
   * day). Uses the same 6-tier thresholds as the live endpoint, so a structurally
   * quiet park still reads "high" on its own busy days. Falls back to "moderate"
   * when the park has no baseline yet (brand-new).
   */
  private toCrowdLevel(avgWaitP90: number, typicalDayPeak: number): CrowdLevel {
    if (!typicalDayPeak || typicalDayPeak <= 0) return "moderate";
    return determineCrowdLevel((avgWaitP90 / typicalDayPeak) * 100);
  }
}
