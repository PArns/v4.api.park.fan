import { Injectable, Inject, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { subDays, subYears } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
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
import { rateOrUnknown } from "../common/utils/crowd-level.util";

/** Response schema version — bump when the contract changes (see DTO). */
const SCHEMA_VERSION = 2;
/** Default minimum sample days before the section is considered displayable. */
const DEFAULT_MIN_SAMPLE_DAYS = 30;
/** Drop noisy single-sample hours from the aggregate scan. */
const MIN_SAMPLES_PER_HOUR = 2;

/** One operating day's headliner-only values (peak + typical wait). */
interface DayValue {
  month: number;
  dow: number;
  dayValueP90: number;
  dayValueP50: number;
}

@Injectable()
export class ParkHistoricalStatsService {
  private readonly logger = new Logger(ParkHistoricalStatsService.name);
  private readonly CACHE_TTL = 24 * 60 * 60; // 24 hours

  constructor(
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

    // Headliner-only, matching the calendar's crowd-level semantic (a park's
    // crowd level is its headliners, not the family-ride dilution).
    const headlinerIds = await this.getHeadlinerIds(park.id);

    const [dayValues, topAttrRaw] = await Promise.all([
      this.queryHeadlinerDayValues(
        park.id,
        park.timezone,
        startStr,
        endStr,
        headlinerIds,
      ),
      this.queryTopAttractions(park.id, startStr, endStr, topN),
    ]);

    // typical-day-peak = median over operating days of the day_value
    // (AVG-of-headliner daily peaks). Computed from the SAME source as the
    // numerators below, so a statistically typical day ≈ 100% = moderate.
    const typicalDayPeak = this.median(dayValues.map((d) => d.dayValueP90));

    const byMonth: MonthStatDto[] = this.groupAvg(
      dayValues,
      (d) => d.month,
    ).map((g) => ({
      month: g.key,
      avgCrowdScore: this.toCrowdScore(g.avgP50),
      avgCrowdLevel: this.toCrowdLevel(g.avgP90, typicalDayPeak),
      avgWaitP50: Math.round(g.avgP50),
      avgWaitP90: Math.round(g.avgP90),
      sampleDays: g.sampleDays,
    }));

    const byDayOfWeek: DayOfWeekStatDto[] = this.groupAvg(
      dayValues,
      (d) => d.dow,
    ).map((g) => ({
      dayOfWeek: g.key,
      avgCrowdScore: this.toCrowdScore(g.avgP50),
      avgCrowdLevel: this.toCrowdLevel(g.avgP90, typicalDayPeak),
      avgWaitP50: Math.round(g.avgP50),
      avgWaitP90: Math.round(g.avgP90),
      sampleDays: g.sampleDays,
    }));

    const topAttractions: TopAttractionStatDto[] = topAttrRaw.map((r, i) => ({
      attractionSlug: r.slug as string,
      attractionName: r.name as string,
      avgWaitP50: Math.round(Number(r.avg_p50)),
      avgWaitP90: Math.round(Number(r.avg_p90)),
      sampleDays: Number(r.sample_days),
      rank: i + 1,
    }));

    const totalSampleDays = dayValues.length;

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

  /**
   * Headliner attraction IDs for the park (as text, to match the text
   * `attractionId` column on queue_data_aggregates). Empty ⇒ caller falls back
   * to all attractions, mirroring the calendar's headliner fallback.
   */
  private async getHeadlinerIds(parkId: string): Promise<string[]> {
    const rows: Array<{ id: string }> = await this.aggregateRepo.manager.query(
      `SELECT "attractionId"::text AS id
       FROM headliner_attractions
       WHERE "parkId" = $1::uuid`,
      [parkId],
    );
    return rows.map((r) => r.id);
  }

  /**
   * One row per operating day: the headliner-only day_value = AVG across
   * headliners of that ride's daily peak (P90 of the day's hourly P90s) and
   * the day's typical wait (AVG of hourly P50s). Computed from the hourly
   * pre-aggregation (queue_data_aggregates), restricted to headliners.
   */
  private async queryHeadlinerDayValues(
    parkId: string,
    timezone: string,
    startDate: string,
    endDate: string,
    headlinerIds: string[],
  ): Promise<DayValue[]> {
    const rows: Array<Record<string, unknown>> =
      await this.aggregateRepo.manager.query(
        `WITH per_attraction_day AS (
           SELECT
             (qda.hour AT TIME ZONE $2)::date                     AS day,
             qda."attractionId"                                   AS aid,
             PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qda.p90) AS day_peak,
             AVG(qda.p50)                                         AS day_typical
           FROM queue_data_aggregates qda
           WHERE qda."parkId" = $1
             AND qda.hour >= $3::date
             AND qda.hour <  ($4::date + INTERVAL '1 day')
             AND qda."sampleCount" >= $5
             AND ($6::text[] IS NULL OR qda."attractionId" = ANY($6))
           GROUP BY day, aid
         )
         SELECT
           EXTRACT(MONTH FROM day)::int AS month,
           EXTRACT(DOW FROM day)::int   AS dow,
           AVG(day_peak)                AS day_value_p90,
           AVG(day_typical)             AS day_value_p50
         FROM per_attraction_day
         GROUP BY day
         ORDER BY day`,
        [
          parkId,
          timezone,
          startDate,
          endDate,
          MIN_SAMPLES_PER_HOUR,
          headlinerIds.length > 0 ? headlinerIds : null,
        ],
      );

    return rows.map((r) => ({
      month: Number(r.month),
      dow: Number(r.dow),
      dayValueP90: Number(r.day_value_p90),
      dayValueP50: Number(r.day_value_p50),
    }));
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

  /** Linear-interpolation median over an array (0 when empty). */
  private median(values: number[]): number {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = (sorted.length - 1) / 2;
    const lo = Math.floor(mid);
    const hi = Math.ceil(mid);
    return lo === hi ? sorted[lo] : (sorted[lo] + sorted[hi]) / 2;
  }

  /** Group day-values by a key (month / day-of-week) and average each bucket. */
  private groupAvg(
    rows: DayValue[],
    keyOf: (d: DayValue) => number,
  ): Array<{
    key: number;
    avgP50: number;
    avgP90: number;
    sampleDays: number;
  }> {
    const buckets = new Map<number, { p50: number; p90: number; n: number }>();
    for (const d of rows) {
      const k = keyOf(d);
      const e = buckets.get(k) ?? { p50: 0, p90: 0, n: 0 };
      e.p50 += d.dayValueP50;
      e.p90 += d.dayValueP90;
      e.n += 1;
      buckets.set(k, e);
    }
    return [...buckets.entries()]
      .map(([key, e]) => ({
        key,
        avgP50: e.p50 / e.n,
        avgP90: e.p90 / e.n,
        sampleDays: e.n,
      }))
      .sort((a, b) => a.key - b.key);
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
   * to the park's typical-day-peak (100% = a statistically typical day). Same
   * 6-tier thresholds + headliner-only definition as the calendar, so the two
   * surfaces stay on one scale. Emits "unknown" when there's no baseline yet
   * (park not ratable — < 30 operating days), rather than a made-up "moderate".
   */
  private toCrowdLevel(avgWaitP90: number, typicalDayPeak: number): CrowdLevel {
    return rateOrUnknown(avgWaitP90, typicalDayPeak);
  }
}
