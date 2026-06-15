import { Injectable, Logger, Inject } from "@nestjs/common";
import { CacheKeys } from "../common/cache/cache-keys";
import { safeJsonParse } from "../common/utils/json.util";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, In } from "typeorm";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Park } from "../parks/entities/park.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { WeatherData } from "../parks/entities/weather-data.entity";
import {
  ScheduleEntry,
  ScheduleType,
} from "../parks/entities/schedule-entry.entity";
import { RestaurantLiveData } from "../restaurants/entities/restaurant-live-data.entity";
import { ShowLiveData } from "../shows/entities/show-live-data.entity";
import { PredictionAccuracy } from "../ml/entities/prediction-accuracy.entity";
import { WaitTimePrediction } from "../ml/entities/wait-time-prediction.entity";
import { QueueDataAggregate } from "./entities/queue-data-aggregate.entity";
import { ParkDailyStats } from "../stats/entities/park-daily-stats.entity";
import { HeadlinerAttraction } from "./entities/headliner-attraction.entity";
import { ParkP50Baseline } from "./entities/park-p50-baseline.entity";
import { AttractionP50Baseline } from "./entities/attraction-p50-baseline.entity";
import { ParkP90Baseline } from "./entities/park-p90-baseline.entity";
import { AttractionP90Baseline } from "./entities/attraction-p90-baseline.entity";
import { AttractionHourlyHistory } from "./entities/attraction-hourly-history.entity";
import { AttractionRopeDrop } from "./entities/attraction-rope-drop.entity";
import {
  OccupancyDto,
  ParkStatisticsDto,
  AttractionStatisticsDto,
  GlobalStatsDto,
  ParkStatsItemDto,
  AttractionStatsItemDto,
  PeakHourSource,
} from "./dto";
import { CrowdLevel } from "../common/types/crowd-level.type";
import { RopeDropStored } from "../common/types/rope-drop.type";
import {
  computeRopeDrop,
  DEFAULT_ROPE_DROP_THRESHOLDS,
  RopeDropComputeResult,
  RopeDropDayInput,
} from "./utils/rope-drop.util";
import { buildParkUrl, buildAttractionUrl } from "../common/utils/url.util";
import { peakHourConfidence } from "./utils/peak-hour.util";
import {
  getStartOfDayInTimezone,
  getCurrentDateInTimezone,
  getCurrentTimeInTimezone,
} from "../common/utils/date.util";
import { roundToNearest5Minutes } from "../common/utils/wait-time.utils";
import { determineCrowdLevel } from "../common/utils/crowd-level.util";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import { subDays } from "date-fns";

import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * TTL (seconds) for negatively-cached attraction baselines — i.e. caching
 * the *absence* of a P50/P90 baseline row so attractions without one stop
 * hammering Postgres on every request. Kept short (6h) relative to the
 * daily baseline recompute so a newly-created baseline is picked up soon.
 */
const NEGATIVE_BASELINE_TTL = 6 * 60 * 60;
/** Sentinel value marking a negatively-cached P90 baseline (read path skips it). */
const P90_NEGATIVE_SENTINEL = "-1";

@Injectable()
export class AnalyticsService {
  private readonly logger = new Logger(AnalyticsService.name);
  // Per-park throttle for the "no P50/P90 baseline" warning (1/h/park) — see
  // getCurrentOccupancy; some parks can never have a baseline (sources report
  // status only, zero waits) and would otherwise warn on every request.
  private readonly missingBaselineWarnAt = new Map<string, number>();

  // Differentiated cache TTLs based on data characteristics
  private readonly TTL_REALTIME = 5 * 60; // 5 minutes - real-time wait times, occupancy
  private readonly TTL_GLOBAL_STATS = 5 * 60; // 5 minutes - global statistics
  private readonly TTL_SCHEDULE = 60 * 60; // 1 hour - park schedules

  // Unified filter constants for consistent data quality across all analytics
  /**
   * Minimum wait time threshold (in minutes) for filtering out walk-on attractions
   * Used to exclude attractions with minimal/no wait times from average calculations
   * This prevents walk-ons from diluting the average wait time for the park
   *
   * Strategy: Start with 5min threshold, fallback to 0 if insufficient data
   */
  private readonly MIN_WAIT_TIME_THRESHOLD = 10;

  /**
   * Minimum sample size required before applying MIN_WAIT_TIME_THRESHOLD
   * If fewer samples meet the threshold, fallback to 0 (include all data)
   */
  private readonly MIN_SAMPLE_SIZE_FOR_THRESHOLD = 3;

  constructor(
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
    @InjectRepository(Attraction)
    private attractionRepository: Repository<Attraction>,
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    @InjectRepository(Show)
    private showRepository: Repository<Show>,
    @InjectRepository(Restaurant)
    private restaurantRepository: Repository<Restaurant>,
    @InjectRepository(WeatherData)
    private weatherDataRepository: Repository<WeatherData>,
    @InjectRepository(ScheduleEntry)
    private scheduleEntryRepository: Repository<ScheduleEntry>,
    @InjectRepository(RestaurantLiveData)
    private restaurantLiveDataRepository: Repository<RestaurantLiveData>,
    @InjectRepository(ShowLiveData)
    private showLiveDataRepository: Repository<ShowLiveData>,
    @InjectRepository(PredictionAccuracy)
    private predictionAccuracyRepository: Repository<PredictionAccuracy>,
    @InjectRepository(WaitTimePrediction)
    private waitTimePredictionRepository: Repository<WaitTimePrediction>,
    @InjectRepository(QueueDataAggregate)
    private queueDataAggregateRepository: Repository<QueueDataAggregate>,
    @InjectRepository(ParkDailyStats)
    private parkDailyStatsRepository: Repository<ParkDailyStats>,
    @InjectRepository(HeadlinerAttraction)
    private headlinerAttractionRepository: Repository<HeadlinerAttraction>,
    @InjectRepository(ParkP50Baseline)
    private parkP50BaselineRepository: Repository<ParkP50Baseline>,
    @InjectRepository(AttractionP50Baseline)
    private attractionP50BaselineRepository: Repository<AttractionP50Baseline>,
    @InjectRepository(ParkP90Baseline)
    private parkP90BaselineRepository: Repository<ParkP90Baseline>,
    @InjectRepository(AttractionP90Baseline)
    private attractionP90BaselineRepository: Repository<AttractionP90Baseline>,
    @InjectRepository(AttractionHourlyHistory)
    private attractionHourlyHistoryRepository: Repository<AttractionHourlyHistory>,
    @InjectRepository(AttractionRopeDrop)
    private attractionRopeDropRepository: Repository<AttractionRopeDrop>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Determine the effective start time for analytics filtering
   * Uses today's schedule opening time if available, otherwise midnight in park timezone
   */
  async getEffectiveStartTime(parkId: string, timezone: string): Promise<Date> {
    const todayStr = getCurrentDateInTimezone(timezone);
    const cacheKey = `analytics:effective_start:${parkId}:${todayStr}`;
    const cached = await this.redis.get(cacheKey);
    if (cached) return new Date(cached);

    const schedule = await this.scheduleEntryRepository.findOne({
      where: {
        parkId,
        date: todayStr as any,
        scheduleType: ScheduleType.OPERATING,
      },
      order: { openingTime: "ASC" },
    });

    const result = schedule?.openingTime ?? getStartOfDayInTimezone(timezone);
    // Use short TTL for the midnight fallback so a schedule sync within the hour is picked up quickly.
    // Use full TTL_SCHEDULE once a real opening time is known (it won't change during the day).
    const ttl = schedule?.openingTime ? this.TTL_SCHEDULE : this.TTL_REALTIME;
    await this.redis.set(cacheKey, result.toISOString(), "EX", ttl);
    return result;
  }

  /**
   * Get today's effective closing time for analytics filtering and validation.
   * Used to ensure peak hour and peak wait stats are not shown beyond operating hours.
   *
   * @param parkId - Park ID
   * @param timezone - Park timezone (for date alignment)
   * @returns Closing time in UTC, or null if no schedule or no closing time
   */
  async getEffectiveEndTime(
    parkId: string,
    timezone: string,
  ): Promise<Date | null> {
    const todayStr = getCurrentDateInTimezone(timezone);
    const schedule = await this.scheduleEntryRepository.findOne({
      where: {
        parkId,
        date: todayStr as any,
        scheduleType: ScheduleType.OPERATING,
      },
      order: { openingTime: "ASC" },
    });
    return schedule?.closingTime ?? null;
  }

  /**
   * Get effective start times for multiple parks in batch
   * Optimized to avoid N+1 queries by fetching all schedules in a single query
   *
   * @param parks - Array of parks with id and timezone
   * @returns Map of parkId -> effective start time
   */
  async getBatchEffectiveStartTime(
    parks: Array<{ id: string; timezone: string }>,
  ): Promise<Map<string, Date>> {
    const resultMap = new Map<string, Date>();

    if (parks.length === 0) {
      return resultMap;
    }

    // Group parks by timezone to batch fetch schedules efficiently
    const timezoneGroups = new Map<string, string[]>();
    for (const park of parks) {
      const timezone = park.timezone || "UTC";
      if (!timezoneGroups.has(timezone)) {
        timezoneGroups.set(timezone, []);
      }
      timezoneGroups.get(timezone)!.push(park.id);
    }

    // Fetch schedules for all parks in parallel, grouped by timezone
    const schedulePromises = Array.from(timezoneGroups.entries()).map(
      async ([timezone, parkIds]) => {
        const todayStr = getCurrentDateInTimezone(timezone);
        const schedules = await this.scheduleEntryRepository.find({
          where: {
            parkId: In(parkIds),
            date: todayStr as any,
            scheduleType: ScheduleType.OPERATING,
          },
          order: {
            parkId: "ASC",
            openingTime: "ASC",
          },
        });

        // Group by parkId and take first (earliest) opening time per park
        const scheduleMap = new Map<string, ScheduleEntry>();
        for (const schedule of schedules) {
          if (!scheduleMap.has(schedule.parkId)) {
            scheduleMap.set(schedule.parkId, schedule);
          }
        }

        return { timezone, parkIds, scheduleMap };
      },
    );

    const scheduleResults = await Promise.all(schedulePromises);

    // Build result map with fallback to start of day
    for (const park of parks) {
      const timezone = park.timezone || "UTC";
      const result = scheduleResults.find((r) => r.timezone === timezone);
      const schedule = result?.scheduleMap.get(park.id);

      if (schedule?.openingTime) {
        resultMap.set(park.id, schedule.openingTime);
      } else {
        resultMap.set(park.id, getStartOfDayInTimezone(timezone));
      }
    }

    return resultMap;
  }

  /**
   * Calculate park occupancy for multiple parks in batch
   * OPTIMIZED: Cache-first strategy using Redis
   * Pre-computed values are written during wait-times sync
   */
  async getBatchParkOccupancy(
    parkIds: string[],
  ): Promise<Map<string, OccupancyDto>> {
    const resultMap = new Map<string, OccupancyDto>();

    if (parkIds.length === 0) {
      return resultMap;
    }

    // Try to fetch from Redis first (cache-first strategy)
    const cacheKeys = parkIds.map((id) => CacheKeys.parkOccupancy(id));
    const cachedValues = await this.redis.mget(...cacheKeys);

    for (let i = 0; i < parkIds.length; i++) {
      const parkId = parkIds[i];
      const cached = cachedValues[i];

      if (cached) {
        try {
          const parsed = JSON.parse(cached);
          resultMap.set(parkId, parsed);
        } catch (_e) {
          this.logger.warn(`Failed to parse cached occupancy for ${parkId}`);
        }
      }
    }

    // For parks not in cache, calculate on-demand using EXISTING function
    // This is rare if warmup works, but provides accurate fallback.
    // Chunks of 5 in parallel: each calculateParkOccupancy fires several
    // heavy queue_data queries, so unbounded parallelism over a cold list
    // would saturate the DB (same reasoning as the warmup batch size),
    // while the old one-at-a-time loop made a cold country listing linear
    // in park count on the request path.
    const missingParkIds = parkIds.filter((id) => !resultMap.has(id));
    if (missingParkIds.length > 0) {
      this.logger.verbose(
        `Computing occupancy for ${missingParkIds.length} parks (cache miss)`,
      );
      const CHUNK_SIZE = 5;
      for (let i = 0; i < missingParkIds.length; i += CHUNK_SIZE) {
        const chunk = missingParkIds.slice(i, i + CHUNK_SIZE);
        await Promise.all(
          chunk.map(async (parkId) => {
            try {
              const occupancy = await this.calculateParkOccupancy(parkId);
              resultMap.set(parkId, occupancy);
            } catch (error) {
              this.logger.warn(
                `Failed to calculate occupancy for ${parkId}`,
                error,
              );
            }
          }),
        );
      }
    }

    return resultMap;
  }

  /**
   * Calculate park occupancy based on 90th percentile of historical wait times
   *
   * **Occupancy Calculation:**
   * - 100% = 90th percentile of typical wait times for this hour/weekday over last 1 year
   * - Formula: (currentAvgWait / p90Baseline) * 100
   * - Can exceed 100% on extremely busy days (e.g., 150% = 50% busier than typical P90)
   *
   * **Timezone-Aware:**
   * - Uses park's timezone (not UTC) for hour/day-of-week calculation
   * - Ensures accurate baseline matching for parks in different timezones
   * - Critical for international parks (e.g., Tokyo Disney vs. Orlando)
   *
   * **Trend Calculation (Hybrid Logic):**
   * - Compares current spot wait vs. last hour average (fast trend)
   * - Compares last hour vs. previous hour average (slow trend)
   * - Uses computeTrend() for final determination
   *
   * **Edge Cases:**
   * - No current data: Returns 0% occupancy with "typical" status
   * - No historical baseline: Returns 50% occupancy (default) with warning
   * - Park closed: Should not be called (caller should check status first)
   *
   *
   * @param parkId - Park ID
   * @returns OccupancyDto with current percentage, trend, comparison, and breakdown
   *
   * @example
   * // For a park currently at 45min average wait with 30min P90 baseline
   * // Result: { current: 150, trend: "up", comparisonStatus: "higher", ... }
   * // Meaning: 150% of typical (50% busier than normal)
   */
  async calculateParkOccupancy(parkId: string): Promise<OccupancyDto> {
    const now = new Date();

    // Headliner-only for current + trend (same rides as the P90 baseline so
    // the live "peak feel" stays apples-to-apples with the 548-day reference).
    const headliners = await this.headlinerAttractionRepository.find({
      where: { parkId },
      select: ["parkId", "attractionId"],
    });
    const headlinerIds = headliners.map((h) => h.attractionId);

    // Live park crowd level — P90 across per-headliner ratios
    // (latest_wait ÷ that-attraction's-P50). Computes "how stressed is
    // the typical busy-headliner queue right now" rather than averaging
    // ratios (which a single quiet ride can drag down to "very_low" even
    // when a marquee ride is at its typical peak). Each ride contributes
    // its LATEST sample in the last 60 min so we stay responsive when a
    // queue drops, while still catching sparse-reporting headliners.
    // Fall back to the park-wide MAX/avg pattern when per-ride P50s
    // aren't available (brand-new park, all rides missing P50 rows).
    const perRideRatios = await this.getPerHeadlinerRatios(
      parkId,
      headlinerIds.length > 0 ? headlinerIds : undefined,
    );
    const currentPeakWait =
      perRideRatios !== null
        ? perRideRatios.averageCurrentWait
        : await this.getCurrentParkPeakWait(
            parkId,
            headlinerIds.length > 0 ? headlinerIds : undefined,
          );

    if (currentPeakWait === null) {
      return {
        current: 0,
        trend: "stable",
        comparedToTypical: 0,
        comparisonStatus: "typical",
        baseline90thPercentile: 0,
        confidence: "low",
        updatedAt: now.toISOString(),
        breakdown: {
          currentAvgWait: 0,
          typicalAvgWait: 0,
          activeAttractions: 0,
        },
      };
    }

    // P50 (median) baseline = "typical-day wait". Comparing current peak
    // (last 20 min) against the typical median gives an intuitive ratio:
    // 100% = current peak matches a typical wait, >150% = busier than
    // typical, <60% = quiet. P50 and P90 are written atomically by the
    // same daily cron — a missing row means the park is brand-new and
    // both percentiles are absent.
    let baseline = 0;
    let confidence: "high" | "medium" | "low" = "low";

    const p50Record = await this.getP50BaselineWithConfidence(parkId);
    if (p50Record) {
      baseline = p50Record.value;
      confidence = p50Record.confidence;
    }

    if (baseline === 0) {
      // Throttled to once per park per hour: ~24 parks legitimately NEVER get a
      // baseline (their sources — queue-times/wiki — deliver status only, all
      // waits are 0: water parks, Lotte World, several Six Flags/Cedar Fair
      // parks), so this fired on every request and was the loudest line in the
      // log while signalling nothing actionable.
      const lastWarn = this.missingBaselineWarnAt.get(parkId) ?? 0;
      if (now.getTime() - lastWarn > 60 * 60 * 1000) {
        this.missingBaselineWarnAt.set(parkId, now.getTime());
        this.logger.warn(
          `No P50/P90 baseline for park ${parkId} — returning low-confidence default (expected for parks whose sources report no wait values; throttled to 1/h)`,
        );
      }
      return {
        current: 50,
        trend: "stable",
        comparedToTypical: 0,
        comparisonStatus: "typical",
        baseline90thPercentile: 0,
        confidence: "low",
        updatedAt: now.toISOString(),
        breakdown: {
          currentAvgWait: roundToNearest5Minutes(currentPeakWait),
          typicalAvgWait: 0,
          activeAttractions: 0,
        },
      };
    }

    // Occupancy: P90 across per-headliner (latest ÷ P50) ratios × 100 when
    // we have per-ride baselines; falls back to avg current ÷ park-P50
    // when we don't. 100% = the 90th-percentile-busiest headliner is at
    // its typical wait. > 150% means even the busier rides are running
    // materially above typical.
    const occupancyPercentage =
      perRideRatios !== null
        ? perRideRatios.ratioP90 * 100
        : (currentPeakWait / baseline) * 100;

    // Calculate park trend (hybrid logic) — headliner-only: average per headliner, then divide by count
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
    const twoHoursAgo = new Date(now.getTime() - 120 * 60 * 1000);

    let avgLastHour: number | null = null;
    let avgPrevHour: number | null = null;

    if (headlinerIds.length > 0) {
      // Per-headliner average per bucket, then average of those (each headliner counts once)
      const trendQuery = `
        WITH per_ride_bucket AS (
          SELECT
            qd."attractionId",
            CASE
              WHEN qd.timestamp >= $3 THEN 1
              WHEN qd.timestamp >= $2 AND qd.timestamp < $3 THEN 2
            END as bucket,
            AVG(qd."waitTime") as avg_wait
          FROM queue_data qd
          WHERE qd."attractionId" = ANY($1)
            AND qd.timestamp >= $2
            AND qd.status = 'OPERATING'
            AND qd."waitTime" IS NOT NULL
            AND qd."queueType" = 'STANDBY'
          GROUP BY qd."attractionId", bucket
        )
        SELECT bucket, AVG(avg_wait) as bucket_avg
        FROM per_ride_bucket
        WHERE bucket IS NOT NULL
        GROUP BY bucket
      `;
      const trendResult = await this.queueDataRepository.query(trendQuery, [
        headlinerIds,
        twoHoursAgo,
        oneHourAgo,
      ]);
      const buckets: Record<number, number> = {};
      for (const row of trendResult) {
        if (row.bucket != null && row.bucket_avg != null) {
          buckets[row.bucket] = parseFloat(row.bucket_avg);
        }
      }
      avgLastHour = buckets[1] ?? null;
      avgPrevHour = buckets[2] ?? null;
    } else {
      // Fallback: all attractions, avg-of-per-ride-averages per bucket
      const trendQuery = `
        WITH per_ride_bucket AS (
          SELECT
            qd."attractionId",
            CASE
              WHEN qd.timestamp >= $3 THEN 1
              WHEN qd.timestamp >= $2 AND qd.timestamp < $3 THEN 2
            END as bucket,
            AVG(qd."waitTime") as avg_wait
          FROM queue_data qd
          JOIN attractions a ON qd."attractionId" = a.id
          WHERE a."parkId" = $1::uuid
            AND qd.timestamp >= $2
            AND qd.status = 'OPERATING'
            AND qd."waitTime" IS NOT NULL
            AND qd."queueType" = 'STANDBY'
          GROUP BY qd."attractionId", bucket
        )
        SELECT bucket, AVG(avg_wait) as avg_wait
        FROM per_ride_bucket
        WHERE bucket IS NOT NULL
        GROUP BY bucket
      `;
      const trendResult = await this.queueDataRepository.query(trendQuery, [
        parkId,
        twoHoursAgo,
        oneHourAgo,
      ]);
      const buckets: Record<number, number> = {};
      for (const row of trendResult) {
        if (row.bucket && row.avg_wait) {
          buckets[row.bucket] = parseFloat(row.avg_wait);
        }
      }
      avgLastHour = buckets[1] || null;
      avgPrevHour = buckets[2] || null;
    }

    let trend: "up" | "down" | "stable" = "stable";
    if (avgLastHour !== null) {
      trend = this.computeTrend(currentPeakWait, avgLastHour, avgPrevHour);
    }

    // Comparison to typical: derived from the SAME basis as `current` so the
    // two fields are always internally consistent (comparedToTypical =
    // current − 100). Previously this used a park-level (currentPeakWait −
    // P50) / P50 diff while `current` used the per-ride-P90 ratio aggregate —
    // different denominators that produced contradictory pairs on the park
    // page (e.g. current 204 %, comparedToTypical 42 %). Anchoring on
    // occupancyPercentage fixes both paths: in the per-ride path it tracks
    // ratioP90 × 100 − 100, and in the park-wide fallback it reduces exactly
    // to the original (currentPeakWait − P50) / P50 × 100.
    const comparedToTypical = occupancyPercentage - 100;

    // Determine status based on percentage difference
    let comparisonStatus: "higher" | "lower" | "typical" = "typical";
    if (Math.abs(comparedToTypical) > 10) {
      // >10% difference
      comparisonStatus = comparedToTypical > 0 ? "higher" : "lower";
    }
    return {
      current: Math.round(occupancyPercentage),
      trend,
      comparedToTypical: Math.round(comparedToTypical),
      comparisonStatus,
      baseline90thPercentile: Math.round(baseline),
      confidence,
      updatedAt: now.toISOString(),
      breakdown: {
        currentAvgWait: roundToNearest5Minutes(currentPeakWait),
        typicalAvgWait: roundToNearest5Minutes(baseline),
        activeAttractions: await this.getActiveAttractionsCount(parkId),
      },
    };
  }

  /**
   * Get current park occupancy percentage for ML features
   *
   * Simplified version of calculateParkOccupancy that returns only the percentage
   * Used by ML service for park-wide crowding predictions
   *
   * @param parkId - Park ID
   * @returns Occupancy percentage (0-200%) or 100 if no data
   *
   * Example: 75 = park is at 75% of typical P90 wait times
   */
  async getCurrentOccupancy(parkId: string): Promise<number> {
    // ML feature `park_occupancy_pct`. Kept on the simpler park-wide
    // (avg latest ÷ park-P50) shape — not the per-ride-ratio P90 used
    // by the user-facing crowd level. Trained models depend on this
    // exact feature distribution; switching to per-ride ratios would
    // require a retrain cycle. We DO simplify the fallback here:
    // P50 and P90 are written atomically by the daily cron, so when
    // P50 is missing P90 is missing too.
    const headliners = await this.headlinerAttractionRepository.find({
      where: { parkId },
      select: ["parkId", "attractionId"],
    });
    const headlinerIds = headliners.map((h) => h.attractionId);
    const currentPeakWait = await this.getCurrentParkPeakWait(
      parkId,
      headlinerIds.length > 0 ? headlinerIds : undefined,
    );

    if (currentPeakWait === null) {
      return 100;
    }

    const baseline = await this.getP50BaselineFromCache(parkId);
    if (baseline === 0) {
      return 100;
    }

    const occupancyPercentage = (currentPeakWait / baseline) * 100;
    return Math.round(Math.min(occupancyPercentage, 200));
  }

  /**
   * Get park-level percentiles for today
   * Uses pre-computed queue_data_aggregates for efficient calculation
   *
   * Returns P50, P75, P90, P95 across all attractions in park
   */
  async getParkPercentilesToday(parkId: string): Promise<{
    p50: number;
    p75: number;
    p90: number;
    p95: number;
  } | null> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["timezone"],
    });
    const startOfDay = getStartOfDayInTimezone(park?.timezone || "UTC");

    try {
      // Query aggregates for all attractions in this park today (park timezone)
      const result = await this.queueDataAggregateRepository
        .createQueryBuilder("agg")
        .select("percentile_cont(0.50) WITHIN GROUP (ORDER BY agg.p50)", "p50")
        .addSelect(
          "percentile_cont(0.75) WITHIN GROUP (ORDER BY agg.p75)",
          "p75",
        )
        .addSelect(
          "percentile_cont(0.90) WITHIN GROUP (ORDER BY agg.p90)",
          "p90",
        )
        .addSelect(
          "percentile_cont(0.95) WITHIN GROUP (ORDER BY agg.p95)",
          "p95",
        )
        .where("agg.parkId = :parkId", { parkId })
        .andWhere("agg.hour >= :startOfDay", { startOfDay })
        .getRawOne();

      if (!result || result.p50 === null) {
        return null;
      }

      return {
        p50: Math.round(parseFloat(result.p50)),
        p75: Math.round(parseFloat(result.p75)),
        p90: Math.round(parseFloat(result.p90)),
        p95: Math.round(parseFloat(result.p95)),
      };
    } catch (error) {
      this.logger.warn(`Failed to get park percentiles for ${parkId}:`, error);
      return null;
    }
  }

  /**
   * Get attraction-level percentile distribution for today
   * Includes P25, P50, P75, P90, and IQR (Interquartile Range)
   */
  async getAttractionPercentilesToday(attractionId: string): Promise<{
    p25: number;
    p50: number;
    p75: number;
    p90: number;
    p95?: number;
    iqr: number;
    sampleCount: number;
  } | null> {
    const attraction = await this.attractionRepository.findOne({
      where: { id: attractionId },
      relations: ["park"],
      select: { park: { timezone: true } },
    });
    const startOfDay = getStartOfDayInTimezone(
      attraction?.park?.timezone || "UTC",
    );

    try {
      const result = await this.queueDataAggregateRepository
        .createQueryBuilder("agg")
        .select("percentile_cont(0.25) WITHIN GROUP (ORDER BY agg.p25)", "p25")
        .addSelect(
          "percentile_cont(0.50) WITHIN GROUP (ORDER BY agg.p50)",
          "p50",
        )
        .addSelect(
          "percentile_cont(0.75) WITHIN GROUP (ORDER BY agg.p75)",
          "p75",
        )
        .addSelect(
          "percentile_cont(0.90) WITHIN GROUP (ORDER BY agg.p90)",
          "p90",
        )
        .addSelect("AVG(agg.iqr)", "iqr")
        .addSelect("SUM(agg.sampleCount)", "sampleCount")
        .where("agg.attractionId = :attractionId", { attractionId })
        .andWhere("agg.hour >= :startOfDay", { startOfDay })
        .getRawOne();

      if (!result || result.p50 === null) {
        return null;
      }

      return {
        p25: Math.round(parseFloat(result.p25)),
        p50: Math.round(parseFloat(result.p50)),
        p75: Math.round(parseFloat(result.p75)),
        p90: Math.round(parseFloat(result.p90)),
        iqr: Math.round(parseFloat(result.iqr || "0")),
        sampleCount: parseInt(result.sampleCount || "0"),
      };
    } catch (error) {
      this.logger.warn(
        `Failed to get attraction percentiles for ${attractionId}:`,
        error,
      );
      return null;
    }
  }

  /**
   * Batch version of getAttractionPercentilesToday for multiple attractions
   * Groups by park timezone to minimize queries
   */
  async getBatchAttractionPercentilesToday(attractionIds: string[]): Promise<
    Map<
      string,
      {
        p25: number;
        p50: number;
        p75: number;
        p90: number;
        iqr: number;
        sampleCount: number;
      } | null
    >
  > {
    if (attractionIds.length === 0) {
      return new Map();
    }

    // Fetch park info for timezone lookup (single query)
    const attractions = await this.attractionRepository.find({
      where: { id: In(attractionIds) },
      relations: ["park"],
      select: { id: true, park: { timezone: true } },
    });

    // Group attractions by park timezone (like getBatchEffectiveStartTime)
    const timezoneGroups = new Map<string, string[]>();

    for (const attr of attractions) {
      const tz = attr.park?.timezone || "UTC";
      if (!timezoneGroups.has(tz)) {
        timezoneGroups.set(tz, []);
      }
      timezoneGroups.get(tz)!.push(attr.id);
    }

    const resultMap = new Map();

    // Query all timezone groups in parallel
    const queryPromises = Array.from(timezoneGroups.entries()).map(
      async ([timezone, ids]) => {
        const startOfDay = getStartOfDayInTimezone(timezone);

        try {
          const result = await this.queueDataAggregateRepository
            .createQueryBuilder("agg")
            .select("agg.attractionId", "attractionId")
            .addSelect(
              "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY agg.p25)",
              "p25",
            )
            .addSelect(
              "PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY agg.p50)",
              "p50",
            )
            .addSelect(
              "PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY agg.p75)",
              "p75",
            )
            .addSelect(
              "PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY agg.p90)",
              "p90",
            )
            .addSelect("AVG(agg.iqr)", "iqr")
            .addSelect("SUM(agg.sampleCount)", "sampleCount")
            .where("agg.attractionId IN (:...ids)", { ids })
            .andWhere("agg.hour >= :startOfDay", { startOfDay })
            .groupBy("agg.attractionId")
            .getRawMany();

          return result;
        } catch (error) {
          this.logger.warn(
            `Failed to fetch batch percentiles for timezone ${timezone}:`,
            error,
          );
          return [];
        }
      },
    );

    const allResults = (await Promise.all(queryPromises)).flat();

    // Build result map
    for (const row of allResults) {
      if (!row || row.p50 === null) {
        resultMap.set(row.attractionId, null);
        continue;
      }

      resultMap.set(row.attractionId, {
        p25: Math.round(parseFloat(row.p25)),
        p50: Math.round(parseFloat(row.p50)),
        p75: Math.round(parseFloat(row.p75)),
        p90: Math.round(parseFloat(row.p90)),
        iqr: Math.round(parseFloat(row.iqr || "0")),
        sampleCount: parseInt(row.sampleCount || "0", 10),
      });
    }

    // Fill in nulls for attractions with no data
    for (const id of attractionIds) {
      if (!resultMap.has(id)) {
        resultMap.set(id, null);
      }
    }

    return resultMap;
  }

  /**
   * Get current park wait time for occupancy/trend consistency.
   * - With headliners: per-headliner AVG(wait) in last 60 min, then sum/count (same as trend/peak).
   * - Without headliners: median of latest wait per attraction (legacy).
   *
   * @param parkId - Park ID
   * @param percentile - Percentile when not using headliners (0.5 = Median). Ignored when headlinerIds set.
   * @param minWaitTime - Minimum wait time threshold (default: 5 min to exclude walk-ons)
   * @param headlinerIds - Optional: restrict to headliners (average per headliner, then divide by count)
   * @returns Wait time in minutes or null if no data
   */
  private async getCurrentSpotWaitTime(
    parkId: string,
    percentile: number = 0.5,
    minWaitTime: number = 5,
    headlinerIds?: string[],
  ): Promise<number | null> {
    const windowAgo = new Date(Date.now() - 60 * 60 * 1000);
    const useHeadliners = headlinerIds && headlinerIds.length > 0;

    if (useHeadliners) {
      // Same formula as trend/peak: per headliner AVG(wait) in window, then sum / count
      const perRideResult = await this.queueDataRepository.query(
        `
        SELECT qd."attractionId", AVG(qd."waitTime") as avg_wait
        FROM queue_data qd
        WHERE qd."attractionId" = ANY($1)
          AND qd.timestamp >= $2
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
          AND qd."waitTime" >= $3
          AND qd."queueType" = 'STANDBY'
        GROUP BY qd."attractionId"
        `,
        [headlinerIds, windowAgo, minWaitTime],
      );
      if (perRideResult.length === 0) {
        if (minWaitTime > 0) {
          return this.getCurrentSpotWaitTime(
            parkId,
            percentile,
            0,
            headlinerIds,
          );
        }
        return null;
      }
      const sum = perRideResult.reduce(
        (acc: number, row: { avg_wait: string | number }) =>
          acc + Number(row.avg_wait ?? 0),
        0,
      );
      const avg = sum / perRideResult.length;
      return Math.round(avg);
    }

    // Legacy: all attractions, median of latest wait per attraction
    const result = await this.queueDataRepository.query(
      `
      WITH LatestWaits AS (
        SELECT DISTINCT ON (qd."attractionId") 
          qd."waitTime"
        FROM queue_data qd
        JOIN attractions a ON qd."attractionId" = a.id
        WHERE a."parkId" = $1::uuid
          AND qd.timestamp >= $2
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
          AND qd."waitTime" >= $3
          AND qd."queueType" = 'STANDBY'
        ORDER BY qd."attractionId", qd.timestamp DESC
      )
      SELECT 
        PERCENTILE_CONT($4) WITHIN GROUP (ORDER BY "waitTime") as "pWait",
        COUNT(*) as "count"
      FROM LatestWaits
    `,
      [parkId, windowAgo, minWaitTime, percentile],
    );

    const row = result[0];
    if (
      row?.count &&
      parseInt(row.count) < this.MIN_SAMPLE_SIZE_FOR_THRESHOLD &&
      minWaitTime > 0
    ) {
      return this.getCurrentSpotWaitTime(parkId, percentile, 0);
    }
    return row?.pWait ? Math.round(parseFloat(row.pWait)) : null;
  }

  /**
   * Current park "peak feel" — avg-across-headliners of (per-headliner MAX
   * wait in the last `windowMinutes`). This is the live-side counterpart
   * to the 548-day P90 baseline: same shape (peak-oriented, headliner-only,
   * per-ride then averaged) but a small recent window. With samples every
   * ~5 min, MAX in a 60-min window holds ~12 data points so it effectively
   * tracks the 90-100th percentile of the recent slice — what the user
   * would describe as "the longest wait at the headliners right now".
   *
   * Comparison `currentPeak / p90Baseline` stays apples-to-apples (peak
   * vs. peak), 100%-centred, identical scale to the calendar day metric.
   */
  /**
   * For each reporting headliner, compute (latest_wait ÷ that ride's P50
   * baseline) and return the P90 of those ratios plus the simple-avg of
   * the underlying current waits.
   *
   * The P90 of ratios protects against "everything reads very_low because
   * one quiet ride drags the average down". A park with 9 quiet rides and
   * 1 marquee at typical wait shouldn't be labelled "very_low" — the
   * marquee experience is what visitors remember.
   *
   * Returns null when:
   * - No headlinerIds given (caller should fall back to park-wide path)
   * - No ride has both a P50 baseline AND a recent operating sample.
   */
  private async getPerHeadlinerRatios(
    _parkId: string,
    headlinerIds: string[] | undefined,
  ): Promise<{
    ratioP90: number;
    averageCurrentWait: number;
    rideCount: number;
  } | null> {
    if (!headlinerIds || headlinerIds.length === 0) return null;

    const sixtyMinAgo = new Date(Date.now() - 60 * 60 * 1000);
    const latestRows = await this.queueDataRepository.query(
      `
      SELECT DISTINCT ON (qd."attractionId")
        qd."attractionId",
        qd."waitTime" AS latest_wait
      FROM queue_data qd
      WHERE qd."attractionId" = ANY($1)
        AND qd.timestamp >= $2
        AND qd.status = 'OPERATING'
        AND qd."waitTime" IS NOT NULL
        AND qd."waitTime" >= $3
        AND qd."queueType" = 'STANDBY'
      ORDER BY qd."attractionId", qd.timestamp DESC
      `,
      [headlinerIds, sixtyMinAgo, this.MIN_WAIT_TIME_THRESHOLD],
    );

    if (latestRows.length === 0) return null;

    const p50Map = await this.getBatchAttractionP50s(headlinerIds);

    const ratios: number[] = [];
    let waitSum = 0;
    let waitCount = 0;
    for (const row of latestRows) {
      const wait = Number(row.latest_wait ?? 0);
      const p50 = p50Map.get(row.attractionId);
      if (!p50 || p50 <= 0) continue;
      ratios.push(wait / p50);
      waitSum += wait;
      waitCount++;
    }

    if (ratios.length === 0) return null;

    ratios.sort((a, b) => a - b);
    const idx = (ratios.length - 1) * 0.9;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    const ratioP90 = ratios[lo] * (1 - (idx - lo)) + ratios[hi] * (idx - lo);

    return {
      ratioP90,
      averageCurrentWait: Math.round(waitSum / waitCount),
      rideCount: ratios.length,
    };
  }

  private async getCurrentParkPeakWait(
    parkId: string,
    headlinerIds?: string[],
    windowMinutes = 60,
    minWaitTime = this.MIN_WAIT_TIME_THRESHOLD,
  ): Promise<number | null> {
    // For each headliner: take the latest STANDBY/OPERATING waitTime in
    // the freshness window (default 60 min), then average across rides.
    //
    // "Latest reading per ride" — not MAX over window — is what makes the
    // park reading responsive: when a queue drops 80 → 30 we see 30 (the
    // last sample), not 80 (a stale peak from earlier in the window). The
    // 60-min window is long enough to catch sparse-reporting headliners
    // (e.g. Mario Kart, Harry Potter) which only emit every 10-15 min,
    // and is auto-expanded to 240 min when nothing falls in window at all
    // (real source outage).
    const expandWindow = (m: number) => (m < 240 ? 240 : null);

    const windowAgo = new Date(Date.now() - windowMinutes * 60 * 1000);
    const useHeadliners = headlinerIds && headlinerIds.length > 0;

    if (useHeadliners) {
      const perRide = await this.queueDataRepository.query(
        `
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd."waitTime" AS latest_wait
        FROM queue_data qd
        WHERE qd."attractionId" = ANY($1)
          AND qd.timestamp >= $2
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
          AND qd."waitTime" >= $3
          AND qd."queueType" = 'STANDBY'
        ORDER BY qd."attractionId", qd.timestamp DESC
        `,
        [headlinerIds, windowAgo, minWaitTime],
      );
      if (perRide.length === 0) {
        if (minWaitTime > 0) {
          return this.getCurrentParkPeakWait(
            parkId,
            headlinerIds,
            windowMinutes,
            0,
          );
        }
        const next = expandWindow(windowMinutes);
        if (next !== null) {
          return this.getCurrentParkPeakWait(parkId, headlinerIds, next, 0);
        }
        return null;
      }
      const sum = perRide.reduce(
        (acc: number, row: { latest_wait: string | number }) =>
          acc + Number(row.latest_wait ?? 0),
        0,
      );
      return Math.round(sum / perRide.length);
    }

    // Fallback when no headliners: per-attraction latest wait, then avg.
    const result = await this.queueDataRepository.query(
      `
      WITH latest_per_ride AS (
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd."waitTime" AS latest_wait
        FROM queue_data qd
        JOIN attractions a ON qd."attractionId" = a.id
        WHERE a."parkId" = $1::uuid
          AND qd.timestamp >= $2
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
          AND qd."waitTime" >= $3
          AND qd."queueType" = 'STANDBY'
        ORDER BY qd."attractionId", qd.timestamp DESC
      )
      SELECT AVG(latest_wait) as peak FROM latest_per_ride
      `,
      [parkId, windowAgo, minWaitTime],
    );
    const peak = result[0]?.peak;
    if (peak) return Math.round(parseFloat(peak));
    if (minWaitTime > 0) {
      return this.getCurrentParkPeakWait(parkId, undefined, windowMinutes, 0);
    }
    const next = expandWindow(windowMinutes);
    if (next !== null) {
      return this.getCurrentParkPeakWait(parkId, undefined, next, 0);
    }
    return null;
  }

  /**
   * Calculate 95th percentile of wait times for specific hour/weekday over last 2 years
   */
  private async calculate95thPercentile(
    parkId: string,
    hour: number,
    dayOfWeek: number,
  ): Promise<number> {
    const twoYearsAgo = new Date();
    twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);

    // Query to get all wait times for this hour/weekday
    const waitTimes = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("qd.waitTime", "waitTime")
      .innerJoin("qd.attraction", "attraction")
      .where("attraction.parkId = :parkId", { parkId })
      .andWhere("qd.timestamp >= :twoYearsAgo", { twoYearsAgo })
      .andWhere("EXTRACT(HOUR FROM qd.timestamp) = :hour", { hour })
      .andWhere("EXTRACT(DOW FROM qd.timestamp) = :dayOfWeek", { dayOfWeek })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .andWhere("qd.waitTime IS NOT NULL")
      .andWhere("qd.waitTime >= 10")
      .andWhere("qd.queueType = 'STANDBY'")
      .getRawMany();

    if (waitTimes.length === 0) {
      return 0;
    }

    // Calculate 95th percentile
    const sortedWaitTimes = waitTimes
      .map((wt) => parseFloat(wt.waitTime))
      .sort((a, b) => a - b);

    const percentileIndex = Math.ceil(sortedWaitTimes.length * 0.95) - 1;
    return sortedWaitTimes[percentileIndex];
  }

  /**
   * Get count of currently operating attractions
   */
  private async getActiveAttractionsCount(parkId: string): Promise<number> {
    // Use 120 minutes (2 hours) to accommodate sync intervals
    const windowAgo = new Date(Date.now() - 120 * 60 * 1000);

    const result = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("COUNT(DISTINCT qd.attractionId)", "count")
      .innerJoin("qd.attraction", "attraction")
      .where("attraction.parkId = :parkId", { parkId })
      .andWhere("qd.timestamp >= :windowAgo", { windowAgo })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .getRawOne();

    return result?.count ? parseInt(result.count) : 0;
  }

  /**
   * Get park-wide statistics (optimized single query version)
   *
   * Previously made 5+ sequential queries:
   * - getCurrentAverageWaitTime
   * - getDailyAverageWaitTime
   * - getPeakHourToday
   * - getAttractionCounts
   * - calculateParkOccupancy
   *
   * Now uses a single CTE query for ~70% performance improvement.
   */
  async getParkStatistics(
    parkId: string,
    timezone?: string,
    startTime?: Date,
  ): Promise<ParkStatisticsDto> {
    const now = new Date();
    const windowAgo = new Date(Date.now() - 120 * 60 * 1000); // 2 hours window

    // 1. Resolve Timezone
    let resolvedTimezone = timezone;
    if (!resolvedTimezone) {
      const park = await this.parkRepository.findOne({
        where: { id: parkId },
        select: ["timezone"],
      });
      resolvedTimezone = park?.timezone || "UTC";
    }

    // 2. Resolve Start Time (Effective "Start of Day")
    let resolvedStartTime = startTime;
    if (!resolvedStartTime) {
      resolvedStartTime = await this.getEffectiveStartTime(
        parkId,
        resolvedTimezone,
      );
    }
    const startOfDay = resolvedStartTime;

    // Try cache first (safeJsonParse: corrupt entry = miss, rebuild below)
    const cacheKey = `park:statistics:${parkId}`;
    const cached = safeJsonParse<ParkStatisticsDto>(
      await this.redis.get(cacheKey),
    );
    if (cached) {
      return cached;
    }

    // OPTIMIZATION: Use ParkDailyStats for "avg_wait_today" and "max_wait_today"
    // when available (written by StatsProcessor → StatsService.calculateAndStoreDailyStats).
    // Outlier protection is in StatsService so bad queue_data never poisons the cache.
    let optimizedAvgWait: number | null = null;
    let optimizedMaxWait: number | null = null;
    try {
      const todayStr = getCurrentDateInTimezone(resolvedTimezone);
      const dailyStats = await this.parkDailyStatsRepository.findOne({
        where: { parkId, date: todayStr },
      });

      if (dailyStats) {
        optimizedAvgWait = dailyStats.p90WaitTime; // P90 as representative "avg today"
        optimizedMaxWait = dailyStats.maxWaitTime;
      }
    } catch (_e) {
      // Ignore errors, fall back to live aggregation
    }

    // Build SQL query conditionally based on whether we have optimized stats
    let query: string;
    let queryParams: any[];

    if (optimizedAvgWait !== null && optimizedMaxWait !== null) {
      // Fast path: Use pre-computed stats from ParkDailyStats
      query = `
      WITH latest_queue AS (
        -- Get latest queue data per attraction (for current stats)
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd."waitTime",
          qd.status,
          qd.timestamp
        FROM queue_data qd
        INNER JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = $1::uuid
          AND qd.timestamp >= $2  -- Last 2 hours window
        ORDER BY qd."attractionId", 
          CASE WHEN qd."queueType" = 'STANDBY' THEN 0 ELSE 1 END,
          qd.timestamp DESC
      ),
      today_hourly AS (
        -- avg-of-per-ride-averages per hour so each ride contributes equally.
        SELECT hour, AVG(ride_avg) as hour_avg
        FROM (
          SELECT
            EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE $4) as hour,
            qd."attractionId",
            AVG(qd."waitTime") as ride_avg
          FROM queue_data qd
          INNER JOIN attractions a ON a.id = qd."attractionId"
          WHERE a."parkId" = $1::uuid
            AND qd.timestamp >= $3
            AND qd."queueType" = 'STANDBY'
            AND qd.status = 'OPERATING'
            AND qd."waitTime" IS NOT NULL
          GROUP BY hour, qd."attractionId"
        ) per_ride
        GROUP BY hour
        ORDER BY hour_avg DESC
        LIMIT 1
      ),
      attraction_counts AS (
        -- Total attraction count
        SELECT COUNT(*) as total_attractions
        FROM attractions
        WHERE "parkId" = $1::uuid
      )
      SELECT 
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY CASE WHEN lq."waitTime" >= $5 THEN lq."waitTime" END)::numeric) as current_avg_wait,
        -- Count explicitly closed attractions (has recent data AND not OPERATING)
        COUNT(CASE WHEN lq.status IS NOT NULL AND lq.status != 'OPERATING' THEN 1 END) as explicitly_closed_count,
        (SELECT total_attractions FROM attraction_counts) as total_count,
        $6::numeric as avg_wait_today,
        $7::numeric as max_wait_today,
        (SELECT hour FROM today_hourly) as peak_hour
      FROM attractions a
      LEFT JOIN latest_queue lq ON lq."attractionId" = a.id
      WHERE a."parkId" = $1::uuid
      `;
      queryParams = [
        parkId, // $1
        windowAgo, // $2
        startOfDay, // $3
        resolvedTimezone, // $4 (was $5)
        this.MIN_WAIT_TIME_THRESHOLD, // $5 (was $6)
        optimizedAvgWait, // $6 (was $7)
        optimizedMaxWait, // $7 (was $8)
      ];
    } else {
      // Slow path: Calculate from queue_data
      query = `
      WITH latest_queue AS (
        -- Get latest queue data per attraction (for current stats)
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd."waitTime",
          qd.status,
          qd.timestamp
        FROM queue_data qd
        INNER JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = $1::uuid
          AND qd.timestamp >= $2  -- Last 2 hours window
        ORDER BY qd."attractionId", 
          CASE WHEN qd."queueType" = 'STANDBY' THEN 0 ELSE 1 END,
          qd.timestamp DESC
      ),
      today_hourly AS (
        -- Aggregate by hour to find peak (using Park Timezone)
        SELECT 
          EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE $5) as hour,
          AVG(qd."waitTime") as hour_avg
        FROM queue_data qd
        INNER JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = $1::uuid
          AND qd.timestamp >= $3  -- Start of today (Effective)
          AND qd."queueType" = 'STANDBY'
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
        GROUP BY hour
        ORDER BY hour_avg DESC
        LIMIT 1
      ),
      today_max AS (
        -- Find max wait time for the park today
        SELECT MAX(qd."waitTime") as max_wait_today
        FROM queue_data qd
        INNER JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = $1::uuid
          AND qd.timestamp BETWEEN $3 AND $4
          AND qd."queueType" = 'STANDBY'
          AND qd.status = 'OPERATING'
      ),
      today_avg AS (
        -- avg-of-per-ride P90: each ride contributes equally regardless of
        -- reporting frequency, consistent with all other park-wide aggregations.
        SELECT AVG(per_ride.p90) as avg_wait_today
        FROM (
          SELECT
            qd."attractionId",
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") as p90
          FROM queue_data qd
          INNER JOIN attractions a ON a.id = qd."attractionId"
          WHERE a."parkId" = $1::uuid
            AND qd.timestamp BETWEEN $3 AND $4
            AND qd."queueType" = 'STANDBY'
            AND qd.status = 'OPERATING'
            AND qd."waitTime" IS NOT NULL
            AND qd."waitTime" >= 10
          GROUP BY qd."attractionId"
        ) per_ride
      ),
      attraction_counts AS (
        -- Total attraction count
        SELECT COUNT(*) as total_attractions
        FROM attractions
        WHERE "parkId" = $1::uuid
      )
      SELECT 
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY CASE WHEN lq."waitTime" >= $6 THEN lq."waitTime" END)::numeric) as current_avg_wait,
        -- Count explicitly closed attractions (has recent data AND not OPERATING)
        COUNT(CASE WHEN lq.status IS NOT NULL AND lq.status != 'OPERATING' THEN 1 END) as explicitly_closed_count,
        (SELECT total_attractions FROM attraction_counts) as total_count,
        (SELECT avg_wait_today FROM today_avg) as avg_wait_today,
        (SELECT max_wait_today FROM today_max) as max_wait_today,
        (SELECT hour FROM today_hourly) as peak_hour
      FROM attractions a
      LEFT JOIN latest_queue lq ON lq."attractionId" = a.id
      WHERE a."parkId" = $1::uuid
      `;
      queryParams = [
        parkId,
        windowAgo,
        startOfDay,
        now,
        resolvedTimezone,
        this.MIN_WAIT_TIME_THRESHOLD,
      ];
    }

    const result = await this.queueDataRepository.query(query, queryParams);

    const stats = result[0];

    // Calculate occupancy for crowd level AND current avg wait
    // This uses the unified Smart Logic (> 10m fallback to > 0)
    const occupancy = await this.calculateParkOccupancy(parkId);

    // Get TODAY's aggregate statistics for history
    const avgWaitToday = roundToNearest5Minutes(stats?.avg_wait_today || 0);
    // Park peak: average of the peaks (per-headliner MAX today, then divide by count) — typical peak load, not dominated by a single ride
    let peakWaitToday = roundToNearest5Minutes(stats?.max_wait_today || 0);
    const headliners = await this.headlinerAttractionRepository.find({
      where: { parkId },
      select: ["parkId", "attractionId"],
    });
    const headlinerIds = headliners.map((h) => h.attractionId);
    if (headlinerIds.length > 0) {
      const headlinerMaxPerRide = await this.queueDataRepository.query(
        `
        SELECT qd."attractionId", MAX(qd."waitTime") as max_wait
        FROM queue_data qd
        WHERE qd."attractionId" = ANY($1)
          AND qd.timestamp BETWEEN $2 AND $3
          AND qd."queueType" = 'STANDBY'
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
        GROUP BY qd."attractionId"
        `,
        [headlinerIds, startOfDay, now],
      );
      if (headlinerMaxPerRide.length > 0) {
        const sum = headlinerMaxPerRide.reduce(
          (acc: number, row: { max_wait: string | number }) =>
            acc + Number(row.max_wait ?? 0),
          0,
        );
        const avgHeadlinerMax = sum / headlinerMaxPerRide.length;
        peakWaitToday = roundToNearest5Minutes(avgHeadlinerMax);
      }
    }

    // Optimistic calculation: totalAttractions - explicitlyClosedCount
    // This matches Discovery Service logic and prevents showing "0 operating" during data gaps
    const totalAttractions = parseInt(stats?.total_count) || 0;
    const explicitlyClosedCount = parseInt(stats?.explicitly_closed_count) || 0;

    // Use occupancy data to determine if park is likely open
    const isParkLikelyOpen =
      occupancy.current > 0 || (occupancy.breakdown?.currentAvgWait ?? 0) > 0;

    const operatingAttractions = isParkLikelyOpen
      ? Math.max(0, totalAttractions - explicitlyClosedCount)
      : 0;

    // Caching Strategy for Typical Peak Hour (Heavy Query, changes slowly).
    // NOTE: `park:typical-peak:` (with hyphen) holds the typical peak HOUR
    // ("HH:00" string) — distinct from `park:typicalpeak:` (no hyphen) in
    // cacheTypicalDayPeak, which holds the typical-day peak WAIT (number).
    const typicalPeakKey = `park:typical-peak:${parkId}`;
    let typicalPeakHour = await this.redis.get(typicalPeakKey);

    if (!typicalPeakHour) {
      typicalPeakHour = await this.getTypicalPeakHour(parkId, resolvedTimezone);
      if (typicalPeakHour) {
        await this.redis.set(
          typicalPeakKey,
          typicalPeakHour,
          "EX",
          24 * 60 * 60,
        ); // 24 hours
      }
    }

    const todayPeakRaw = stats?.peak_hour
      ? `${String(Math.floor(stats.peak_hour)).padStart(2, "0")}:00`
      : null;

    // determine which peak hour to show (current hour must be in park timezone)
    let displayPeakHour = todayPeakRaw;
    // Track provenance so clients can render confidence ("≈ 14:00" for forecasts).
    let peakHourSource: PeakHourSource | null = todayPeakRaw
      ? "observed_today"
      : null;

    // If we have a typical peak prediction
    if (typicalPeakHour) {
      const nowInPark = getCurrentTimeInTimezone(resolvedTimezone);
      const currentHour = nowInPark.getHours();
      const typicalHour = parseInt(typicalPeakHour.split(":")[0]);

      // If today's peak hasn't happened yet (or it's early), show prediction
      // e.g. Now is 10:00, Typical is 14:00 -> Show 14:00
      // e.g. Now is 16:00, Typical is 14:00 -> Show Today's Peak (or Typical if today was weirdly flat)
      if (currentHour < typicalHour) {
        displayPeakHour = typicalPeakHour;
        // We have a live signal today but the peak is still ahead -> forecast.
        // No live signal at all -> historical fallback.
        peakHourSource = todayPeakRaw ? "prediction" : "historical_fallback";
      } else if (!displayPeakHour) {
        displayPeakHour = typicalPeakHour;
        peakHourSource = "historical_fallback";
      }
      // If currentHour > typicalHour and we have displayPeakHour (Actual), keep Actual.
    }

    // Validate against today's operating hours: do not show peak hour after closing (e.g. Efteling 19:00)
    const closingTime = await this.getEffectiveEndTime(
      parkId,
      resolvedTimezone,
    );
    if (closingTime && displayPeakHour) {
      const closingHour = parseInt(
        formatInTimeZone(closingTime, resolvedTimezone, "H"),
        10,
      );
      const peakHour = parseInt(displayPeakHour.split(":")[0], 10);
      if (peakHour >= closingHour) {
        displayPeakHour = null;
        peakHourSource = null;
      }
    }

    let history: import("./types/analytics-response.type").WaitTimeHistoryItem[] =
      [];
    try {
      // PERFORMANCE: Only select timezone, not entire park entity
      const park = await this.parkRepository.findOne({
        where: { id: parkId },
        select: ["id", "timezone"],
      });
      if (park && park.timezone) {
        const startTime = await this.getEffectiveStartTime(
          parkId,
          park.timezone,
        );
        const endTime = await this.getEffectiveEndTime(parkId, park.timezone);
        history = await this.getParkWaitTimeHistory(parkId, startTime, endTime);
      } else {
        // Fallback if park not found or no timezone
        const startOfDay = getStartOfDayInTimezone("UTC");
        history = await this.getParkWaitTimeHistory(parkId, startOfDay);
      }
    } catch (error) {
      this.logger.warn(
        `Failed to fetch park wait time history for ${parkId}:`,
        error,
      );
      // Continue with empty history rather than failing the entire analytics fetch
    }

    // Convert "HH:00" peak hour (park local time) to a full ISO-8601 datetime with timezone
    // offset so clients don't misinterpret the value as UTC.
    // e.g. "11:00" in America/New_York → "2026-03-02T11:00:00-05:00"
    let peakHourIso: string | null = null;
    if (displayPeakHour) {
      const todayPeakDateStr = getCurrentDateInTimezone(resolvedTimezone);
      const peakUtc = fromZonedTime(
        `${todayPeakDateStr}T${displayPeakHour}:00`,
        resolvedTimezone,
      );
      peakHourIso = formatInTimeZone(
        peakUtc,
        resolvedTimezone,
        "yyyy-MM-dd'T'HH:mm:ssxxx",
      );
    }

    const statsDto: ParkStatisticsDto = {
      // Use UNIFIED Smart Logic from calculateParkOccupancy
      avgWaitTime: occupancy.breakdown?.currentAvgWait || 0,
      avgWaitToday,
      peakWaitToday,
      peakHour: peakHourIso,
      peakHourLocal: displayPeakHour,
      peakHourConfidence: peakHourConfidence(peakHourSource),
      peakHourSource,
      // Use utility method for consistency
      crowdLevel: this.getParkCrowdLevel(occupancy.current),
      totalAttractions,
      operatingAttractions,
      closedAttractions: totalAttractions - operatingAttractions,
      timestamp: now,
      history,
    };

    // Cache for 5 minutes
    await this.redis.setex(cacheKey, 5 * 60, JSON.stringify(statsDto));

    return statsDto;
  }

  /**
   * Get batch attraction statistics for today
   * Returns: Avg, Min, Max wait time, and Timestamp of Max Wait
   *
   * @param attractionIds - Array of attraction IDs to fetch statistics for
   * @param startTime - Start time for filtering (e.g. Schedule Opening Time or Start of Day)
   */
  async getBatchAttractionStatistics(
    attractionIds: string[],
    startTime: Date,
  ): Promise<
    Map<
      string,
      {
        avg: number;
        min: number;
        max: number;
        maxTimestamp: Date | null;
        count: number;
      }
    >
  > {
    if (attractionIds.length === 0) return new Map();

    const now = new Date();
    const startOfDay = startTime;

    // Use CTE to get stats and find the timestamp of the max wait
    const result = await this.queueDataRepository.query(
      `
      WITH stats AS (
        SELECT 
          qd."attractionId",
          AVG(qd."waitTime") as avg_wait,
          MIN(qd."waitTime") as min_wait,
          MAX(qd."waitTime") as max_wait,
          COUNT(*) as count
        FROM queue_data qd
        WHERE qd."attractionId" = ANY($1)
          AND qd.timestamp BETWEEN $2 AND $3
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
          AND qd."waitTime" > 0
          AND qd."queueType" = 'STANDBY'
        GROUP BY qd."attractionId"
      ),
      max_timestamps AS (
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd.timestamp as max_timestamp
        FROM queue_data qd
        INNER JOIN stats s ON s."attractionId" = qd."attractionId"
        WHERE qd."attractionId" = ANY($1)
          AND qd.timestamp BETWEEN $2 AND $3
          AND qd."waitTime" IS NOT NULL
          -- Floating point comparison check
          AND ABS(qd."waitTime" - s.max_wait) < 0.01
          AND qd."queueType" = 'STANDBY'
        ORDER BY qd."attractionId", qd.timestamp DESC
      )
      SELECT 
        s."attractionId",
        s.avg_wait,
        s.min_wait,
        s.max_wait,
        s.count,
        mt.max_timestamp
      FROM stats s
      LEFT JOIN max_timestamps mt ON mt."attractionId" = s."attractionId"
      `,
      [attractionIds, startOfDay, now],
    );

    const map = new Map();
    for (const row of result) {
      map.set(row.attractionId, {
        avg: row.avg_wait
          ? roundToNearest5Minutes(parseFloat(row.avg_wait))
          : 0,
        min: row.min_wait
          ? roundToNearest5Minutes(parseFloat(row.min_wait))
          : 0,
        max: row.max_wait
          ? roundToNearest5Minutes(parseFloat(row.max_wait))
          : 0,
        maxTimestamp: row.max_timestamp ? new Date(row.max_timestamp) : null,
        count: parseInt(row.count),
      });
    }

    return map;
  }

  /**
   * Low-level sparkline fetch for a set of attractions that all share the same
   * startTime (i.e. they belong to the same park, or the caller has already
   * resolved the correct window).
   *
   * Returns deduplicated, 5-min-rounded timestamp/waitTime pairs ordered oldest
   * to newest. Only changed values are recorded, so the array stays small.
   *
   * For attractions from multiple parks (different timezones / opening times)
   * use getAttractionSparklinesBatch instead — it resolves startTime per park.
   *
   * @param attractionIds - Attraction IDs to fetch history for
   * @param startTime - Window start (e.g. result of getEffectiveStartTime)
   */
  async getBatchAttractionWaitTimeHistory(
    attractionIds: string[],
    startTime: Date,
  ): Promise<Map<string, { timestamp: string; waitTime: number }[]>> {
    if (attractionIds.length === 0) return new Map();

    // Use provided start time
    const startOfDay = startTime;

    const result = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("qd.attractionId", "attractionId")
      .addSelect("qd.timestamp", "timestamp")
      .addSelect("qd.waitTime", "waitTime")
      .where("qd.attractionId IN (:...ids)", { ids: attractionIds })
      .andWhere("qd.timestamp >= :start", { start: startOfDay })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .andWhere("qd.waitTime IS NOT NULL")
      .andWhere("qd.queueType = :type", { type: "STANDBY" })
      .orderBy("qd.timestamp", "ASC")
      .getRawMany();

    const map = new Map<string, { timestamp: string; waitTime: number }[]>();

    for (const row of result) {
      if (!map.has(row.attractionId)) {
        map.set(row.attractionId, []);
      }
      const list = map.get(row.attractionId)!;
      const currentWait = roundToNearest5Minutes(parseFloat(row.waitTime));

      // Deduplicate: Only record if value changed from the last recorded point
      if (list.length === 0 || list[list.length - 1].waitTime !== currentWait) {
        list.push({
          timestamp: new Date(row.timestamp).toISOString(),
          waitTime: currentWait,
        });
      }
    }

    return map;
  }

  /**
   * Fetch sparklines for attractions that may span multiple parks.
   *
   * Each park's effective start time (schedule opening or local midnight) is
   * resolved independently, so rides from Tokyo and Orlando both get the
   * correct window without UTC drift.
   *
   * Use this for multi-park contexts (global stats, recommendations, …).
   * For a single park, keep using getBatchAttractionWaitTimeHistory directly
   * with the startTime you already have.
   *
   * @param attractions - Attractions with their parkId and timezone
   * @returns Map of attractionId → sparkline data points
   */
  async getAttractionSparklinesBatch(
    attractions: { id: string; parkId: string; timezone: string }[],
  ): Promise<Map<string, { timestamp: string; waitTime: number }[]>> {
    if (attractions.length === 0) return new Map();

    // Group by park so we resolve startTime once per park, not once per ride.
    const byPark = new Map<string, { ids: string[]; timezone: string }>();
    for (const a of attractions) {
      if (!byPark.has(a.parkId)) {
        byPark.set(a.parkId, { ids: [], timezone: a.timezone });
      }
      byPark.get(a.parkId)!.ids.push(a.id);
    }

    const results = await Promise.all(
      [...byPark.entries()].map(([parkId, { ids, timezone }]) =>
        this.getEffectiveStartTime(parkId, timezone).then((startTime) =>
          this.getBatchAttractionWaitTimeHistory(ids, startTime),
        ),
      ),
    );

    // Merge per-park maps into one.
    const merged = new Map<string, { timestamp: string; waitTime: number }[]>();
    for (const partial of results) {
      for (const [id, points] of partial) {
        merged.set(id, points);
      }
    }
    return merged;
  }

  /**
   * Get park-wide wait time history for today
   *
   * @param parkId - Park ID
   * @param startTime - Start time for filtering (e.g. Schedule Opening Time or Start of Day)
   */
  async getParkWaitTimeHistory(
    parkId: string,
    startTime: Date,
    endTime?: Date | null,
  ): Promise<import("./types/analytics-response.type").WaitTimeHistoryItem[]> {
    const now = new Date();
    // Cap end at closing time (if known) or now — whichever is earlier
    const effectiveEnd = endTime && endTime < now ? endTime : now;

    // Group by 10-minute intervals to get a smooth average trend for the park.
    // waitTime > 0 excludes placeholder/walk-on reports that deflate the P90.
    const result = await this.queueDataRepository.query(
      `
      SELECT
        to_timestamp(floor(extract(epoch from qd.timestamp) / 600) * 600) AT TIME ZONE 'UTC' as interval_timestamp,
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime")::numeric) as avg_wait
      FROM queue_data qd
      INNER JOIN attractions a ON a.id = qd."attractionId"
      WHERE a."parkId" = $1::uuid
        AND qd.timestamp >= $2
        AND qd.timestamp <= $3
        AND qd.status = 'OPERATING'
        AND qd."waitTime" IS NOT NULL
        AND qd."waitTime" > 0
        AND qd."queueType" = 'STANDBY'
      GROUP BY interval_timestamp
      ORDER BY interval_timestamp ASC
      `,
      [parkId, startTime, effectiveEnd],
    );

    return result.map(
      (row: { interval_timestamp: Date; avg_wait: string }) => ({
        timestamp: new Date(row.interval_timestamp).toISOString(),
        waitTime: roundToNearest5Minutes(parseFloat(row.avg_wait) || 0),
      }),
    );
  }

  /**
   * Get park statistics for multiple parks in batch
   * Optimized version to avoid N+1 queries
   *
   * @param parkIds - Array of park IDs
   * @returns Map of parkId -> ParkStatisticsDto
   */
  async getBatchParkStatistics(
    parkIds: string[],
    context?: Map<string, { timezone: string; startTime: Date }>,
  ): Promise<Map<string, ParkStatisticsDto>> {
    const resultMap = new Map<string, ParkStatisticsDto>();

    if (parkIds.length === 0) {
      return resultMap;
    }

    // Resolve context if missing
    let resolvedContext = context;
    if (!resolvedContext) {
      resolvedContext = new Map();
      // Batch fetch timezones
      const parks = await this.parkRepository.find({
        where: { id: In(parkIds) },
        select: ["id", "timezone"],
      });

      // Calculate effective start times in batch
      const startTimeMap = await this.getBatchEffectiveStartTime(parks);
      for (const park of parks) {
        const timezone = park.timezone || "UTC";
        const startTime = startTimeMap.get(park.id)!;
        resolvedContext.set(park.id, { timezone, startTime });
      }
    }

    // Execute statistics queries in sequential batches to avoid connection pool contention
    // and "client.query() already executing" deprecation warnings.
    const results: (ParkStatisticsDto | null)[] = [];
    const batchSize = 5; // Process 5 parks at a time

    for (let i = 0; i < parkIds.length; i += batchSize) {
      const batchIds = parkIds.slice(i, i + batchSize);
      const batchResults = await Promise.all(
        batchIds.map((id) => {
          const ctx = resolvedContext!.get(id);
          return this.getParkStatistics(
            id,
            ctx?.timezone,
            ctx?.startTime,
          ).catch((err) => {
            this.logger.warn(`Failed to get statistics for park ${id}:`, err);
            return null;
          });
        }),
      );
      results.push(...batchResults);
    }

    // Map results
    parkIds.forEach((id, index) => {
      const stats = results[index];
      if (stats) {
        resultMap.set(id, stats);
      }
    });

    return resultMap;
  }

  /**
   * Get average wait time for the park over a period
   */
  private async getDailyAverageWaitTime(
    parkId: string,
    start: Date,
    end: Date,
  ): Promise<number | null> {
    const result = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("AVG(qd.waitTime)", "avgWait")
      .innerJoin("qd.attraction", "attraction")
      .where("attraction.parkId = :parkId", { parkId })
      .andWhere("qd.timestamp BETWEEN :start AND :end", { start, end })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .andWhere("qd.waitTime IS NOT NULL")
      .andWhere("qd.waitTime >= 10")
      .andWhere("qd.queueType = 'STANDBY'")
      .getRawOne();

    return result?.avgWait ? parseFloat(result.avgWait) : null;
  }

  /**
   * Find peak hour today
   */
  private async getPeakHourToday(
    parkId: string,
    startOfDay: Date,
    now: Date,
  ): Promise<string | null> {
    const result = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("EXTRACT(HOUR FROM qd.timestamp)", "hour")
      .addSelect('AVG(qd."waitTime")', "avgWait")
      .innerJoin("qd.attraction", "attraction")
      .where('attraction."parkId" = :parkId', { parkId })
      .andWhere("qd.timestamp BETWEEN :startOfDay AND :now", {
        startOfDay,
        now,
      })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .andWhere('qd."waitTime" IS NOT NULL')
      .andWhere("qd.\"queueType\" = 'STANDBY'")
      .groupBy("hour")
      .orderBy('"avgWait"', "DESC")
      .limit(1)
      .getRawOne();

    if (!result?.hour) return null;

    const hour = parseInt(result.hour);
    return `${hour.toString().padStart(2, "0")}:00`;
  }

  /**
   * Predict the Typical Peak Hour for this time of year (Seasonal Sliding Window).
   *
   * Uses historical data from the last 60 days to determine the hour with the highest average wait.
   * This adapts to seasonal changes (e.g. earlier closing in winter) without needing explicit schedules.
   */
  private async getTypicalPeakHour(
    parkId: string,
    timezone: string,
  ): Promise<string | null> {
    // Sliding window: Last 60 days
    // This allows the "Peak" to shift from 16:00 (Summer) to 14:00 (Winter) automatically
    const validSince = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000);

    const result = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE :timezone)", "hour")
      .addSelect('AVG(qd."waitTime")', "avgWait")
      .innerJoin("qd.attraction", "attraction")
      .where('attraction."parkId" = :parkId', { parkId })
      .andWhere("qd.timestamp >= :validSince", { validSince })
      .andWhere("qd.status = 'OPERATING'")
      .andWhere('qd."waitTime" IS NOT NULL')
      .andWhere("qd.\"queueType\" = 'STANDBY'")
      .setParameter("timezone", timezone)
      .groupBy("hour")
      .orderBy('"avgWait"', "DESC")
      .limit(1)
      .getRawOne();

    if (!result?.hour) return null;

    const hour = parseInt(result.hour);
    return `${hour.toString().padStart(2, "0")}:00`;
  }

  /**
   * Get attraction counts by status
   * Matches logic in ParksController.buildIntegratedParkResponse:
   * - Attractions WITH queue_data: use status from queue_data
   * - Attractions WITHOUT queue_data: considered CLOSED
   */
  private async getAttractionCounts(
    parkId: string,
  ): Promise<import("./types/analytics-response.type").AttractionCounts> {
    const total = await this.attractionRepository.count({
      where: { parkId },
    });

    // Get attractions with their latest queue data status
    // Matches controller logic: attractions without queue data are CLOSED
    const result = await this.queueDataRepository.query(
      `
      WITH latest_queue AS (
        SELECT DISTINCT ON (qd."attractionId") 
          qd."attractionId",
          qd.status
        FROM queue_data qd
        WHERE qd."queueType" = 'STANDBY'
        ORDER BY qd."attractionId", qd.timestamp DESC
      )
      SELECT COUNT(*) as operating_count
      FROM attractions a
      LEFT JOIN latest_queue lq ON lq."attractionId" = a.id
      WHERE a."parkId" = $1::uuid
      AND lq.status = 'OPERATING'
      `,
      [parkId],
    );

    const operating = result[0]?.operating_count
      ? parseInt(result[0].operating_count)
      : 0;
    const closed = total - operating;

    return { total, operating, closed };
  }

  /**
   * Determine crowd level from occupancy percentage
   *
   * **Single Source of Truth** for crowd level calculation across all services.
   * All services should use this method instead of implementing their own logic.
   *
   * **Peak-vs-peak thresholds (±10% around the typical peak):**
   * Callers pass `(current_peak / p90_baseline) * 100`; with the P50
   * fallback when no P90 row exists yet, the math stays apples-to-apples.
   * - 100% = baseline = **"moderate"** (a typical day's peak)
   * - very_low: ≤ 60% — much quieter than expected
   * - low: 61-89% — below expected
   * - moderate: 90-110% — around expected (±10%)
   * - high: 111-150% — above expected
   * - very_high: 151-200% — significantly above expected
   * - extreme: > 200% — exceptionally crowded
   *
   * @param occupancy - Occupancy percentage relative to the baseline (0-300+)
   * @returns Crowd level rating
   *
   * @public - Use this method from other services instead of duplicating logic
   */
  public determineCrowdLevel(occupancy: number): CrowdLevel {
    return determineCrowdLevel(occupancy);
  }

  /**
   * Determine comparison status from percentage difference
   */
  private determineComparisonStatus(
    comparedToTypical: number,
  ): "much_lower" | "lower" | "typical" | "higher" | "much_higher" {
    if (comparedToTypical <= -50) return "much_lower"; // 50%+ below typical
    if (comparedToTypical < -10) return "lower"; // 10-50% below
    if (comparedToTypical <= 10) return "typical"; // Within ±10%
    if (comparedToTypical <= 50) return "higher"; // 10-50% above
    return "much_higher"; // 50%+ above typical
  }
  /**
   * Convert crowd level rating to comparison text
   * Maps 6-level ratings to 5-level comparison status (combines extreme with much_higher)
   *
   * @param rating - Crowd level rating (very_low to extreme)
   * @returns Comparison status text
   */
  public getComparisonText(
    rating: string,
  ): "much_lower" | "lower" | "typical" | "higher" | "much_higher" {
    switch (rating) {
      case "very_low":
        return "much_lower"; // <= 30% of baseline
      case "low":
        return "lower"; // 30-60% of baseline
      case "moderate":
        return "typical"; // 60-110% of baseline
      case "high":
        return "higher"; // 110-140% of baseline
      case "very_high":
        return "much_higher"; // 140-180% of baseline
      case "extreme":
        return "much_higher"; // > 180% of baseline (maps to much_higher)
      default:
        return "typical";
    }
  }

  /**
   * Get attraction-specific statistics
   *
   * @param attractionId - Attraction ID
   * @param startTime - Start time for filtering (e.g. Schedule Opening Time or Start of Day)
   */
  async getAttractionStatistics(
    attractionId: string,
    startTime: Date,
    timezone: string,
  ): Promise<AttractionStatisticsDto> {
    const now = new Date();
    // Calculate current hour/DOW in PARK TIME
    const currentHour = parseInt(formatInTimeZone(now, timezone, "H"));
    const currentDayOfWeek = parseInt(formatInTimeZone(now, timezone, "i")) % 7;

    // Get today's statistics (using provided start time)
    // Use batch method (efficient CTE) instead of multiple queries
    const batchStats = await this.getBatchAttractionStatistics(
      [attractionId],
      startTime,
    );
    const batchStat = batchStats.get(attractionId);

    const todayStats = {
      avg: batchStat?.avg || null,
      max: batchStat?.max || null,
      min: batchStat?.min || null,
      count: batchStat?.count || 0,
      maxTimestamp: batchStat?.maxTimestamp || null,
    };

    // Get typical wait (AVG) and P95 for this hour/weekday (2-year average)
    const { avg: typicalWait, p95: p95ThisHour } =
      await this.getTypicalStatsForHour(
        attractionId,
        currentHour,
        currentDayOfWeek,
        timezone,
      );

    // Calculate current vs typical
    const currentVsTypical =
      todayStats.avg && typicalWait
        ? Math.round(((todayStats.avg - typicalWait) / typicalWait) * 100)
        : null;

    // Get wait time history for today (using provided start time)
    // Re-use the batch logic but for single ID
    const historyMap = await this.getBatchAttractionWaitTimeHistory(
      [attractionId],
      startTime,
    );
    const history = historyMap.get(attractionId) || [];

    return {
      avgWaitToday: todayStats.avg,
      peakWaitToday: todayStats.max,
      peakWaitTimestamp: todayStats.maxTimestamp,
      minWaitToday: todayStats.min,
      typicalWaitThisHour: typicalWait,
      percentile95ThisHour: p95ThisHour,
      currentVsTypical,
      dataPoints: todayStats.count,
      timestamp: now,
      history,
    };
  }

  /**
   * Get typical wait (AVG) and P95 for a specific attraction/hour/weekday.
   * Both values are computed in a single DB aggregation and cached for 24 h —
   * they are 2-year historical averages that barely change day to day.
   */
  private async getTypicalStatsForHour(
    attractionId: string,
    hour: number,
    dayOfWeek: number,
    timezone: string,
  ): Promise<{ avg: number | null; p95: number | null }> {
    const cacheKey = `analytics:typical:${attractionId}:${hour}:${dayOfWeek}`;
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      try {
        return JSON.parse(cached);
      } catch {
        // fall through
      }
    }

    const twoYearsAgo = new Date();
    twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);

    // Read from the pre-aggregated hourly rollup (queue_data_aggregates: 1 row per
    // attraction/hour-bucket, OPERATING+STANDBY only, populated nightly) instead of
    // scanning millions of raw queue_data rows — drops this from ~15s to <10ms (it was the
    // top offender in the slow-query log). Served by the (attractionId, hour) index; the
    // EXTRACT filters are residual over a single attraction's ~hourly rows.
    //   avg = sample-weighted mean of bucket means → EXACT (verified vs raw on live).
    //   p95 = 95th percentile of the per-bucket p95s → an approximation (cross-bucket
    //         percentiles can't be recombined exactly), but it tracks the raw 2-year p95
    //         well — e.g. raw 55/70/35/75 → 56/70/37/89 — far better than weight-averaging
    //         the bucket p95s (which collapses the cross-day tail). Fine for the rounded
    //         "typical wait this hour" reference.
    const rows = await this.queueDataRepository.query(
      `SELECT
         SUM(mean * "sampleCount") / NULLIF(SUM("sampleCount"), 0)  AS avg_wait,
         PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY p95)          AS p95_wait
       FROM queue_data_aggregates
       WHERE "attractionId" = $1
         AND hour >= $2
         AND EXTRACT(HOUR FROM hour AT TIME ZONE $3) = $4
         AND EXTRACT(DOW  FROM hour AT TIME ZONE $3) = $5`,
      [attractionId, twoYearsAgo, timezone, hour, dayOfWeek],
    );

    const row = rows[0];
    const result = {
      avg:
        row?.avg_wait != null
          ? roundToNearest5Minutes(parseFloat(row.avg_wait))
          : null,
      p95:
        row?.p95_wait != null
          ? roundToNearest5Minutes(parseFloat(row.p95_wait))
          : null,
    };

    await this.redis
      .set(cacheKey, JSON.stringify(result), "EX", 24 * 60 * 60)
      .catch((e) =>
        this.logger.debug(
          `Redis analytics cache set failed for key ${cacheKey}: ${e?.message ?? e}`,
        ),
      );

    return result;
  }

  /**
   * Detect wait time trend for a specific attraction
   * Analyzes last 2-3 hours to determine if wait times are increasing/decreasing/stable
   *
   * @param attractionId - Attraction ID
   * @param queueType - Queue type to analyze (defaults to STANDBY)
   * @returns Trend object with direction and metrics
   */
  async detectAttractionTrend(
    attractionId: string,
    queueType: string = "STANDBY",
    currentSpotWait?: number | null,
  ): Promise<import("./types/analytics-response.type").WaitTimeTrend> {
    // Cache only the historical bucket averages (slow-changing, shared across callers).
    // computeTrend is always run live because it depends on currentSpotWait which
    // varies per caller and must not bleed between cached results.
    const bucketCacheKey = `analytics:trend:buckets:${attractionId}:${queueType}`;
    const cachedBuckets = safeJsonParse<{
      avgLastHour: number | null;
      avgTwoToOne: number | null;
      avgThreeToTwo: number | null;
    }>(await this.redis.get(bucketCacheKey));

    let avgLastHour: number | null;
    let avgTwoToOne: number | null;
    let avgThreeToTwo: number | null;

    if (cachedBuckets) {
      ({ avgLastHour, avgTwoToOne, avgThreeToTwo } = cachedBuckets);
    } else {
      const now = new Date();
      const threeHoursAgo = new Date(now.getTime() - 180 * 60 * 1000);
      const twoHoursAgo = new Date(now.getTime() - 120 * 60 * 1000);
      const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);

      // Optimized: Single query using time buckets
      const result = await this.queueDataRepository.query(
        `
        SELECT
          CASE
            WHEN qd.timestamp >= $4 THEN 1 -- Last 1h (Recent)
            WHEN qd.timestamp >= $3 AND qd.timestamp < $4 THEN 2 -- 1h-2h ago (Previous)
            WHEN qd.timestamp >= $2 AND qd.timestamp < $3 THEN 3 -- 2h-3h ago (Previous Previous)
          END as bucket,
          AVG(qd."waitTime") as avg_wait
        FROM queue_data qd
        WHERE qd."attractionId" = $1::uuid
          AND qd.timestamp >= $2
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
          AND qd."queueType" = $5
        GROUP BY bucket
      `,
        [attractionId, threeHoursAgo, twoHoursAgo, oneHourAgo, queueType],
      );

      const buckets: Record<number, number> = {};
      for (const row of result) {
        if (row.bucket && row.avg_wait) {
          buckets[row.bucket] = parseFloat(row.avg_wait);
        }
      }

      avgLastHour = buckets[1] || null;
      avgTwoToOne = buckets[2] || null;
      avgThreeToTwo = buckets[3] || null;

      await this.redis.set(
        bucketCacheKey,
        JSON.stringify({ avgLastHour, avgTwoToOne, avgThreeToTwo }),
        "EX",
        this.TTL_REALTIME,
      );
    }

    // Not enough data
    if (
      avgLastHour === null ||
      (avgTwoToOne === null && currentSpotWait === undefined)
    ) {
      return {
        trend: "stable",
        changeRate: 0,
        recentAverage: avgLastHour,
        previousAverage: avgTwoToOne,
      };
    }

    // Always compute trend live — depends on currentSpotWait which must not be cached
    const changeRate =
      avgTwoToOne !== null && avgLastHour !== null
        ? avgLastHour - avgTwoToOne
        : 0;

    const trendRaw = this.computeTrend(
      currentSpotWait ?? avgLastHour,
      avgLastHour,
      avgTwoToOne,
      avgThreeToTwo,
    );

    const trend: "increasing" | "stable" | "decreasing" =
      trendRaw === "up"
        ? "increasing"
        : trendRaw === "down"
          ? "decreasing"
          : "stable";

    return {
      trend,
      changeRate: Math.round(changeRate * 10) / 10,
      recentAverage: avgLastHour ? roundToNearest5Minutes(avgLastHour) : null,
      previousAverage: avgTwoToOne ? roundToNearest5Minutes(avgTwoToOne) : null,
    };
  }

  /**
   * Helper to compute trend using Hybrid Logic:
   * 1. Fast Trend (Spot vs Recent Average)
   * 2. Slow Trend (Recent Average vs Previous Average)
   *
   * @param current - Current wait time (spot measurement)
   * @param recentAvg - Average wait time over last hour
   * @param previousAvg - Average wait time over previous hour (can be null)
   * @param previousPreviousAvg - Average wait time over hour before previous (optional, for weighted trend)
   * @param thresholdRelative - Relative threshold as fraction (default: 0.1 = 10%)
   * @param thresholdAbsolute - Absolute threshold in minutes (default: 5)
   * @returns "up" | "down" | "stable"
   */
  public computeTrend(
    current: number,
    recentAvg: number,
    previousAvg: number | null,
    previousPreviousAvg: number | null = null,
    thresholdRelative: number = 0.1,
    thresholdAbsolute: number = 5,
  ): "up" | "down" | "stable" {
    // Input validation
    if (typeof current !== "number" || isNaN(current) || current < 0) {
      this.logger.warn(
        `Invalid current value in computeTrend: ${current}, defaulting to stable`,
      );
      return "stable";
    }

    if (typeof recentAvg !== "number" || isNaN(recentAvg) || recentAvg < 0) {
      this.logger.warn(
        `Invalid recentAvg value in computeTrend: ${recentAvg}, defaulting to stable`,
      );
      return "stable";
    }

    // Validate thresholds
    if (thresholdRelative < 0 || thresholdRelative > 1) {
      this.logger.warn(
        `Invalid thresholdRelative: ${thresholdRelative}, using default 0.1`,
      );
      thresholdRelative = 0.1;
    }

    if (thresholdAbsolute < 0) {
      this.logger.warn(
        `Invalid thresholdAbsolute: ${thresholdAbsolute}, using default 5`,
      );
      thresholdAbsolute = 5;
    }

    // 1. Fast Trend Check (Immediate reaction to spikes)
    const fastDiff = current - recentAvg;
    const fastThreshold = Math.max(
      thresholdAbsolute,
      recentAvg * thresholdRelative,
    );

    if (fastDiff > fastThreshold) return "up";
    if (fastDiff < -fastThreshold) return "down";

    // 2. Slow Trend Check (Hourly Momentum)
    // Treat missing previous average as 0 (e.g. ride was closed/no data)
    // This allows detecting "0 -> 300" jumps as "UP" instead of "Stable"
    const prev = previousAvg ?? 0;

    // Validate previousAvg if provided
    let validatedPrev = prev;
    if (
      previousAvg !== null &&
      (typeof previousAvg !== "number" || isNaN(previousAvg) || previousAvg < 0)
    ) {
      this.logger.warn(
        `Invalid previousAvg value in computeTrend: ${previousAvg}, treating as 0`,
      );
      validatedPrev = 0;
    }

    // Only apply threshold check if we have enough data (at least recentAvg exists)
    // If both are 0, it's stable.
    if (recentAvg === 0 && validatedPrev === 0) return "stable";

    const threshold = Math.max(
      thresholdAbsolute,
      validatedPrev * thresholdRelative,
    );

    if (previousPreviousAvg !== null) {
      // Validate previousPreviousAvg
      if (
        typeof previousPreviousAvg !== "number" ||
        isNaN(previousPreviousAvg) ||
        previousPreviousAvg < 0
      ) {
        this.logger.warn(
          `Invalid previousPreviousAvg value in computeTrend: ${previousPreviousAvg}, using simple comparison`,
        );
        // Fall through to simple comparison
        const change = recentAvg - validatedPrev;
        if (change > threshold) return "up";
        if (change < -threshold) return "down";
        return "stable";
      }

      // Weighted average trend
      const change1 = validatedPrev - previousPreviousAvg;
      const change2 = recentAvg - validatedPrev;
      const avgChange = (change1 + change2) / 2;

      if (avgChange > threshold) return "up";
      if (avgChange < -threshold) return "down";
    } else {
      // Simple comparison
      const change = recentAvg - validatedPrev;
      if (change > threshold) return "up";
      if (change < -threshold) return "down";
    }

    return "stable";
  }

  /**
   * Get batch attraction trends
   * Efficiently calculates trends for multiple attractions using a single query
   */
  async getBatchAttractionTrends(attractionIds: string[]): Promise<
    Map<
      string,
      {
        trend: "increasing" | "stable" | "decreasing";
        changeRate: number;
        recentAverage: number | null;
        previousAverage: number | null;
      }
    >
  > {
    if (attractionIds.length === 0) return new Map();

    const now = new Date();
    const threeHoursAgo = new Date(now.getTime() - 180 * 60 * 1000);
    const twoHoursAgo = new Date(now.getTime() - 120 * 60 * 1000);
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);

    // Fetch aggregated stats for 3 buckets:
    // Bucket 1: 1h-Now (Recent)
    // Bucket 2: 2h-1h (Previous)
    // Bucket 3: 3h-2h (Oldest)
    const result = await this.queueDataRepository.query(
      `
      SELECT 
        "attractionId",
        CASE 
          WHEN timestamp >= $4 THEN 1 -- Bucket 1: 1h-Now
          WHEN timestamp >= $3 AND timestamp < $4 THEN 2 -- Bucket 2: 2h-1h
          WHEN timestamp >= $2 AND timestamp < $3 THEN 3 -- Bucket 3: 3h-2h
        END as bucket,
        AVG("waitTime") as avg_wait
      FROM queue_data
      WHERE "attractionId" = ANY($1)
        AND timestamp >= $2
        AND status = 'OPERATING'
        AND "waitTime" IS NOT NULL
        AND "queueType" = 'STANDBY'
      GROUP BY "attractionId", bucket
    `,
      [attractionIds, threeHoursAgo, twoHoursAgo, oneHourAgo],
    );

    const trendsMap = new Map();

    // Process results per attraction
    const attractionData = new Map<string, Record<number, number>>();
    for (const row of result) {
      if (!row.bucket) continue;
      if (!attractionData.has(row.attractionId)) {
        attractionData.set(row.attractionId, {});
      }
      attractionData.get(row.attractionId)![row.bucket] = parseFloat(
        row.avg_wait,
      );
    }

    for (const id of attractionIds) {
      const data = attractionData.get(id) || {};
      const avgLastHour = data[1] || null; // Recent
      const avgTwoToOne = data[2] || null; // Previous
      const avgThreeToTwo = data[3] || null; // Oldest

      if (avgLastHour === null) {
        trendsMap.set(id, {
          trend: "stable",
          changeRate: 0,
          recentAverage: avgLastHour,
          previousAverage: avgTwoToOne,
        });
        continue;
      }

      const changeRate =
        avgTwoToOne !== null && avgLastHour !== null
          ? avgLastHour - avgTwoToOne
          : 0;

      const trendRaw = this.computeTrend(
        avgLastHour,
        avgLastHour,
        avgTwoToOne,
        avgThreeToTwo,
      );

      const trend: "increasing" | "stable" | "decreasing" =
        trendRaw === "up"
          ? "increasing"
          : trendRaw === "down"
            ? "decreasing"
            : "stable";

      trendsMap.set(id, {
        trend,
        changeRate: Math.round(changeRate * 10) / 10,
        recentAverage: avgLastHour ? roundToNearest5Minutes(avgLastHour) : null,
        previousAverage: avgTwoToOne
          ? roundToNearest5Minutes(avgTwoToOne)
          : null,
      });
    }

    return trendsMap;
  }

  /**
   * Calculate load rating based on current wait vs 90th percentile baseline
   *
   * Uses relative thresholds when baseline is available, absolute thresholds as fallback.
   *
   * @param current - Current wait time in minutes
   * @param baseline - 90th percentile baseline wait time
   * @returns Object with rating and baseline value
   */
  /**
   * Get load rating from current wait time and baseline
   * Uses unified thresholds consistent with determineCrowdLevel
   *
   * @param current - Current wait time
   * @param baseline - Baseline (typically P50 median; P90 fallback)
   * @returns Rating and baseline
   */
  public getLoadRating(
    current: number,
    baseline: number,
  ): {
    rating: "very_low" | "low" | "moderate" | "high" | "very_high" | "extreme";
    baseline: number;
  } {
    // STRICT baseline-relative: no absolute threshold fallbacks.
    // If no baseline available, default to 'moderate' — honest about
    // lack of data rather than guessing an absolute cutoff.
    if (baseline === 0 || current === 0) {
      return { rating: "moderate", baseline };
    }

    // Calculate occupancy percentage: (current / baseline) * 100
    const occupancy = (current / baseline) * 100;

    // Use unified thresholds (same as determineCrowdLevel)
    const rating = this.determineCrowdLevel(occupancy);

    return { rating, baseline };
  }

  /**
   * Get global real-time statistics
   *
   * - Open vs Closed parks
   * - Top/Bottom parks by average wait time
   * - Longest/Shortest wait rides
   *
   * Cached for 5 minutes.
   */

  /** @see getGlobalRealtimeStats */
  private buildParkStatsItem(
    row: {
      id: string;
      name: string;
      slug: string;
      city: string;
      country: string;
      timezone: string;
      countrySlug: string;
      continentSlug: string;
      citySlug: string;
      avg_wait: number | string;
      total_attractions: string;
      explicitly_closed_attractions: string;
    },
    crowdLevel: string | null,
  ): ParkStatsItemDto {
    return {
      id: row.id,
      name: row.name,
      slug: row.slug,
      city: row.city,
      country: row.country,
      timezone: row.timezone,
      countrySlug: row.countrySlug,
      averageWaitTime: roundToNearest5Minutes(parseFloat(String(row.avg_wait))),
      url: buildParkUrl(row),
      totalAttractions: parseInt(row.total_attractions || "0"),
      operatingAttractions: Math.max(
        0,
        parseInt(row.total_attractions || "0") -
          parseInt(row.explicitly_closed_attractions || "0"),
      ),
      crowdLevel,
    };
  }

  /** @see getGlobalRealtimeStats */
  private buildAttractionStatsItem(
    row: {
      attractionId: string;
      attractionName: string;
      attractionSlug: string;
      parkName: string;
      slug: string;
      city: string;
      country: string;
      countrySlug: string;
      timezone: string;
      waitTime: number;
      continentSlug: string;
      citySlug: string;
    },
    enrichment: {
      crowdLevel: string | null;
      sparkline: { timestamp: string; waitTime: number }[];
      stats: AttractionStatisticsDto | null;
    },
  ): AttractionStatsItemDto {
    return {
      id: row.attractionId,
      name: row.attractionName,
      slug: row.attractionSlug,
      parkName: row.parkName,
      parkSlug: row.slug,
      parkCity: row.city,
      parkCountry: row.country,
      parkCountrySlug: row.countrySlug,
      parkTimezone: row.timezone,
      waitTime: row.waitTime,
      url: buildAttractionUrl(row, { slug: row.attractionSlug }),
      crowdLevel: enrichment.crowdLevel,
      sparkline: enrichment.sparkline,
      avgWaitToday: enrichment.stats?.avgWaitToday ?? null,
      minWaitToday: enrichment.stats?.minWaitToday ?? null,
      peakWaitToday: enrichment.stats?.peakWaitToday ?? null,
      peakWaitTimestamp:
        enrichment.stats?.peakWaitTimestamp?.toISOString() ?? null,
      typicalWaitThisHour: enrichment.stats?.typicalWaitThisHour ?? null,
      currentVsTypical: enrichment.stats?.currentVsTypical ?? null,
    };
  }

  async getGlobalRealtimeStats(): Promise<GlobalStatsDto> {
    const cacheKey = "analytics:global_stats:v2";
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      return JSON.parse(cached);
    }

    // 1. Get Park Statuses & Average Waits concurrently
    // IMPORTANT: Only consider parks that are currently OPERATING
    // to avoid showing closed parks with 0 wait times
    const activeParksResult = await this.queueDataRepository.query(`
      WITH schedule_open_parks AS (
        SELECT DISTINCT s."parkId"
        FROM schedule_entries s
        WHERE s."scheduleType" = 'OPERATING'
          AND s."openingTime" <= NOW()
          AND s."closingTime" > NOW()
      ),
      ride_open_parks AS (
        -- UNKNOWN-schedule parks detected as open via live ride data (no OPERATING entries ever)
        SELECT a."parkId"
        FROM attractions a
        JOIN queue_data qd ON qd."attractionId" = a.id
          AND qd.timestamp > NOW() - INTERVAL '2 hours'
          AND qd."waitTime" IS NOT NULL
        WHERE NOT EXISTS (
          SELECT 1 FROM schedule_entries se WHERE se."parkId" = a."parkId" AND se."scheduleType" = 'OPERATING'
        )
        AND NOT EXISTS (
          SELECT 1 FROM schedule_entries se WHERE se."parkId" = a."parkId" AND se."scheduleType" = 'CLOSED' AND se.date = CURRENT_DATE
        )
        GROUP BY a."parkId"
        HAVING COUNT(*) >= 3
          AND 100.0 * COUNT(CASE WHEN qd."waitTime" >= 10 THEN 1 END) / COUNT(*) >= 25
      ),
      park_status AS (
        SELECT "parkId" FROM schedule_open_parks
        UNION
        SELECT "parkId" FROM ride_open_parks
      ),
      latest_updates AS (
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd."waitTime",
          qd."status",
          a."parkId",
          qd.timestamp
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        JOIN park_status ps ON ps."parkId" = a."parkId"
        WHERE qd.timestamp > NOW() - INTERVAL '24 hours'
          AND qd."queueType" = 'STANDBY'
        ORDER BY qd."attractionId", qd.timestamp DESC
      ),
      -- Pre-aggregate per-park attraction counts so park_stats can JOIN
      -- against this CTE once instead of running two correlated subqueries
      -- per park (a SELECT COUNT(*) + a LATERAL latest-status lookup).
      -- The LATERAL form was the main cache-miss cost: O(parks × attractions)
      -- correlated executions; this version is a single scan with FILTER.
      attraction_counts AS (
        SELECT
          a."parkId" AS "parkId",
          COUNT(*)::int AS total,
          COUNT(*) FILTER (
            WHERE lu."attractionId" IS NULL OR lu.status <> 'OPERATING'
          )::int AS closed
        FROM attractions a
        LEFT JOIN latest_updates lu ON lu."attractionId" = a.id
        GROUP BY a."parkId"
      ),
      park_stats AS (
        SELECT
          p.id,
          p.name,
          p.slug,
          p.city,
          p.country,
          p.timezone,
          p."continentSlug",
          p."countrySlug",
          p."citySlug",
          ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY lu."waitTime")::numeric) as avg_wait,
          COUNT(*) as active_rides,
          COALESCE(ac.total, 0) as total_attractions,
          COALESCE(ac.closed, 0) as explicitly_closed_attractions
        FROM latest_updates lu
        JOIN parks p ON p.id = lu."parkId"
        LEFT JOIN attraction_counts ac ON ac."parkId" = p.id
        WHERE lu.status = 'OPERATING'
        GROUP BY p.id, p.name, p.slug, p.city, p.country, p.timezone, p."continentSlug", p."countrySlug", p."citySlug", ac.total, ac.closed
      )
      SELECT * FROM park_stats
    `);

    // Count open parks (those with > 0 active rides)
    const openParks = activeParksResult;

    // Parallel count queries for all entities (Optimized with caching)
    const [
      totalParksCount,
      totalAttractionsCount,
      totalShowsCount,
      totalRestaurantsCount,
      queueDataCount,
    ] = await Promise.all([
      this.getCachedCount(this.parkRepository, "count:parks"),
      this.getCachedCount(this.attractionRepository, "count:attractions"),
      this.getCachedCount(this.showRepository, "count:shows"),
      this.getCachedCount(this.restaurantRepository, "count:restaurants"),
      this.getCachedCount(this.queueDataRepository, "count:queue_data"),
    ]);

    const openParksCount = openParks.length;

    // 2. Find Most/Least Crowded Park (by Avg Wait)
    openParks.sort(
      (a: { avg_wait: number }, b: { avg_wait: number }) =>
        b.avg_wait - a.avg_wait,
    );

    const mostCrowdedParkRow = openParks.length > 0 ? openParks[0] : null;
    const leastCrowdedParkRow =
      openParks.length > 0 ? openParks[openParks.length - 1] : null;

    // 3. Find Longest/Shortest Wait Ride (Global)
    // IMPORTANT: Only consider rides from parks that are currently OPERATING
    // to avoid showing rides from closed parks (e.g., stale data)
    const rideStats = await this.queueDataRepository.query(`
      WITH schedule_open_parks AS (
        SELECT DISTINCT s."parkId"
        FROM schedule_entries s
        WHERE s."scheduleType" = 'OPERATING'
          AND s."openingTime" <= NOW()
          AND s."closingTime" > NOW()
      ),
      ride_open_parks AS (
        SELECT a."parkId"
        FROM attractions a
        JOIN queue_data qd ON qd."attractionId" = a.id
          AND qd.timestamp > NOW() - INTERVAL '2 hours'
          AND qd."waitTime" IS NOT NULL
        WHERE NOT EXISTS (
          SELECT 1 FROM schedule_entries se WHERE se."parkId" = a."parkId" AND se."scheduleType" = 'OPERATING'
        )
        AND NOT EXISTS (
          SELECT 1 FROM schedule_entries se WHERE se."parkId" = a."parkId" AND se."scheduleType" = 'CLOSED' AND se.date = CURRENT_DATE
        )
        GROUP BY a."parkId"
        HAVING COUNT(*) >= 3
          AND 100.0 * COUNT(CASE WHEN qd."waitTime" >= 10 THEN 1 END) / COUNT(*) >= 25
      ),
      park_status AS (
        SELECT "parkId" FROM schedule_open_parks
        UNION
        SELECT "parkId" FROM ride_open_parks
      ),
      latest_rides AS (
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd."waitTime",
          qd."status",
          a.name as "attractionName",
          a.slug as "attractionSlug",
          p.id as "parkId",
          p.name as "parkName",
          p.slug,
          p.timezone,
          p.city,
          p.country,
          p."continentSlug",
          p."countrySlug",
          p."citySlug"
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        JOIN parks p ON p.id = a."parkId"
        JOIN park_status ps ON ps."parkId" = p.id
        WHERE qd.timestamp > NOW() - INTERVAL '24 hours'
          AND qd."waitTime" >= 10
          AND qd."waitTime" <= 360
        ORDER BY qd."attractionId", qd.timestamp DESC
      )
      SELECT *
      FROM latest_rides
      WHERE status = 'OPERATING'
    `);

    // Sort in JS
    rideStats.sort(
      (a: { waitTime: number }, b: { waitTime: number }) =>
        b.waitTime - a.waitTime,
    );

    const longestRaw = rideStats.length > 0 ? rideStats[0] : null;
    const shortestRaw =
      rideStats.length > 0 ? rideStats[rideStats.length - 1] : null;

    // 4. Enrich all four items in one parallel round-trip.
    const [
      mostCrowdedOccupancy,
      leastCrowdedOccupancy,
      longestRideRating,
      shortestRideRating,
      sparklineMap,
      longestStats,
      shortestStats,
    ] = await Promise.all([
      mostCrowdedParkRow
        ? this.calculateParkOccupancy(mostCrowdedParkRow.id)
        : Promise.resolve(null),
      leastCrowdedParkRow
        ? this.calculateParkOccupancy(leastCrowdedParkRow.id)
        : Promise.resolve(null),
      longestRaw
        ? this.getBaselineForAttraction(longestRaw.attractionId).then(
            (baseline) => this.getLoadRating(longestRaw.waitTime, baseline),
          )
        : Promise.resolve(null),
      shortestRaw
        ? this.getBaselineForAttraction(shortestRaw.attractionId).then(
            (baseline) => this.getLoadRating(shortestRaw.waitTime, baseline),
          )
        : Promise.resolve(null),
      this.getAttractionSparklinesBatch(
        [longestRaw, shortestRaw]
          .filter((r): r is NonNullable<typeof r> => r !== null)
          .map((r) => ({
            id: r.attractionId,
            parkId: r.parkId,
            timezone: r.timezone,
          })),
      ),
      longestRaw
        ? this.getEffectiveStartTime(
            longestRaw.parkId,
            longestRaw.timezone,
          ).then((startTime) =>
            this.getAttractionStatistics(
              longestRaw.attractionId,
              startTime,
              longestRaw.timezone,
            ),
          )
        : Promise.resolve(null),
      shortestRaw
        ? this.getEffectiveStartTime(
            shortestRaw.parkId,
            shortestRaw.timezone,
          ).then((startTime) =>
            this.getAttractionStatistics(
              shortestRaw.attractionId,
              startTime,
              shortestRaw.timezone,
            ),
          )
        : Promise.resolve(null),
    ]);

    const mostCrowdedParkDetails = mostCrowdedParkRow
      ? this.buildParkStatsItem(
          mostCrowdedParkRow,
          mostCrowdedOccupancy
            ? this.determineCrowdLevel(mostCrowdedOccupancy.current)
            : null,
        )
      : null;

    const leastCrowdedParkDetails = leastCrowdedParkRow
      ? this.buildParkStatsItem(
          leastCrowdedParkRow,
          leastCrowdedOccupancy
            ? this.determineCrowdLevel(leastCrowdedOccupancy.current)
            : null,
        )
      : null;

    const longestWaitRideDetails = longestRaw
      ? this.buildAttractionStatsItem(longestRaw, {
          crowdLevel: longestRideRating?.rating ?? null,
          sparkline: sparklineMap.get(longestRaw.attractionId) ?? [],
          stats: longestStats,
        })
      : null;

    const shortestWaitRideDetails = shortestRaw
      ? this.buildAttractionStatsItem(shortestRaw, {
          crowdLevel: shortestRideRating?.rating ?? null,
          sparkline: sparklineMap.get(shortestRaw.attractionId) ?? [],
          stats: shortestStats,
        })
      : null;

    // Count open vs closed attractions
    const openAttractionsCount = await this.attractionRepository.query(`
      SELECT COUNT(DISTINCT a.id) as count
      FROM attractions a
      JOIN LATERAL (
        SELECT qd.status
        FROM queue_data qd
        WHERE qd."attractionId" = a.id
          AND qd.timestamp > NOW() - INTERVAL '24 hours'
        ORDER BY timestamp DESC
        LIMIT 1
      ) latest_status ON true
      WHERE latest_status.status = 'OPERATING'
    `);

    const openAttractions = parseInt(openAttractionsCount[0]?.count || "0");

    const response: GlobalStatsDto = {
      counts: {
        openParks: openParksCount,
        parks: totalParksCount,
        openAttractions,
        attractions: totalAttractionsCount,
        shows: totalShowsCount,
        restaurants: totalRestaurantsCount,
        queueDataRecords: queueDataCount,
        totalWaitTime: rideStats.reduce(
          (sum: number, stat: { waitTime?: number }) =>
            sum + (stat.waitTime || 0),
          0,
        ),
      },
      mostCrowdedPark: mostCrowdedParkDetails,
      leastCrowdedPark: leastCrowdedParkDetails,
      longestWaitRide: longestWaitRideDetails,
      shortestWaitRide: shortestWaitRideDetails,
    };

    // Cache the result ONLY if we have data (prevent caching zero states)
    if (response.counts.parks > 0 || response.counts.attractions > 0) {
      await this.redis.set(
        cacheKey,
        JSON.stringify(response),
        "EX",
        this.TTL_GLOBAL_STATS, // 5 minutes for real-time data
      );
    }

    return response;
  }

  /**
   * GET /v1/analytics/ticker
   * Top 40 OPERATING attractions by wait time across all open parks.
   * Cached 120 s in Redis (matches Cache-Control header).
   */
  async getTickerData(): Promise<{ items: object[]; generatedAt: string }> {
    const cacheKey = "analytics:ticker:v1";
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      return JSON.parse(cached);
    }

    const rows: Array<{
      attractionId: string;
      waitTime: number;
      historicalWaitTime: number | null;
      attractionName: string;
      attractionSlug: string;
      parkName: string;
      parkSlug: string;
      continent: string;
      continentSlug: string | null;
      country: string;
      countrySlug: string | null;
      city: string;
      citySlug: string | null;
      p50Baseline: string;
      isHeadliner: boolean;
    }> = await this.queueDataRepository.query(`
      WITH schedule_open_parks AS (
        SELECT DISTINCT s."parkId"
        FROM schedule_entries s
        WHERE s."scheduleType" = 'OPERATING'
          AND s."openingTime" <= NOW()
          AND s."closingTime" > NOW()
      ),
      ride_open_parks AS (
        SELECT a."parkId"
        FROM attractions a
        JOIN queue_data qd ON qd."attractionId" = a.id
          AND qd.timestamp > NOW() - INTERVAL '2 hours'
          AND qd."waitTime" IS NOT NULL
        WHERE NOT EXISTS (
          SELECT 1 FROM schedule_entries se
          WHERE se."parkId" = a."parkId" AND se."scheduleType" = 'OPERATING'
        )
        AND NOT EXISTS (
          SELECT 1 FROM schedule_entries se
          WHERE se."parkId" = a."parkId"
            AND se."scheduleType" = 'CLOSED'
            AND se.date = CURRENT_DATE
        )
        GROUP BY a."parkId"
        HAVING COUNT(*) >= 3
          AND 100.0 * COUNT(CASE WHEN qd."waitTime" >= 10 THEN 1 END) / COUNT(*) >= 25
      ),
      park_status AS (
        SELECT "parkId" FROM schedule_open_parks
        UNION
        SELECT "parkId" FROM ride_open_parks
      ),
      historical_rides AS (
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd."waitTime" AS "historicalWaitTime"
        FROM queue_data qd
        WHERE qd.timestamp BETWEEN NOW() - INTERVAL '45 minutes' AND NOW() - INTERVAL '15 minutes'
          AND qd."queueType" = 'STANDBY'
          AND qd."waitTime" IS NOT NULL
        ORDER BY qd."attractionId", qd.timestamp DESC
      ),
      latest_rides AS (
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd."waitTime",
          qd."status",
          hr."historicalWaitTime",
          a.name                                    AS "attractionName",
          a.slug                                    AS "attractionSlug",
          p.name                                    AS "parkName",
          p.slug                                    AS "parkSlug",
          p.continent,
          p."continentSlug",
          p.country,
          p."countrySlug",
          p.city,
          p."citySlug",
          COALESCE(apb."p50Baseline", 0)::float     AS "p50Baseline",
          COALESCE(apb."isHeadliner", false)        AS "isHeadliner"
        FROM queue_data qd
        JOIN attractions a  ON a.id = qd."attractionId"
        JOIN parks p        ON p.id = a."parkId"
        JOIN park_status ps ON ps."parkId" = p.id
        LEFT JOIN attraction_p50_baselines apb ON apb."attractionId" = qd."attractionId"
        LEFT JOIN historical_rides hr ON hr."attractionId" = qd."attractionId"
        WHERE qd.timestamp > NOW() - INTERVAL '24 hours'
          AND qd."queueType" = 'STANDBY'
        ORDER BY qd."attractionId", qd.timestamp DESC
      )
      -- Priority buckets: 1=popular+full, 2=full, 3=popular+empty, 4=rest
      -- "full" = waitTime >= p50 baseline (fallback threshold: 20 min when baseline unknown)
      -- One entry per park (best by priority then waitTime) for ticker diversity
      SELECT *
      FROM (
        SELECT *,
          CASE
            WHEN "isHeadliner" = true
                 AND "waitTime" >= GREATEST("p50Baseline", 20) THEN 1
            WHEN "isHeadliner" = false
                 AND "waitTime" >= GREATEST("p50Baseline", 20) THEN 2
            WHEN "isHeadliner" = true                          THEN 3
            ELSE                                                    4
          END AS "sortPriority",
          ROW_NUMBER() OVER (
            PARTITION BY "parkSlug"
            ORDER BY
              CASE
                WHEN "isHeadliner" = true
                     AND "waitTime" >= GREATEST("p50Baseline", 20) THEN 1
                WHEN "isHeadliner" = false
                     AND "waitTime" >= GREATEST("p50Baseline", 20) THEN 2
                WHEN "isHeadliner" = true                          THEN 3
                ELSE                                                    4
              END ASC,
              "waitTime" DESC
          ) AS "parkRank"
        FROM latest_rides
        WHERE status = 'OPERATING'
          AND "waitTime" > 0
      ) ranked
      WHERE "parkRank" = 1
      ORDER BY "sortPriority" ASC, "waitTime" DESC
      LIMIT 40
    `);

    const items = rows.map((row) => {
      const current = Number(row.waitTime);
      const historical =
        row.historicalWaitTime != null ? Number(row.historicalWaitTime) : null;
      let trend: "rising" | "falling" | "stable" | null = null;
      if (historical !== null) {
        const delta = current - historical;
        trend = delta >= 5 ? "rising" : delta <= -5 ? "falling" : "stable";
      }
      return {
        parkName: row.parkName,
        parkSlug: row.parkSlug,
        continent: row.continent,
        continentSlug: row.continentSlug,
        country: row.country,
        countrySlug: row.countrySlug,
        city: row.city,
        citySlug: row.citySlug,
        attractionName: row.attractionName,
        attractionSlug: row.attractionSlug,
        waitTime: current,
        trend,
        crowdLevel: this.getAttractionCrowdLevel(
          current,
          parseFloat(row.p50Baseline),
        ),
        url: buildAttractionUrl(
          {
            continentSlug: row.continentSlug,
            countrySlug: row.countrySlug,
            citySlug: row.citySlug,
            slug: row.parkSlug,
          },
          { slug: row.attractionSlug },
        ),
      };
    });

    const response = { items, generatedAt: new Date().toISOString() };

    await this.redis.set(cacheKey, JSON.stringify(response), "EX", 300);

    return response;
  }

  /**
   * Get live geographic statistics for all continents/countries/cities
   * Cached for 5 minutes
   */
  async getGeoLiveStats() {
    const cacheKey = "analytics:geo_live_stats:v2"; // v2: removed cities and averageWaitTime
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      return JSON.parse(cached);
    }

    // Get all parks with their current status and wait times
    const parksData = await this.queueDataRepository.query(`
      WITH schedule_open_parks AS (
        SELECT DISTINCT s."parkId"
        FROM schedule_entries s
        WHERE s."scheduleType" = 'OPERATING'
          AND s."openingTime" <= NOW()
          AND s."closingTime" > NOW()
      ),
      ride_open_parks AS (
        SELECT a."parkId"
        FROM attractions a
        JOIN queue_data qd ON qd."attractionId" = a.id
          AND qd.timestamp > NOW() - INTERVAL '2 hours'
          AND qd."waitTime" IS NOT NULL
        WHERE NOT EXISTS (
          SELECT 1 FROM schedule_entries se WHERE se."parkId" = a."parkId" AND se."scheduleType" = 'OPERATING'
        )
        AND NOT EXISTS (
          SELECT 1 FROM schedule_entries se WHERE se."parkId" = a."parkId" AND se."scheduleType" = 'CLOSED' AND se.date = CURRENT_DATE
        )
        GROUP BY a."parkId"
        HAVING COUNT(*) >= 3
          AND 100.0 * COUNT(CASE WHEN qd."waitTime" >= 10 THEN 1 END) / COUNT(*) >= 25
      ),
      park_status AS (
        SELECT "parkId" FROM schedule_open_parks
        UNION
        SELECT "parkId" FROM ride_open_parks
      ),
      latest_updates AS (
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd."waitTime",
          a."parkId",
          qd."status",
          qd.timestamp
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        JOIN park_status ps ON ps."parkId" = a."parkId"
        WHERE qd.timestamp > NOW() - INTERVAL '24 hours'
          AND qd."queueType" = 'STANDBY'
        ORDER BY qd."attractionId", qd.timestamp DESC
      ),
      park_stats AS (
        SELECT
          p.id as park_id,
          p."continentSlug",
          p."countrySlug",
          p."citySlug",
          AVG(lu."waitTime") as avg_wait
        FROM latest_updates lu
        JOIN parks p ON p.id = lu."parkId"
        WHERE lu.status = 'OPERATING'
        GROUP BY p.id, p."continentSlug", p."countrySlug", p."citySlug"
      )
      SELECT * FROM park_stats
    `);

    // Aggregate by continent > country > city
    const continentMap = new Map<
      string,
      {
        openParkCount: number;
        totalWaitTime: number;
        parkCount: number;
        countries: Map<
          string,
          {
            openParkCount: number;
            totalWaitTime: number;
            parkCount: number;
            cities: Map<
              string,
              {
                openParkCount: number;
                totalWaitTime: number;
                parkCount: number;
              }
            >;
          }
        >;
      }
    >();

    for (const park of parksData) {
      const continentSlug = park.continentSlug;
      const countrySlug = park.countrySlug;
      const citySlug = park.citySlug;

      // Initialize continent
      if (!continentMap.has(continentSlug)) {
        continentMap.set(continentSlug, {
          openParkCount: 0,
          totalWaitTime: 0,
          parkCount: 0,
          countries: new Map(),
        });
      }

      const continent = continentMap.get(continentSlug)!;
      continent.openParkCount++;
      continent.totalWaitTime += parseFloat(park.avg_wait || "0");
      continent.parkCount++;

      // Initialize country
      if (!continent.countries.has(countrySlug)) {
        continent.countries.set(countrySlug, {
          openParkCount: 0,
          totalWaitTime: 0,
          parkCount: 0,
          cities: new Map(),
        });
      }

      const country = continent.countries.get(countrySlug)!;
      country.openParkCount++;
      country.totalWaitTime += parseFloat(park.avg_wait || "0");
      country.parkCount++;

      // Initialize city
      if (!country.cities.has(citySlug)) {
        country.cities.set(citySlug, {
          openParkCount: 0,
          totalWaitTime: 0,
          parkCount: 0,
        });
      }

      const city = country.cities.get(citySlug)!;
      city.openParkCount++;
      city.totalWaitTime += parseFloat(park.avg_wait || "0");
      city.parkCount++;
    }

    // Build response structure
    const continents = [];
    for (const [continentSlug, continentData] of continentMap) {
      const countries = [];
      for (const [countrySlug, countryData] of continentData.countries) {
        countries.push({
          slug: countrySlug,
          openParkCount: countryData.openParkCount,
        });
      }

      continents.push({
        slug: continentSlug,
        openParkCount: continentData.openParkCount,
        countries,
      });
    }

    const response = { continents };

    // Cache for 5 minutes
    await this.redis.set(
      cacheKey,
      JSON.stringify(response),
      "EX",
      this.TTL_REALTIME,
    );

    return response;
  }

  /**
   * Get cached count for a table to improve performance
   */
  private async getCachedCount(
    repository: Repository<any>,
    cacheKey: string,
    ttl: number = 3600,
  ): Promise<number> {
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      return parseInt(cached, 10);
    }

    try {
      const count = await repository.count();
      await this.redis.set(cacheKey, count.toString(), "EX", ttl);
      return count;
    } catch (err) {
      this.logger.warn(`Failed to count for ${cacheKey}`, err);
      return 0;
    }
  }

  /**
   * Map an attraction's current wait time to a crowd level rating against
   * its median baseline (P50). The baseline argument should be the 548-day
   * per-attraction P50 — what a typical wait looks like at a typical
   * moment — so the resulting percentage reads as "current peak vs typical
   * median". 100% = current peak matches the typical median wait,
   * 150%+ = elevated (above-typical day), 200%+ = extreme. Callers should
   * pass getAttractionP50BaselineFromCache, falling back to P90 only when
   * no P50 row exists yet (brand-new attraction before the next cron).
   */
  public getAttractionCrowdLevel(
    waitTime: number | undefined,
    baseline: number | undefined,
  ): CrowdLevel | null {
    if (!waitTime || waitTime === 0) return null;
    if (baseline && baseline > 0) {
      const occupancy = (waitTime / baseline) * 100;
      return this.determineCrowdLevel(occupancy);
    }
    return null;
  }

  /**
   * Public utility: Get park crowd level from occupancy percentage
   * Single source of truth for park crowd level calculation
   */
  public getParkCrowdLevel(occupancy: number): CrowdLevel {
    return this.determineCrowdLevel(occupancy);
  }

  /**
   * Calculate crowd level for a specific date (historical or today)
   *
   * **Unified method for both parks and attractions.**
   * This is the single source of truth for calculating crowd levels
   * for any date from the first known data point to today.
   *
   * **Cache Strategy:**
   * - Today's data: 30 minutes (dynamic, frequently updated)
   * - Historical data: 24 hours (stable)
   *
   * @param entityId - Park or attraction ID
   * @param type - "park" or "attraction"
   * @param date - Date to calculate for (in park timezone, YYYY-MM-DD format)
   * @param timezone - Park timezone
   * @returns Crowd level result with percentage, level, confidence, and metadata
   */
  async calculateCrowdLevelForDate(
    entityId: string,
    type: "park" | "attraction",
    date: string,
    timezone: string,
  ): Promise<{
    percentage: number;
    crowdLevel: CrowdLevel;
    peakCrowdLevel: CrowdLevel;
    hasData: boolean;
    confidence: "high" | "medium" | "low";
    avgWaitTime: number | null;
    p90WaitTime: number | null;
    p90Baseline: number;
    actualP90Baseline: number;
    baselineType: string;
    sampleCount: number;
    isToday: boolean;
  }> {
    // Determine if this is today's data
    const todayStr = getCurrentDateInTimezone(timezone);
    const isToday = date === todayStr;

    // Cache key varies by entity and date
    const cacheKey = `analytics:crowdlevel:${type}:${entityId}:${date}`;
    const cacheTTL = isToday ? 30 * 60 : 6 * 60 * 60; // 30 min for today, 6h for historical (reduced from 24h)

    // Try cache first (but skip for today to ensure freshness within 30 min window)
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      try {
        return JSON.parse(cached);
      } catch {
        // Invalid cache, continue
      }
    }

    // Resolve baseline. For parks the calendar reads "a day's peak ÷ a
    // typical day's peak": numerator = AVG-across-headliners of the per-ride
    // day-P90, denominator = the typical-day-peak baseline (median over days
    // of that same quantity). 100% = a typical day = moderate; busy seasons
    // (Wintertraum, Easter) reach very_high/extreme. The pooled P90 baseline
    // is NOT used here because it's inflated by the busiest season and
    // compresses the top. No P90/P50 fallback: the typical-day-peak is
    // written atomically with P50/P90, so a missing value means the park has
    // no baseline at all (brand-new) → the no-baseline default below applies.
    let baseline = 0;
    let baselineType: "typical_day" | "p90" | "p50" = "typical_day";
    let baselineConfidence: "high" | "medium" | "low" = "low";

    if (type === "park") {
      baseline = await this.getTypicalDayPeakFromCache(entityId);
      if (baseline > 0) {
        const rec = await this.parkP50BaselineRepository.findOne({
          where: { parkId: entityId },
          select: ["confidence"],
        });
        baselineConfidence = rec?.confidence || "low";
      }
    } else {
      baseline = await this.getAttractionP90BaselineFromCache(entityId);
      if (baseline > 0) {
        const rec = await this.attractionP90BaselineRepository.findOne({
          where: { attractionId: entityId },
        });
        baselineConfidence = rec?.confidence || "low";
      } else {
        baseline = await this.getAttractionP50BaselineFromCache(entityId);
        baselineType = "p50";
        if (baseline > 0) {
          const rec = await this.attractionP50BaselineRepository.findOne({
            where: { attractionId: entityId },
          });
          baselineConfidence = rec?.confidence || "low";
        }
      }
    }

    // Calculate date range for the specific day.
    // When a schedule is available, trim 5 minutes from both ends so that
    // pre-opening ride tests and post-closing stragglers don't deflate the P50.
    const SCHEDULE_TRIM_MS = 5 * 60 * 1000;
    let startOfDay = fromZonedTime(`${date}T00:00:00`, timezone);
    let endOfDay = fromZonedTime(`${date}T23:59:59`, timezone);

    if (type === "park") {
      const daySchedule = await this.scheduleEntryRepository.findOne({
        where: {
          parkId: entityId,
          date: date as any,
          scheduleType: ScheduleType.OPERATING,
        },
        order: { openingTime: "ASC" },
      });
      if (daySchedule?.openingTime) {
        startOfDay = new Date(
          daySchedule.openingTime.getTime() + SCHEDULE_TRIM_MS,
        );
      }
      if (daySchedule?.closingTime) {
        endOfDay = new Date(
          daySchedule.closingTime.getTime() - SCHEDULE_TRIM_MS,
        );
      }
    }

    // Query average (P50) and peak (P90) wait times for the day
    let dailyStats: { p50: number | null; p90: number | null; count: number };

    if (type === "attraction") {
      const result = await this.queueDataRepository.query(
        `
        SELECT
          ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime")::numeric, 2) as p50,
          ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime")::numeric, 2) as p90,
          COUNT(*) as count
        FROM queue_data qd
        WHERE qd."attractionId" = $1::uuid
          AND qd.timestamp >= $2
          AND qd.timestamp <= $3
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
          AND qd."waitTime" >= 10
          AND qd."queueType" = 'STANDBY'
        `,
        [entityId, startOfDay, endOfDay],
      );

      dailyStats = {
        p50: result[0]?.p50 ? parseFloat(result[0].p50) : null,
        p90: result[0]?.p90 ? parseFloat(result[0].p90) : null,
        count: parseInt(result[0]?.count || "0", 10),
      };
    } else {
      // For parks, use headliner attractions to match live crowd level and baseline calculations
      const headliners = await this.headlinerAttractionRepository.find({
        where: { parkId: entityId },
        select: ["parkId", "attractionId"],
      });
      let targetAttractionIds = headliners.map((h) => h.attractionId);

      // Fallback: if no headliners defined, use all attractions for the park
      if (targetAttractionIds.length === 0) {
        const allAttractions = await this.attractionRepository.find({
          where: { parkId: entityId },
          select: ["id"],
        });
        targetAttractionIds = allAttractions.map((a) => a.id);
      }

      if (targetAttractionIds.length > 0) {
        // For each ride: compute its own P50 / P90 over the day. Then
        // aggregate AVG across rides for both — every headliner counts
        // equally, exactly like the P50/P90 park baselines
        // (avg-of-per-headliner-percentile). This keeps the day value
        // apples-to-apples with the baseline it's divided by:
        // - AVG(p50) — "typical wait that day across headliners" (avgWaitTime).
        // - AVG(p90) — the day's peak across headliners; ÷ the P90 baseline
        //   gives peak-vs-peak (100% = a typical day's peak).
        const result = await this.queueDataRepository.query(
          `
          SELECT
            ROUND(AVG(per_ride.p50)::numeric, 2)   as p50,
            ROUND(AVG(per_ride.p90)::numeric, 2)   as p90,
            SUM(per_ride.cnt)::integer              as count
          FROM (
            SELECT
              qd."attractionId",
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") as p50,
              PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") as p90,
              COUNT(*) as cnt
            FROM queue_data qd
            WHERE qd."attractionId" = ANY($1::uuid[])
              AND qd.timestamp >= $2
              AND qd.timestamp <= $3
              AND qd.status = 'OPERATING'
              AND qd."waitTime" IS NOT NULL
              AND qd."waitTime" >= 10
              AND qd."queueType" = 'STANDBY'
            GROUP BY qd."attractionId"
          ) per_ride
          `,
          [targetAttractionIds, startOfDay, endOfDay],
        );

        dailyStats = {
          p50: result[0]?.p50 ? parseFloat(result[0].p50) : null,
          p90: result[0]?.p90 ? parseFloat(result[0].p90) : null,
          count: parseInt(result[0]?.count || "0", 10),
        };
      } else {
        dailyStats = { p50: null, p90: null, count: 0 };
      }
    }

    // crowdLevel: the day's peak ÷ the typical day's peak. 100% = this
    // day's peak matched a typical day's peak, >150% = noticeably busier
    // than typical. The numerator is the day's AVG-of-per-headliner-P90
    // for both the typical-day-peak and the P90-baseline fallback; only the
    // brand-new P50 fallback uses the P50 (median) numerator so that
    // comparison stays apples-to-apples (median-vs-median).
    let percentage = 0;
    let crowdLevel: CrowdLevel = "very_low";
    const hasData = dailyStats.p50 !== null && dailyStats.count > 0;
    const dailyValue = baselineType === "p50" ? dailyStats.p50 : dailyStats.p90;

    if (hasData && baseline > 0 && dailyValue !== null) {
      percentage = Math.round((dailyValue / baseline) * 100);
      crowdLevel = this.determineCrowdLevel(percentage);
    } else if (hasData) {
      crowdLevel = "moderate";
      percentage = 100;
    }

    // peakCrowdLevel kept in the response shape for backwards compatibility
    // with any caller that still reads it (it used to mean "P90-based"
    // crowd; now crowdLevel itself is P90-based so they're identical when
    // the P90 baseline is present).
    const response = {
      percentage,
      crowdLevel,
      peakCrowdLevel: crowdLevel,
      hasData,
      confidence: baselineConfidence,
      avgWaitTime: dailyStats.p50
        ? roundToNearest5Minutes(dailyStats.p50)
        : null,
      p90WaitTime: dailyStats.p90
        ? roundToNearest5Minutes(dailyStats.p90)
        : null,
      p90Baseline: baseline,
      actualP90Baseline: baseline,
      baselineType,
      sampleCount: dailyStats.count,
      isToday,
    };

    // Cache the result
    await this.redis.set(cacheKey, JSON.stringify(response), "EX", cacheTTL);

    return response;
  }

  /**
   * Get crowd level data for ML training
   *
   * Returns historical data with labels for model training.
   * Exports data in a format suitable for the ML service.
   *
   * @param entityId - Park or attraction ID
   * @param type - "park" or "attraction"
   * @param fromDate - Start date (YYYY-MM-DD)
   * @param toDate - End date (YYYY-MM-DD)
   * @param timezone - Park timezone
   * @returns Array of daily crowd level data for ML training
   */
  async getCrowdLevelTrainingData(
    entityId: string,
    type: "park" | "attraction",
    fromDate: string,
    toDate: string,
    timezone: string,
  ): Promise<
    Array<{
      date: string;
      dayOfWeek: number;
      avgWaitTime: number;
      p90Baseline: number;
      percentage: number;
      crowdLevel: CrowdLevel;
      confidence: "high" | "medium" | "low";
    }>
  > {
    // ML training labels follow the same peak-vs-median semantic as the
    // user-facing crowd level: P50 baseline. The model predicts wait
    // times directly; the crowd-level baseline here only affects the
    // labelled percentage exposed for evaluation.
    let baseline = 0;
    let confidence: "high" | "medium" | "low" = "low";
    if (type === "park") {
      baseline = await this.getP50BaselineFromCache(entityId);
      if (baseline > 0) {
        const rec = await this.parkP50BaselineRepository.findOne({
          where: { parkId: entityId },
        });
        confidence = rec?.confidence || "low";
      }
    } else {
      baseline = await this.getAttractionP50BaselineFromCache(entityId);
      if (baseline > 0) {
        const rec = await this.attractionP50BaselineRepository.findOne({
          where: { attractionId: entityId },
        });
        confidence = rec?.confidence || "low";
      }
    }

    // Query daily aggregates
    const startDate = fromZonedTime(`${fromDate}T00:00:00`, timezone);
    const endDate = fromZonedTime(`${toDate}T23:59:59`, timezone);

    // Each day's "value" is its P90 (peak-of-day) so the % stays
    // apples-to-apples with the P90 baseline. `avgWaitTime` in the
    // response is still the P50 (kept under that name for backwards
    // compatibility with downstream ML feature consumers); the model
    // sees both the label percentage AND the underlying wait stats.
    let dailyData: Array<{
      date: string;
      avgWait: number;
      peakWait: number;
      dayOfWeek: number;
    }>;

    if (type === "attraction") {
      dailyData = await this.queueDataRepository.query(
        `
        SELECT
          DATE(qd.timestamp AT TIME ZONE $2) as date,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") as "avgWait",
          PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") as "peakWait",
          EXTRACT(DOW FROM qd.timestamp AT TIME ZONE $2) as "dayOfWeek"
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        LEFT JOIN schedule_entries se
          ON se."parkId" = a."parkId"
          AND se.date = DATE(qd.timestamp AT TIME ZONE $2)
          AND se."attractionId" IS NULL
        WHERE qd."attractionId" = $1::uuid
          AND qd.timestamp >= $3
          AND qd.timestamp <= $4
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
          AND qd."waitTime" >= 10
          AND qd."queueType" = 'STANDBY'
          AND (se.id IS NULL OR se."scheduleType" IN ('OPERATING', 'UNKNOWN'))
        GROUP BY DATE(qd.timestamp AT TIME ZONE $2),
                 EXTRACT(DOW FROM qd.timestamp AT TIME ZONE $2)
        ORDER BY date
      `,
        [entityId, timezone, startDate, endDate],
      );
    } else {
      dailyData = await this.queueDataRepository.query(
        `
        SELECT
          DATE(qd.timestamp AT TIME ZONE $2) as date,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime") as "avgWait",
          PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") as "peakWait",
          EXTRACT(DOW FROM qd.timestamp AT TIME ZONE $2) as "dayOfWeek"
        FROM queue_data qd
        INNER JOIN attractions a ON qd."attractionId" = a.id
        LEFT JOIN schedule_entries se
          ON se."parkId" = a."parkId"
          AND se.date = DATE(qd.timestamp AT TIME ZONE $2)
          AND se."attractionId" IS NULL
        WHERE a."parkId" = $1::uuid
          AND qd.timestamp >= $3
          AND qd.timestamp <= $4
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
          AND qd."waitTime" >= 10
          AND qd."queueType" = 'STANDBY'
          AND (se.id IS NULL OR se."scheduleType" IN ('OPERATING', 'UNKNOWN'))
        GROUP BY DATE(qd.timestamp AT TIME ZONE $2),
                 EXTRACT(DOW FROM qd.timestamp AT TIME ZONE $2)
        ORDER BY date
      `,
        [entityId, timezone, startDate, endDate],
      );
    }

    return dailyData.map((row) => {
      const avgWait = parseFloat(String(row.avgWait));
      const peakWait = parseFloat(String(row.peakWait || row.avgWait));
      const percentage =
        baseline > 0 ? Math.round((peakWait / baseline) * 100) : 50;

      return {
        date:
          typeof row.date === "string"
            ? row.date
            : new Date(row.date).toISOString().split("T")[0],
        dayOfWeek: parseInt(String(row.dayOfWeek), 10),
        avgWaitTime: roundToNearest5Minutes(avgWait),
        p90Baseline: baseline,
        percentage,
        crowdLevel: this.determineCrowdLevel(percentage),
        confidence,
      };
    });
  }

  // ==================================================================================
  // P50 BASELINE SYSTEM - HEADLINER IDENTIFICATION & CROWD LEVEL CALCULATION
  // ==================================================================================

  /**
   * Check if a park has any queue_data (STANDBY, OPERATING, waitTime>0) in the given window.
   * Use to skip P50 baseline calculation for parks with no historical data and reduce log noise.
   *
   * @param parkId - Park ID
   * @param windowDays - Number of days to look back (default 548)
   * @returns true if at least one row exists in the window
   */
  async parkHasQueueDataInWindow(
    parkId: string,
    windowDays: number = 548,
  ): Promise<boolean> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["timezone"],
    });
    const timezone = park?.timezone || "UTC";
    const now = new Date();
    const todayStr = formatInTimeZone(now, timezone, "yyyy-MM-dd");
    const today = fromZonedTime(`${todayStr}T00:00:00`, timezone);
    const cutoff = subDays(today, windowDays);

    const rows = await this.queueDataRepository.query(
      `
      SELECT 1
      FROM queue_data qd
      INNER JOIN attractions a ON qd."attractionId" = a.id
      WHERE a."parkId" = $1::uuid
        AND qd.timestamp >= $2
        AND qd."queueType" = 'STANDBY'
        AND qd.status = 'OPERATING'
        AND qd."waitTime" > 0
      LIMIT 1
      `,
      [parkId, cutoff],
    );
    return Array.isArray(rows) && rows.length > 0;
  }

  /**
   * Identify headliner attractions for a park using 3-tier adaptive strategy
   *
   * Tier 1 (Major Parks): Absolute thresholds (AVG > 15min, P90 > 25min)
   * Tier 2 (Medium Parks): Relative thresholds (Top 40%, P90 > 1.5x P50)
   * Tier 3 (Small Parks): Relative thresholds (Top 50% - median wait) (fallback)
   *
   * @param parkId - Park ID
   * @returns Array of headliner attractions with tier classification
   */
  async identifyHeadliners(parkId: string): Promise<HeadlinerAttraction[]> {
    const SLIDING_WINDOW_DAYS = 548; // 1.5 years

    // Get park timezone
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["timezone"],
    });
    const timezone = park?.timezone || "UTC";

    // Calculate cutoff date
    const now = new Date();
    const todayStr = formatInTimeZone(now, timezone, "yyyy-MM-dd");
    const today = fromZonedTime(`${todayStr}T00:00:00`, timezone);
    const cutoff = subDays(today, SLIDING_WINDOW_DAYS);

    // 3-Tier Adaptive Headliner Identification
    const result = await this.queueDataRepository.query(
      `
      -- Step 1: Calculate statistics for all attractions
      -- waitTime >= 10 excludes the "1-minute walk-on placeholder" common in water park
      -- APIs (where waitTime=1 means "open, no real queue"). This aligns with the
      -- getCurrentSpotWaitTime minWaitTime=5 default so baseline and current use the same data.
      -- Schedule JOIN: exclude days where the park is explicitly scheduled as non-OPERATING.
      -- If no schedule entry exists for a day, the day is included (unknown = include by default).
      WITH attraction_stats AS (
        SELECT
          a.id as attraction_id,
          a."parkId" as park_id,
          ROUND(AVG(qd."waitTime")::numeric, 2) as avg_wait,
          ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime")::numeric, 2) as p50_wait,
          ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime")::numeric, 2) as p90_wait,
          COUNT(DISTINCT DATE(qd.timestamp AT TIME ZONE $2)) as operating_days,
          COUNT(*) as sample_count
        FROM queue_data qd
        INNER JOIN attractions a ON qd."attractionId" = a.id
        LEFT JOIN schedule_entries se
          ON se."parkId" = a."parkId"
          AND se.date = DATE(qd.timestamp AT TIME ZONE $2)
          AND se."attractionId" IS NULL
        WHERE a."parkId" = $1::uuid
          AND qd.timestamp >= $3
          AND qd."queueType" = 'STANDBY'
          AND qd.status = 'OPERATING'
          AND qd."waitTime" >= 10
          AND qd."waitTime" < 380
          AND (se.id IS NULL OR se."scheduleType" IN ('OPERATING', 'UNKNOWN'))
        GROUP BY a.id, a."parkId"
      ),
      -- Step 2: Calculate park-wide stats for relative thresholds
      park_stats AS (
        SELECT
          COUNT(*) as total_attractions,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_wait) as park_median_wait,
          PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY avg_wait) as park_60th_percentile, -- Calculated here to avoid Window Function error
          MAX(operating_days) as max_operating_days
        FROM attraction_stats
      ),
      -- Tier 1: Absolute thresholds (major parks)
      tier1_headliners AS (
        SELECT
          attraction_id,
          park_id,
          'tier1' as tier,
          avg_wait,
          p50_wait,
          p90_wait,
          operating_days,
          sample_count
        FROM attraction_stats ast
        CROSS JOIN park_stats ps
        WHERE ast.avg_wait > 20
          AND ast.p90_wait > 30
          AND ast.operating_days > (ps.max_operating_days * 0.6) -- Relaxed from 0.8
      ),
      -- Tier 2: Relative thresholds (medium parks) - only if Tier 1 < 3
      tier2_headliners AS (
        SELECT
          ast.attraction_id,
          ast.park_id,
          'tier2' as tier,
          ast.avg_wait,
          ast.p50_wait,
          ast.p90_wait,
          ast.operating_days,
          ast.sample_count
        FROM attraction_stats ast
        CROSS JOIN park_stats ps
        WHERE (SELECT COUNT(*) FROM tier1_headliners) < 3
          AND ast.avg_wait >= ps.park_60th_percentile  -- Top 40% (using pre-calculated value)
          AND ast.p90_wait > ast.p50_wait * 1.5    -- Can spike
          AND ast.operating_days > (ps.max_operating_days * 0.6) -- Relaxed from 0.7
      ),
      -- Tier 3: All attractions fallback (small parks) - only if Tier 1+2 < 3
      tier3_headliners AS (
        SELECT
          ast.attraction_id,
          ast.park_id,
          'tier3' as tier,
          ast.avg_wait,
          ast.p50_wait,
          ast.p90_wait,
          ast.operating_days,
          ast.sample_count
        FROM attraction_stats ast
        CROSS JOIN park_stats ps
        WHERE (SELECT COUNT(*) FROM tier1_headliners) < 3
          AND (SELECT COUNT(*) FROM tier2_headliners) < 3
          AND ast.avg_wait >= ps.park_median_wait  -- Relative threshold (Top 50%)
      )
      -- Union all tiers (priority: Tier 1 > Tier 2 > Tier 3)
      SELECT * FROM tier1_headliners
      UNION ALL
      SELECT * FROM tier2_headliners
      UNION ALL
      SELECT * FROM tier3_headliners
      ORDER BY tier, avg_wait DESC;
      `,
      [parkId, timezone, cutoff],
    );

    // Fallback: If no headliners found (e.g. small parks with low wait times),
    // pick Top 5 attractions by P90 wait time to ensure we have a baseline.
    if (result.length === 0) {
      this.logger.warn(
        `No headliners identified for park ${parkId} using standard tiers. Attempting fallback...`,
      );

      const fallbackResult = await this.queueDataRepository.query(
        `
        SELECT
          a.id as attraction_id,
          a."parkId" as park_id,
          'tier3' as tier,
          ROUND(AVG(qd."waitTime")::numeric, 2) as avg_wait,
          ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime")::numeric, 2) as p50_wait,
          ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime")::numeric, 2) as p90_wait,
          COUNT(DISTINCT DATE(qd.timestamp AT TIME ZONE $2)) as operating_days,
          COUNT(*) as sample_count
        FROM queue_data qd
        INNER JOIN attractions a ON qd."attractionId" = a.id
        LEFT JOIN schedule_entries se
          ON se."parkId" = a."parkId"
          AND se.date = DATE(qd.timestamp AT TIME ZONE $2)
          AND se."attractionId" IS NULL
        WHERE a."parkId" = $1::uuid
          AND qd.timestamp >= $3
          AND qd."queueType" = 'STANDBY'
          AND qd.status = 'OPERATING'
          AND qd."waitTime" >= 10
          AND qd."waitTime" < 380
          AND (se.id IS NULL OR se."scheduleType" IN ('OPERATING', 'UNKNOWN'))
        GROUP BY a.id, a."parkId"
        ORDER BY p90_wait DESC
        LIMIT 5
        `,
        [parkId, timezone, cutoff],
      );

      if (fallbackResult.length > 0) {
        this.logger.log(
          `Fallback: Identified ${fallbackResult.length} headliners for park ${parkId} (Top P90)`,
        );
        result.push(...fallbackResult);
      } else {
        this.logger.warn(
          `Fallback failed: No queue_data (STANDBY, OPERATING, waitTime>0) in the last ${SLIDING_WINDOW_DAYS} days for any attraction in park ${parkId} – check data coverage or seasonal closure`,
        );
      }
    }

    // Deduplicate by (parkId, attractionId): same attraction can appear in multiple tiers (UNION ALL).
    // Keep one row per attraction with best tier (tier1 > tier2 > tier3).
    const tierOrder = { tier1: 1, tier2: 2, tier3: 3 } as const;
    const byAttraction = new Map<string, { row: any; tierRank: number }>();
    for (const row of result) {
      const id = row.attraction_id as string;
      const rank = tierOrder[row.tier as keyof typeof tierOrder] ?? 3;
      const existing = byAttraction.get(id);
      if (!existing || rank < existing.tierRank) {
        byAttraction.set(id, { row, tierRank: rank });
      }
    }
    let deduped = Array.from(byAttraction.values()).map(({ row }) => row);

    // Cap tier1 results: if a major park yields many tier1 headliners, keep only
    // the top 10 by avg_wait. Borderline rides (avg ~20-22 min) otherwise dilute
    // the P50 baseline and distort crowd level / ML occupancy features.
    const MAX_TIER1_HEADLINERS = 10;
    const tier1Count = deduped.filter((r: any) => r.tier === "tier1").length;
    if (tier1Count > MAX_TIER1_HEADLINERS) {
      const tier1 = deduped
        .filter((r: any) => r.tier === "tier1")
        .sort(
          (a: any, b: any) => parseFloat(b.avg_wait) - parseFloat(a.avg_wait),
        )
        .slice(0, MAX_TIER1_HEADLINERS);
      deduped = tier1;
      this.logger.log(
        `Capped tier1 headliners for park ${parkId}: ${tier1Count} → ${MAX_TIER1_HEADLINERS}`,
      );
    }

    this.logger.log(
      `Identified ${deduped.length} headliners for park ${parkId} (Tiers: T1=${deduped.filter((r: any) => r.tier === "tier1").length}, T2=${deduped.filter((r: any) => r.tier === "tier2").length}, T3=${deduped.filter((r: any) => r.tier === "tier3").length})`,
    );

    return deduped.map((row: any) =>
      Object.assign(new HeadlinerAttraction(), {
        parkId,
        attractionId: row.attraction_id,
        tier: row.tier as "tier1" | "tier2" | "tier3",
        avgWait548d: parseFloat(row.avg_wait),
        p50Wait548d: parseFloat(row.p50_wait),
        p90Wait548d: parseFloat(row.p90_wait),
        operatingDays: parseInt(row.operating_days, 10),
        sampleCount: parseInt(row.sample_count, 10),
        lastCalculatedAt: new Date(),
        createdAt: new Date(),
        updatedAt: new Date(),
      }),
    );
  }

  /**
   * Calculate P50 (median) baseline for a park using headliners only
   *
   * @param parkId - Park ID
   * @param headliners - Array of headliner attractions
   * @returns P50 baseline object with value, confidence, and metadata
   */
  async calculateP50Baseline(
    parkId: string,
    headliners: HeadlinerAttraction[],
  ): Promise<{
    p50: number;
    p90: number;
    typicalDayPeak: number;
    sampleCount: number;
    distinctDays: number;
    confidence: "high" | "medium" | "low";
    tier: "tier1" | "tier2" | "tier3";
  }> {
    if (headliners.length === 0) {
      return {
        p50: 0,
        p90: 0,
        typicalDayPeak: 0,
        sampleCount: 0,
        distinctDays: 0,
        confidence: "low",
        tier: "tier3",
      };
    }

    // Calculate P50 as average of per-attraction P50s (statistically consistent with
    // getCurrentSpotWaitTime which computes avg-of-per-ride-averages).
    // Pooling all headliner data into a single PERCENTILE_CONT would be dominated by
    // high-frequency low-P50 rides, causing the park baseline to be underestimated
    // and crowd levels to be artificially inflated.
    const validHeadliners = headliners.filter((h) => Number(h.p50Wait548d) > 0);
    const p50 =
      validHeadliners.length > 0
        ? Math.round(
            (validHeadliners.reduce(
              (sum, h) => sum + Number(h.p50Wait548d),
              0,
            ) /
              validHeadliners.length) *
              100,
          ) / 100
        : 0;

    // P90 uses the same per-headliner-average approach. headliner_attractions
    // already has p90Wait548d populated alongside p50Wait548d so this is free.
    const validP90Headliners = headliners.filter(
      (h) => Number(h.p90Wait548d) > 0,
    );
    const p90 =
      validP90Headliners.length > 0
        ? Math.round(
            (validP90Headliners.reduce(
              (sum, h) => sum + Number(h.p90Wait548d),
              0,
            ) /
              validP90Headliners.length) *
              100,
          ) / 100
        : 0;

    const sampleCount = validHeadliners.reduce(
      (sum, h) => sum + (h.sampleCount || 0),
      0,
    );
    const distinctDays = Math.max(
      ...validHeadliners.map((h) => h.operatingDays || 0),
      0,
    );

    // Determine confidence level
    let confidence: "high" | "medium" | "low" = "low";
    if (distinctDays >= 90) {
      confidence = "high";
    } else if (distinctDays >= 30) {
      confidence = "medium";
    }

    // Determine tier (use highest tier from headliners)
    const tier =
      headliners.find((h) => h.tier === "tier1")?.tier ||
      headliners.find((h) => h.tier === "tier2")?.tier ||
      "tier3";

    // Typical-day-peak: computed together with P50/P90 (same headliner set,
    // same window) so the three are always written atomically — the calendar
    // never needs to fall back to P90/P50.
    const typicalDayPeak = await this.calculateTypicalDayPeak(
      parkId,
      headliners.map((h) => h.attractionId),
    );

    this.logger.log(
      `Calculated baselines for park ${parkId}: P50=${p50}min P90=${p90}min typical-day-peak=${typicalDayPeak}min (samples: ${sampleCount}, days: ${distinctDays}, confidence: ${confidence}, tier: ${tier})`,
    );

    return {
      p50,
      p90,
      typicalDayPeak,
      sampleCount,
      distinctDays,
      confidence,
      tier,
    };
  }

  /**
   * Typical-day-peak baseline = median over operating days (548-day window)
   * of the day value (AVG across headliners of each ride's daily P90).
   *
   * This is the reference the calendar divides a day's peak by, so that
   * 100% = a typical day's peak = `moderate`. The pooled P90 baseline is a
   * poor reference for "is this day busier than typical" because it is
   * inflated by the busiest season (which lives inside its own 548-day
   * window) and therefore compresses the top — even peak-season days can't
   * exceed it by much. The median-of-daily-peaks centers a typical day at
   * 100% and lets genuinely busy days (Wintertraum, Easter) reach
   * very_high/extreme. Headliner-only. Returns 0 when no qualifying data.
   */
  async calculateTypicalDayPeak(
    parkId: string,
    headlinerIds: string[],
  ): Promise<number> {
    if (headlinerIds.length === 0) return 0;
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["timezone"],
    });
    const timezone = park?.timezone || "UTC";
    const cutoff = subDays(new Date(), 548);
    const rows = await this.queueDataRepository.query(
      `
      WITH per_ride_day AS (
        SELECT DATE(qd.timestamp AT TIME ZONE $2) AS d,
               qd."attractionId",
               PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") AS p90
        FROM queue_data qd
        WHERE qd."attractionId" = ANY($1::uuid[])
          AND qd.timestamp >= $3
          AND qd.status = 'OPERATING'
          AND qd."waitTime" >= 10
          AND qd."queueType" = 'STANDBY'
        GROUP BY 1, 2
      ),
      daily AS (
        SELECT d, AVG(p90) AS day_val FROM per_ride_day GROUP BY d
      )
      SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY day_val)::numeric, 2) AS typical_day_peak
      FROM daily
      `,
      [headlinerIds, timezone, cutoff],
    );
    return rows[0]?.typical_day_peak ? parseFloat(rows[0].typical_day_peak) : 0;
  }

  /**
   * Cache the park typical-day-peak baseline (Redis only — no schema change;
   * recomputed by the daily cron). 2-day TTL so a single missed cron run
   * still leaves a usable value; callers fall back to the P90 baseline when
   * it's absent.
   *
   * NOTE: `park:typicalpeak:` (no hyphen) holds the typical-day peak WAIT
   * (number) — distinct from `park:typical-peak:` (with hyphen) in
   * getParkStatistics, which holds the typical peak HOUR ("HH:00" string).
   */
  async cacheTypicalDayPeak(parkId: string, value: number): Promise<void> {
    if (value > 0) {
      await this.redis.set(
        `park:typicalpeak:${parkId}`,
        value.toString(),
        "EX",
        86400 * 2,
      );
    }
  }

  /**
   * Read the park typical-day-peak baseline (Redis, then the
   * park_p50_baselines.typicalDayPeak column). Returns 0 when absent — which
   * only happens for a park with no baseline at all (brand-new), in which
   * case the caller falls through to the no-baseline default. Written
   * atomically with P50/P90, so there is no separate fallback path.
   */
  async getTypicalDayPeakFromCache(parkId: string): Promise<number> {
    const cached = await this.redis.get(`park:typicalpeak:${parkId}`);
    if (cached !== null && cached !== "") {
      return parseFloat(cached);
    }
    const row = await this.parkP50BaselineRepository.findOne({
      where: { parkId },
      select: ["typicalDayPeak"],
    });
    const value =
      row?.typicalDayPeak != null
        ? parseFloat(row.typicalDayPeak.toString())
        : 0;
    if (value > 0) {
      await this.cacheTypicalDayPeak(parkId, value);
    }
    return value;
  }

  /**
   * Save P50 baseline to database and cache
   *
   * @param parkId - Park ID
   * @param baseline - P50 baseline object
   * @param headliners - Array of headliner attractions
   */
  async saveP50Baselines(
    parkId: string,
    baseline: {
      p50: number;
      p90: number;
      typicalDayPeak: number;
      sampleCount: number;
      distinctDays: number;
      confidence: "high" | "medium" | "low";
      tier: "tier1" | "tier2" | "tier3";
    },
    headliners: HeadlinerAttraction[],
  ): Promise<void> {
    // Save headliners
    await this.headlinerAttractionRepository.delete({ parkId });
    await this.headlinerAttractionRepository.save(headliners);

    // Save park P50 baseline + typical-day-peak (Upsert to prevent duplicate
    // key errors). typicalDayPeak is the calendar reference, written
    // atomically here with P50 so the calendar never needs a fallback.
    await this.parkP50BaselineRepository.upsert(
      {
        parkId,
        p50Baseline: baseline.p50,
        typicalDayPeak:
          baseline.typicalDayPeak > 0 ? baseline.typicalDayPeak : null,
        headlinerCount: headliners.length,
        tier: baseline.tier,
        sampleCount: baseline.sampleCount,
        distinctDays: baseline.distinctDays,
        confidence: baseline.confidence,
        calculatedAt: new Date(),
      },
      ["parkId"], // Conflict path
    );

    // Save park P90 baseline alongside (only when we actually have one to
    // store — otherwise leave the row absent so callers fall back to "no
    // peak baseline" rather than treating 0 as a real value).
    if (baseline.p90 > 0) {
      await this.parkP90BaselineRepository.upsert(
        {
          parkId,
          p90Baseline: baseline.p90,
          headlinerCount: headliners.length,
          tier: baseline.tier,
          sampleCount: baseline.sampleCount,
          distinctDays: baseline.distinctDays,
          confidence: baseline.confidence,
          calculatedAt: new Date(),
        },
        ["parkId"],
      );
    }

    // Cache in Redis (24h TTL). We store JSON so callers that care about
    // confidence (calculateParkOccupancy) can read it without an extra DB
    // hop. The plain-number reader (getP50BaselineFromCache) understands
    // both this format and the legacy number-only format.
    const cacheKey = `park:p50:${parkId}`;
    await this.redis.set(
      cacheKey,
      JSON.stringify({ p50: baseline.p50, confidence: baseline.confidence }),
      "EX",
      86400,
    );

    if (baseline.p90 > 0) {
      await this.redis.set(
        `park:p90:${parkId}`,
        JSON.stringify({ p90: baseline.p90, confidence: baseline.confidence }),
        "EX",
        86400,
      );
    }

    // Cache the typical-day-peak (calendar reference) with the same lifecycle.
    await this.cacheTypicalDayPeak(parkId, baseline.typicalDayPeak);

    this.logger.log(
      `Saved baselines for park ${parkId}: P50=${baseline.p50}min P90=${baseline.p90}min typical-day-peak=${baseline.typicalDayPeak}min (${headliners.length} headliners, tier: ${baseline.tier})`,
    );
  }

  /**
   * Get P50 baseline value from cache or database.
   *
   * Returns just the numeric value. For callers that also need the
   * confidence level (e.g. calculateParkOccupancy), use
   * getP50BaselineWithConfidence().
   *
   * @param parkId - Park ID
   * @returns P50 baseline value (minutes)
   */
  async getP50BaselineFromCache(parkId: string): Promise<number> {
    const record = await this.getP50BaselineWithConfidence(parkId);
    return record ? record.value : 0;
  }

  /**
   * Get P50 baseline value AND confidence level from cache or database.
   *
   * The Redis cache key (`park:p50:{parkId}`) now stores JSON containing
   * both fields. Legacy entries written as plain numbers are still
   * accepted (they get treated as high-confidence) and naturally phase
   * out within the 24h TTL.
   *
   * @param parkId - Park ID
   * @returns { value, confidence } or null when no baseline exists yet
   */
  async getP50BaselineWithConfidence(
    parkId: string,
  ): Promise<{ value: number; confidence: "high" | "medium" | "low" } | null> {
    const cacheKey = `park:p50:${parkId}`;
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      if (cached.startsWith("{")) {
        try {
          const parsed = JSON.parse(cached) as {
            p50: number;
            confidence: "high" | "medium" | "low";
          };
          return { value: parsed.p50, confidence: parsed.confidence };
        } catch {
          // fall through to DB
        }
      } else {
        const v = parseFloat(cached);
        if (v > 0) {
          return { value: v, confidence: "high" };
        }
      }
    }

    const baseline = await this.parkP50BaselineRepository.findOne({
      where: { parkId },
    });

    if (baseline) {
      const value = parseFloat(baseline.p50Baseline.toString());
      const confidence = baseline.confidence;
      await this.redis.set(
        cacheKey,
        JSON.stringify({ p50: value, confidence }),
        "EX",
        86400,
      );
      return { value, confidence };
    }

    return null;
  }

  /**
   * Get attraction P50 baseline from cache or database (for crowd level, same as parks).
   *
   * @param attractionId - Attraction ID
   * @returns P50 baseline value (minutes), or 0 if not found
   */
  async getAttractionP50BaselineFromCache(
    attractionId: string,
  ): Promise<number> {
    const cacheKey = `attraction:p50:${attractionId}`;
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      return parseFloat(cached);
    }

    const baseline = await this.attractionP50BaselineRepository.findOne({
      where: { attractionId },
    });

    if (baseline) {
      await this.redis.set(
        cacheKey,
        baseline.p50Baseline.toString(),
        "EX",
        86400,
      );
      return parseFloat(baseline.p50Baseline.toString());
    }

    // Negative-cache the absence so attractions without a baseline row yet
    // (new/low-traffic rides) stop re-hitting Postgres on every request.
    // Short TTL so a freshly-computed baseline surfaces within ~6h.
    await this.redis
      .set(cacheKey, "0", "EX", NEGATIVE_BASELINE_TTL)
      .catch(() => undefined);
    return 0;
  }

  /**
   * Get the median baseline (P50) for an attraction. Used by compareParks
   * and single-attraction load ratings; matches the user-facing
   * peak-vs-median semantic and never triggers a live 548-day scan.
   * Returns 0 when the attraction has no baseline row yet (brand-new).
   */
  private async getBaselineForAttraction(
    attractionId: string,
  ): Promise<number> {
    return this.getAttractionP50BaselineFromCache(attractionId);
  }

  /**
   * Get headliner attraction IDs for a park as a Set (for O(1) lookup).
   */
  async getHeadlinerAttractionIds(parkId: string): Promise<Set<string>> {
    const headliners = await this.getHeadlinerAttractions(parkId);
    return new Set(headliners.map((h) => h.attractionId));
  }

  /**
   * Get full headliner entities for a park.
   */
  async getHeadlinerAttractions(
    parkId: string,
  ): Promise<HeadlinerAttraction[]> {
    // Read-through cache: headliners are recomputed by a periodic job and barely change,
    // but this is called on every calendar build + nearby/favorites crowd-level pass.
    const cacheKey = `analytics:headliners:${parkId}`;
    const cached = await this.redis.get(cacheKey);
    if (cached !== null) {
      try {
        return JSON.parse(cached) as HeadlinerAttraction[];
      } catch {
        // fall through to DB on corrupt cache
      }
    }
    const rows = await this.headlinerAttractionRepository.find({
      where: { parkId },
    });
    await this.redis
      .set(cacheKey, JSON.stringify(rows), "EX", 6 * 60 * 60)
      .catch(() => undefined);
    return rows;
  }

  /**
   * Get P50 baselines for multiple attractions (batch, for list/search).
   * Uses Redis + AttractionP50Baseline table. Missing IDs get 0 (caller should fallback to P90).
   */
  async getBatchAttractionP50s(
    attractionIds: string[],
  ): Promise<Map<string, number>> {
    const resultMap = new Map<string, number>();
    if (attractionIds.length === 0) return resultMap;

    const keys = attractionIds.map((id) => `attraction:p50:${id}`);
    const cached = await this.redis.mget(...keys);
    const uncachedIds: string[] = [];
    attractionIds.forEach((id, i) => {
      const v = cached[i];
      if (v != null && v !== "") {
        resultMap.set(id, parseFloat(v));
      } else {
        uncachedIds.push(id);
      }
    });

    if (uncachedIds.length === 0) return resultMap;

    const rows = await this.attractionP50BaselineRepository.find({
      where: { attractionId: In(uncachedIds) },
      select: ["attractionId", "p50Baseline"],
    });

    const pipeline = this.redis.pipeline();
    const found = new Set<string>();
    for (const row of rows) {
      const val = parseFloat(row.p50Baseline.toString());
      resultMap.set(row.attractionId, val);
      found.add(row.attractionId);
      pipeline.set(
        `attraction:p50:${row.attractionId}`,
        row.p50Baseline.toString(),
        "EX",
        86400,
      );
    }
    // Negative-cache IDs with no baseline row yet so they stop re-querying
    // Postgres every request. "0" reads back as 0, which all consumers treat
    // identically to "missing" (they fall back to P90 via `p50 || p90`).
    for (const id of uncachedIds) {
      if (!found.has(id)) {
        pipeline.set(`attraction:p50:${id}`, "0", "EX", NEGATIVE_BASELINE_TTL);
      }
    }
    await pipeline.exec();

    return resultMap;
  }

  /**
   * Calculate P50 (median) baseline for an individual attraction
   *
   * @param attractionId - Attraction ID
   * @returns P50 baseline object with value, confidence, and metadata
   */
  async calculateAttractionP50(attractionId: string): Promise<{
    p50: number;
    p90: number;
    sampleCount: number;
    distinctDays: number;
    confidence: "high" | "medium" | "low";
    isHeadliner: boolean;
  }> {
    const SLIDING_WINDOW_DAYS = 548;

    // Get attraction and park info
    const attraction = await this.attractionRepository.findOne({
      where: { id: attractionId },
      relations: ["park"],
      select: ["id", "parkId"],
    });

    if (!attraction) {
      return {
        p50: 0,
        p90: 0,
        sampleCount: 0,
        distinctDays: 0,
        confidence: "low",
        isHeadliner: false,
      };
    }

    const timezone = attraction.park?.timezone || "UTC";

    // Calculate cutoff date
    const now = new Date();
    const todayStr = formatInTimeZone(now, timezone, "yyyy-MM-dd");
    const today = fromZonedTime(`${todayStr}T00:00:00`, timezone);
    const cutoff = subDays(today, SLIDING_WINDOW_DAYS);

    // Query both percentiles in the same 548-day scan. PostgreSQL computes
    // PERCENTILE_CONT(0.5) and PERCENTILE_CONT(0.9) in a single sort, so
    // adding P90 here is essentially free vs. the prior P50-only query.
    const result = await this.queueDataRepository.query(
      `
      SELECT
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime")::numeric, 2) as p50,
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime")::numeric, 2) as p90,
        COUNT(*) as sample_count,
        COUNT(DISTINCT DATE(qd.timestamp AT TIME ZONE $2)) as distinct_days
      FROM queue_data qd
      JOIN attractions a ON a.id = qd."attractionId"
      LEFT JOIN schedule_entries se
        ON se."parkId" = a."parkId"
        AND se.date = DATE(qd.timestamp AT TIME ZONE $2)
        AND se."attractionId" IS NULL
      WHERE qd."attractionId" = $1::uuid
        AND qd.timestamp >= $3
        AND qd."queueType" = 'STANDBY'
        AND qd.status = 'OPERATING'
        AND qd."waitTime" >= 10
        AND (se.id IS NULL OR se."scheduleType" IN ('OPERATING', 'UNKNOWN'))
      `,
      [attractionId, timezone, cutoff],
    );

    const p50 = result[0]?.p50 ? parseFloat(result[0].p50) : 0;
    const p90 = result[0]?.p90 ? parseFloat(result[0].p90) : 0;
    const sampleCount = result[0]?.sample_count
      ? parseInt(result[0].sample_count, 10)
      : 0;
    const distinctDays = result[0]?.distinct_days
      ? parseInt(result[0].distinct_days, 10)
      : 0;

    // Determine confidence level
    let confidence: "high" | "medium" | "low" = "low";
    if (distinctDays >= 90) {
      confidence = "high";
    } else if (distinctDays >= 30) {
      confidence = "medium";
    }

    // Check if this is a headliner
    const headliner = await this.headlinerAttractionRepository.findOne({
      where: { attractionId, parkId: attraction.parkId },
    });

    this.logger.log(
      `Calculated P50/P90 baselines for attraction ${attractionId}: P50=${p50}min P90=${p90}min (samples: ${sampleCount}, days: ${distinctDays}, confidence: ${confidence}, headliner: ${!!headliner})`,
    );

    return {
      p50,
      p90,
      sampleCount,
      distinctDays,
      confidence,
      isHeadliner: !!headliner,
    };
  }

  /**
   * Save attraction P50 baseline to database and cache
   *
   * @param attractionId - Attraction ID
   * @param parkId - Park ID
   * @param baseline - P50 baseline object
   */
  /**
   * Batch counterpart of `calculateAttractionP50` — runs the 548-day
   * PERCENTILE_CONT once per *park* (GROUP BY attractionId) instead of
   * once per attraction. The daily cron previously fired ~10k of these
   * scans (one per attraction); this collapses it to ~50 (one per park),
   * a >100× drop in heavy queries.
   *
   * Returns a Map keyed by attractionId so the cron can decide what to
   * persist per attraction (zero-sample attractions are still included
   * so callers can log/skip them consistently with the single-shot
   * variant).
   */
  async calculateAttractionP50P90ForPark(
    parkId: string,
    timezone: string,
  ): Promise<
    Map<
      string,
      {
        p50: number;
        p90: number;
        sampleCount: number;
        distinctDays: number;
        confidence: "high" | "medium" | "low";
        isHeadliner: boolean;
      }
    >
  > {
    const SLIDING_WINDOW_DAYS = 548;
    const now = new Date();
    const todayStr = formatInTimeZone(now, timezone, "yyyy-MM-dd");
    const today = fromZonedTime(`${todayStr}T00:00:00`, timezone);
    const cutoff = subDays(today, SLIDING_WINDOW_DAYS);

    const rows = await this.queueDataRepository.query(
      `
      SELECT
        qd."attractionId" AS attraction_id,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qd."waitTime")::numeric, 2) AS p50,
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime")::numeric, 2) AS p90,
        COUNT(*) AS sample_count,
        COUNT(DISTINCT DATE(qd.timestamp AT TIME ZONE $2)) AS distinct_days
      FROM queue_data qd
      JOIN attractions a ON a.id = qd."attractionId"
      LEFT JOIN schedule_entries se
        ON se."parkId" = a."parkId"
        AND se.date = DATE(qd.timestamp AT TIME ZONE $2)
        AND se."attractionId" IS NULL
      WHERE a."parkId" = $1::uuid
        AND qd.timestamp >= $3
        AND qd."queueType" = 'STANDBY'
        AND qd.status = 'OPERATING'
        AND qd."waitTime" >= 10
        AND (se.id IS NULL OR se."scheduleType" IN ('OPERATING', 'UNKNOWN'))
      GROUP BY qd."attractionId"
      `,
      [parkId, timezone, cutoff],
    );

    // Headliner lookup also batched — one query per park instead of one
    // findOne per attraction.
    const headliners = await this.headlinerAttractionRepository.find({
      where: { parkId },
      select: ["attractionId"],
    });
    const headlinerSet = new Set(headliners.map((h) => h.attractionId));

    const result = new Map<
      string,
      {
        p50: number;
        p90: number;
        sampleCount: number;
        distinctDays: number;
        confidence: "high" | "medium" | "low";
        isHeadliner: boolean;
      }
    >();
    for (const row of rows) {
      const distinctDays = parseInt(row.distinct_days, 10) || 0;
      let confidence: "high" | "medium" | "low" = "low";
      if (distinctDays >= 90) confidence = "high";
      else if (distinctDays >= 30) confidence = "medium";
      result.set(row.attraction_id, {
        p50: row.p50 ? parseFloat(row.p50) : 0,
        p90: row.p90 ? parseFloat(row.p90) : 0,
        sampleCount: parseInt(row.sample_count, 10) || 0,
        distinctDays,
        confidence,
        isHeadliner: headlinerSet.has(row.attraction_id),
      });
    }
    return result;
  }

  /**
   * Bulk-persist a park's worth of attraction P50/P90 baselines in two
   * upserts (one per table) plus a pipelined Redis warmup. Replaces the
   * per-attraction save/upsert loop on the daily cron's hot path.
   *
   * Rows with p50 === 0 are skipped entirely (mirrors the single-shot
   * processor logic — sampleCount=0 means no qualifying data).
   */
  async saveAttractionP50P90BaselinesBatch(
    parkId: string,
    rows: Array<{
      attractionId: string;
      p50: number;
      p90: number;
      sampleCount: number;
      distinctDays: number;
      confidence: "high" | "medium" | "low";
      isHeadliner: boolean;
    }>,
  ): Promise<{ p50Saved: number; p90Saved: number }> {
    const now = new Date();
    const p50Rows = rows
      .filter((r) => r.p50 > 0)
      .map((r) => ({
        attractionId: r.attractionId,
        parkId,
        p50Baseline: r.p50,
        isHeadliner: r.isHeadliner,
        sampleCount: r.sampleCount,
        distinctDays: r.distinctDays,
        confidence: r.confidence,
        calculatedAt: now,
      }));
    const p90Rows = rows
      .filter((r) => r.p90 > 0)
      .map((r) => ({
        attractionId: r.attractionId,
        parkId,
        p90Baseline: r.p90,
        isHeadliner: r.isHeadliner,
        sampleCount: r.sampleCount,
        distinctDays: r.distinctDays,
        confidence: r.confidence,
        calculatedAt: now,
      }));

    if (p50Rows.length > 0) {
      await this.attractionP50BaselineRepository.upsert(p50Rows, [
        "attractionId",
      ]);
    }
    if (p90Rows.length > 0) {
      await this.attractionP90BaselineRepository.upsert(p90Rows, [
        "attractionId",
      ]);
    }

    // Pipelined Redis warmup — single round-trip regardless of count.
    // Skipped entirely when nothing was saved so we don't waste a
    // pipeline object on brand-new / empty parks.
    if (p50Rows.length > 0 || p90Rows.length > 0) {
      const pipeline = this.redis.pipeline();
      for (const r of p50Rows) {
        pipeline.set(
          `attraction:p50:${r.attractionId}`,
          r.p50Baseline.toString(),
          "EX",
          86400,
        );
      }
      for (const r of p90Rows) {
        pipeline.set(
          `attraction:p90:${r.attractionId}`,
          r.p90Baseline.toString(),
          "EX",
          86400,
        );
      }
      await pipeline.exec();
    }

    return { p50Saved: p50Rows.length, p90Saved: p90Rows.length };
  }

  async saveAttractionP50Baseline(
    attractionId: string,
    parkId: string,
    baseline: {
      p50: number;
      p90: number;
      sampleCount: number;
      distinctDays: number;
      confidence: "high" | "medium" | "low";
      isHeadliner: boolean;
    },
  ): Promise<void> {
    // Save P50 row
    await this.attractionP50BaselineRepository.save({
      attractionId,
      parkId,
      p50Baseline: baseline.p50,
      isHeadliner: baseline.isHeadliner,
      sampleCount: baseline.sampleCount,
      distinctDays: baseline.distinctDays,
      confidence: baseline.confidence,
      calculatedAt: new Date(),
    });

    // Save P90 row alongside (only when we have a real P90 value). A
    // missing row signals "no peak baseline yet" so callers know to
    // degrade gracefully rather than treat 0 as "peak = 0min".
    if (baseline.p90 > 0) {
      await this.attractionP90BaselineRepository.save({
        attractionId,
        parkId,
        p90Baseline: baseline.p90,
        isHeadliner: baseline.isHeadliner,
        sampleCount: baseline.sampleCount,
        distinctDays: baseline.distinctDays,
        confidence: baseline.confidence,
        calculatedAt: new Date(),
      });
    }

    // Cache in Redis (24h TTL)
    await this.redis.set(
      `attraction:p50:${attractionId}`,
      baseline.p50.toString(),
      "EX",
      86400,
    );
    if (baseline.p90 > 0) {
      await this.redis.set(
        `attraction:p90:${attractionId}`,
        baseline.p90.toString(),
        "EX",
        86400,
      );
    }

    this.logger.log(
      `Saved P50/P90 baselines for attraction ${attractionId}: P50=${baseline.p50}min P90=${baseline.p90}min`,
    );
  }

  /**
   * Park P90 baseline read API — mirrors getP50BaselineFromCache. Returns
   * 0 when no baseline exists yet (e.g. brand-new park before the next
   * cron run); callers must treat that as "no peak baseline" rather than
   * a real value.
   */
  async getP90BaselineFromCache(parkId: string): Promise<number> {
    const cacheKey = `park:p90:${parkId}`;
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      if (cached.startsWith("{")) {
        try {
          const parsed = JSON.parse(cached) as { p90: number };
          if (parsed.p90 > 0) return parsed.p90;
        } catch {
          // fall through to DB
        }
      } else {
        const v = parseFloat(cached);
        if (v > 0) return v;
      }
    }

    const row = await this.parkP90BaselineRepository.findOne({
      where: { parkId },
    });
    if (!row) return 0;

    const value = parseFloat(row.p90Baseline.toString());
    await this.redis.set(
      cacheKey,
      JSON.stringify({ p90: value, confidence: row.confidence }),
      "EX",
      86400,
    );
    return value;
  }

  /**
   * Per-attraction P90 baseline read API. Cache-first, falls back to DB
   * and re-populates the cache. Returns 0 when no baseline exists.
   */
  async getAttractionP90BaselineFromCache(
    attractionId: string,
  ): Promise<number> {
    const cacheKey = `attraction:p90:${attractionId}`;
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      const v = parseFloat(cached);
      if (v > 0) return v;
    }

    const row = await this.attractionP90BaselineRepository.findOne({
      where: { attractionId },
    });
    if (!row) return 0;

    const value = parseFloat(row.p90Baseline.toString());
    await this.redis.set(cacheKey, value.toString(), "EX", 86400);
    return value;
  }

  /**
   * Batch lookup for attraction P90 baselines. Uses Redis MGET first,
   * then hydrates missing entries from the DB and writes them back to
   * the cache. Replaces the old getBatchAttractionP90s which ran a
   * 548-day PERCENTILE_CONT live for the whole batch on every call.
   */
  async getBatchAttractionP90Baselines(
    attractionIds: string[],
  ): Promise<Map<string, number>> {
    const result = new Map<string, number>();
    if (attractionIds.length === 0) return result;

    const keys = attractionIds.map((id) => `attraction:p90:${id}`);
    const cached = await this.redis.mget(...keys);

    const missing: string[] = [];
    for (let i = 0; i < attractionIds.length; i++) {
      const raw = cached[i];
      if (raw) {
        // Negatively-cached absence — skip the DB, leave it out of the result
        // (consumers fall back to P50/0), don't re-query every request.
        if (raw === P90_NEGATIVE_SENTINEL) continue;
        const v = parseFloat(raw);
        if (v > 0) {
          result.set(attractionIds[i], v);
          continue;
        }
      }
      missing.push(attractionIds[i]);
    }

    if (missing.length > 0) {
      const rows = await this.attractionP90BaselineRepository.find({
        where: { attractionId: In(missing) },
      });
      const pipeline = this.redis.pipeline();
      const found = new Set<string>();
      for (const row of rows) {
        const value = parseFloat(row.p90Baseline.toString());
        if (value > 0) {
          result.set(row.attractionId, value);
          found.add(row.attractionId);
          pipeline.set(
            `attraction:p90:${row.attractionId}`,
            value.toString(),
            "EX",
            86400,
          );
        }
      }
      // Negative-cache IDs with no usable baseline so they stop hammering
      // Postgres on every request (short TTL → picks up new baselines soon).
      for (const id of missing) {
        if (!found.has(id)) {
          pipeline.set(
            `attraction:p90:${id}`,
            P90_NEGATIVE_SENTINEL,
            "EX",
            NEGATIVE_BASELINE_TTL,
          );
        }
      }
      await pipeline.exec();
    }

    return result;
  }

  /**
   * Read pre-aggregated hourly history rows for an attraction across a
   * date range. Returns a map keyed by date string ("YYYY-MM-DD") so the
   * caller can mix-and-match with live "today" data without per-date
   * round trips. Missing rows simply aren't in the map — callers should
   * treat absence as "no data for that day yet" rather than "zero
   * activity".
   */
  async getAttractionHourlyHistory(
    attractionId: string,
    fromDate: string,
    toDate: string,
  ): Promise<Map<string, AttractionHourlyHistory>> {
    const rows = await this.attractionHourlyHistoryRepository
      .createQueryBuilder("h")
      .where("h.attractionId = :attractionId", { attractionId })
      .andWhere("h.date BETWEEN :fromDate AND :toDate", { fromDate, toDate })
      .getMany();

    const map = new Map<string, AttractionHourlyHistory>();
    for (const row of rows) {
      const key =
        typeof row.date === "string"
          ? row.date
          : new Date(row.date).toISOString().split("T")[0];
      map.set(key, row);
    }
    return map;
  }

  /**
   * Per-park hourly rollup for a single date. One SQL pass produces
   * {attractionId, time_slot, p90, avgWait, sampleCount} buckets for
   * every attraction in the park — the cron processor groups these back
   * by attractionId and persists one row per attraction.
   *
   * The date range is computed in park timezone (start of day → next
   * day) so we always capture exactly one operating calendar day.
   */
  async computeParkHourlyHistoryForDate(
    parkId: string,
    date: string,
    timezone: string,
  ): Promise<
    Map<
      string,
      Array<{
        time_slot: string;
        p90: number;
        avgWait: number;
        sampleCount: number;
      }>
    >
  > {
    const startOfDay = fromZonedTime(`${date}T00:00:00`, timezone);
    const nextDay = new Date(startOfDay.getTime() + 24 * 60 * 60 * 1000);

    const rows = await this.queueDataRepository.query(
      `
      SELECT
        qd."attractionId" AS attraction_id,
        (LPAD(EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE $4)::text, 2, '0') ||
         ':' ||
         LPAD((FLOOR(EXTRACT(MINUTE FROM qd.timestamp AT TIME ZONE $4) / 15) * 15)::text, 2, '0')) AS time_slot,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") AS p90,
        AVG(qd."waitTime") AS avg_wait,
        COUNT(*) AS sample_count
      FROM queue_data qd
      JOIN attractions a ON a.id = qd."attractionId"
      WHERE a."parkId" = $1::uuid
        AND qd.timestamp >= $2
        AND qd.timestamp < $3
        AND qd.status = 'OPERATING'
        AND qd."queueType" = 'STANDBY'
        AND qd."waitTime" IS NOT NULL
        AND qd."waitTime" >= 5
      GROUP BY qd."attractionId", time_slot
      HAVING COUNT(*) >= 1
      ORDER BY qd."attractionId", time_slot
      `,
      [parkId, startOfDay, nextDay, timezone],
    );

    const result = new Map<
      string,
      Array<{
        time_slot: string;
        p90: number;
        avgWait: number;
        sampleCount: number;
      }>
    >();
    for (const row of rows) {
      const id = row.attraction_id as string;
      let bucket = result.get(id);
      if (!bucket) {
        bucket = [];
        result.set(id, bucket);
      }
      bucket.push({
        time_slot: row.time_slot,
        p90: roundToNearest5Minutes(parseFloat(row.p90)),
        avgWait: roundToNearest5Minutes(parseFloat(row.avg_wait)),
        sampleCount: parseInt(row.sample_count, 10) || 0,
      });
    }
    return result;
  }

  /**
   * Per-park down-count rollup for a single date. Mirrors the existing
   * inline query inside `getAttractionHistory` — counts distinct hours
   * with status='DOWN' per attraction. Returned as a Map<attractionId,
   * downCount>.
   */
  async computeParkDownCountForDate(
    parkId: string,
    date: string,
    timezone: string,
  ): Promise<Map<string, number>> {
    const startOfDay = fromZonedTime(`${date}T00:00:00`, timezone);
    const nextDay = new Date(startOfDay.getTime() + 24 * 60 * 60 * 1000);

    const rows = await this.queueDataRepository.query(
      `
      SELECT
        qd."attractionId" AS attraction_id,
        COUNT(DISTINCT DATE_TRUNC('hour', qd.timestamp AT TIME ZONE $4)) AS down_count
      FROM queue_data qd
      JOIN attractions a ON a.id = qd."attractionId"
      WHERE a."parkId" = $1::uuid
        AND qd.timestamp >= $2
        AND qd.timestamp < $3
        AND qd.status = 'DOWN'
      GROUP BY qd."attractionId"
      `,
      [parkId, startOfDay, nextDay, timezone],
    );

    const map = new Map<string, number>();
    for (const row of rows) {
      map.set(row.attraction_id as string, parseInt(row.down_count, 10) || 0);
    }
    return map;
  }

  /**
   * Bulk-upsert hourly history rows for a single date — one round-trip
   * regardless of how many attractions changed. Rows for attractions
   * that produced no slots are still written with an empty array so the
   * read path can tell "we know there was nothing" from "no row yet".
   */
  async saveAttractionHourlyHistoryBatch(
    rows: Array<{
      attractionId: string;
      parkId: string;
      date: string;
      slots: Array<{
        time_slot: string;
        p90: number;
        avgWait: number;
        sampleCount: number;
      }>;
      downCount: number;
    }>,
  ): Promise<void> {
    if (rows.length === 0) return;
    const now = new Date();
    await this.attractionHourlyHistoryRepository.upsert(
      rows.map((r) => ({
        attractionId: r.attractionId,
        parkId: r.parkId,
        date: r.date,
        slots: r.slots,
        downCount: r.downCount,
        calculatedAt: now,
      })),
      ["attractionId", "date"],
    );
  }

  // ──────────────────────────────────────────────────────────────────────────
  // Rope-Drop recommendations
  //
  // "Is it worth rope-dropping this headliner?" Two-layer model (see plan /
  // docs/analytics): shape (opening-relative ratio curve, pooled over history)
  // + levels (absolute minutes on a trailing window, weekend/weekday buckets,
  // recomputed daily so they track the current season). Source: the precomputed
  // `attraction_hourly_history` 15-min P90 slots + `schedule_entries` opening
  // times (so only parks/days with a schedule contribute — the feature gate).
  // ──────────────────────────────────────────────────────────────────────────

  private readonly ROPE_DROP_CACHE_TTL = 12 * 60 * 60; // 12h — flips daily

  /**
   * Compute rope-drop recommendations for all tier1/2 headliners of a park.
   * One SELECT per park over the hourly-history slots, then pure aggregation.
   * Returns a map keyed by attractionId; parks without a schedule yield empty.
   */
  async computeRopeDropForPark(
    parkId: string,
    timezone: string,
  ): Promise<Map<string, RopeDropComputeResult>> {
    // Slot timestamps are park-local "HH:MM"; opening time is a UTC instant.
    // Resolve both to local minutes-of-day and subtract → minutes-after-open.
    const rows: Array<{
      attraction_id: string;
      date: string;
      dow: number;
      mao: number;
      p90: number;
    }> = await this.queueDataRepository.query(
      `
      WITH hl AS (
        SELECT "attractionId"
        FROM headliner_attractions
        WHERE "parkId" = $1::uuid AND tier IN ('tier1', 'tier2')
      ),
      sched AS (
        SELECT date, MIN("openingTime") AS opening
        FROM schedule_entries
        WHERE "parkId" = $1::uuid
          AND "scheduleType" = 'OPERATING'
          AND "openingTime" IS NOT NULL
          AND "attractionId" IS NULL
        GROUP BY date
      )
      SELECT
        ahh."attractionId" AS attraction_id,
        ahh.date::text AS date,
        EXTRACT(DOW FROM ahh.date)::int AS dow,
        (
          (split_part(s->>'time_slot', ':', 1)::int * 60
           + split_part(s->>'time_slot', ':', 2)::int)
          - (EXTRACT(HOUR FROM sched.opening AT TIME ZONE $2) * 60
             + EXTRACT(MINUTE FROM sched.opening AT TIME ZONE $2))::int
        ) AS mao,
        (s->>'p90')::float AS p90
      FROM attraction_hourly_history ahh
      JOIN hl ON hl."attractionId" = ahh."attractionId"
      JOIN sched ON sched.date = ahh.date
      CROSS JOIN LATERAL jsonb_array_elements(ahh.slots) AS s
      WHERE ahh."parkId" = $1::uuid
        AND ahh.date >= (CURRENT_DATE - 800)
      `,
      [parkId, timezone],
    );

    // Group rows → per attraction → per date → slots.
    const byAttraction = new Map<string, Map<string, RopeDropDayInput>>();
    for (const row of rows) {
      const mao =
        typeof row.mao === "number" ? row.mao : parseInt(String(row.mao), 10);
      const p90 =
        typeof row.p90 === "number" ? row.p90 : parseFloat(String(row.p90));
      if (!Number.isFinite(mao) || !Number.isFinite(p90)) continue;

      let dates = byAttraction.get(row.attraction_id);
      if (!dates) {
        dates = new Map();
        byAttraction.set(row.attraction_id, dates);
      }
      let day = dates.get(row.date);
      if (!day) {
        day = { date: row.date, dow: Number(row.dow), slots: [] };
        dates.set(row.date, day);
      }
      day.slots.push({ minutesAfterOpen: mao, p90 });
    }

    // Trailing-window cutoff in park-local date terms.
    const windowStart = formatInTimeZone(
      subDays(new Date(), DEFAULT_ROPE_DROP_THRESHOLDS.windowDays),
      timezone,
      "yyyy-MM-dd",
    );

    const result = new Map<string, RopeDropComputeResult>();
    for (const [attractionId, dates] of byAttraction.entries()) {
      const computed = computeRopeDrop(Array.from(dates.values()), windowStart);
      if (computed) result.set(attractionId, computed);
    }
    return result;
  }

  /**
   * Bulk-persist a park's rope-drop rows (DB upsert + pipelined Redis warmup).
   * Stores all computed headliners (incl. `worth=false`) so the ride endpoint
   * can show "not worth today"; the park list filters `worth=true`.
   */
  async saveRopeDropBatch(
    parkId: string,
    results: Map<string, RopeDropComputeResult>,
  ): Promise<number> {
    if (results.size === 0) return 0;
    const now = new Date();
    const rows = Array.from(results.entries()).map(([attractionId, r]) => ({
      attractionId,
      parkId,
      worth: r.worth,
      strength: r.strength,
      confidence: r.confidence,
      busyPeak: r.busyPeak,
      openWait: r.openWait,
      savings: r.savings,
      rideByMinutesAfterOpen: r.rideByMinutesAfterOpen,
      bestSlotMinutesAfterOpen: r.bestSlotMinutesAfterOpen,
      bestSlotWait: r.bestSlotWait,
      endOfDayWorth: r.endOfDayWorth,
      endOfDaySavings: r.endOfDaySavings,
      byDaytype: r.byDaytype,
      windowDays: r.windowDays,
      sampleDays: r.sampleDays,
      calculatedAt: now,
    }));

    await this.attractionRopeDropRepository.upsert(rows, ["attractionId"]);

    const pipeline = this.redis.pipeline();
    for (const r of rows) {
      pipeline.set(
        `attraction:ropedrop:${r.attractionId}`,
        JSON.stringify(this.toRopeDropStored(r)),
        "EX",
        this.ROPE_DROP_CACHE_TTL,
      );
    }
    await pipeline.exec();

    return rows.length;
  }

  /** Map a stored/entity row to the cached `RopeDropStored` shape. */
  private toRopeDropStored(r: {
    worth: boolean;
    strength: "high" | "moderate" | null;
    confidence: "high" | "medium" | "low";
    busyPeak: number | string;
    openWait: number | string;
    savings: number | string;
    rideByMinutesAfterOpen: number;
    bestSlotMinutesAfterOpen: number;
    bestSlotWait: number | string;
    endOfDayWorth: boolean;
    endOfDaySavings: number | string;
    byDaytype: RopeDropStored["byDaytype"];
  }): RopeDropStored {
    return {
      worth: r.worth,
      strength: r.strength,
      confidence: r.confidence,
      busyPeak: Number(r.busyPeak),
      openWait: Number(r.openWait),
      savings: Number(r.savings),
      rideByMinutesAfterOpen: r.rideByMinutesAfterOpen,
      bestSlotMinutesAfterOpen: r.bestSlotMinutesAfterOpen,
      bestSlotWait: Number(r.bestSlotWait),
      endOfDayWorth: r.endOfDayWorth,
      endOfDaySavings: Number(r.endOfDaySavings),
      byDaytype: r.byDaytype,
    };
  }

  /**
   * Read-through single-attraction rope-drop lookup (Redis → DB → re-cache).
   * Returns null when the attraction has no recommendation (not a headliner,
   * park has no schedule, or insufficient data).
   */
  async getRopeDropFromCache(
    attractionId: string,
  ): Promise<RopeDropStored | null> {
    const cacheKey = `attraction:ropedrop:${attractionId}`;
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      try {
        return JSON.parse(cached) as RopeDropStored;
      } catch {
        // fall through to DB on malformed cache
      }
    }

    const row = await this.attractionRopeDropRepository.findOne({
      where: { attractionId },
    });
    if (!row) return null;

    const stored = this.toRopeDropStored(row);
    await this.redis.set(
      cacheKey,
      JSON.stringify(stored),
      "EX",
      this.ROPE_DROP_CACHE_TTL,
    );
    return stored;
  }

  /**
   * Batch rope-drop lookup for a whole park (used by the park integration
   * response). One DB read; returns a map keyed by attractionId.
   */
  async getRopeDropForPark(
    parkId: string,
  ): Promise<Map<string, RopeDropStored>> {
    const rows = await this.attractionRopeDropRepository.find({
      where: { parkId },
    });
    const map = new Map<string, RopeDropStored>();
    for (const row of rows) {
      map.set(row.attractionId, this.toRopeDropStored(row));
    }
    return map;
  }

  /** Count of rope-drop rows — used by the post-deploy bootstrap force-run. */
  async countRopeDropRows(): Promise<number> {
    return this.attractionRopeDropRepository.count();
  }
}
