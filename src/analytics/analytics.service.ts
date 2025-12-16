import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Park } from "../parks/entities/park.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { WeatherData } from "../parks/entities/weather-data.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";
import { RestaurantLiveData } from "../restaurants/entities/restaurant-live-data.entity";
import { ShowLiveData } from "../shows/entities/show-live-data.entity";
import { PredictionAccuracy } from "../ml/entities/prediction-accuracy.entity";
import { WaitTimePrediction } from "../ml/entities/wait-time-prediction.entity";
import { QueueDataAggregate } from "./entities/queue-data-aggregate.entity";
import {
  OccupancyDto,
  ParkStatisticsDto,
  AttractionStatisticsDto,
  GlobalStatsDto,
} from "./dto";
import { buildParkUrl } from "../common/utils/url.util";

import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";

@Injectable()
export class AnalyticsService {
  private readonly logger = new Logger(AnalyticsService.name);

  // Differentiated cache TTLs based on data characteristics
  private readonly TTL_REALTIME = 5 * 60; // 5 minutes - real-time wait times, occupancy
  private readonly TTL_INTEGRATED_RESPONSE = 5 * 60; // 5 minutes - integrated park/attraction responses
  private readonly TTL_PERCENTILES = 24 * 60 * 60; // 24 hours - historical percentiles (very stable)
  private readonly TTL_GLOBAL_STATS = 5 * 60; // 5 minutes - global statistics
  private readonly TTL_SCHEDULE = 60 * 60; // 1 hour - park schedules
  private readonly CACHE_TTL_SECONDS = 60 * 60; // 1 hour - legacy, kept for compatibility

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
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) { }

  /**
   * Calculate park occupancy based on 90th percentile of historical wait times
   * 100% = 90th percentile of typical wait times for this hour/weekday over last 1 year
   *
   * NOTE: Changed from P95 to P90 as universal baseline (Phase 4)
   */
  async calculateParkOccupancy(parkId: string): Promise<OccupancyDto> {
    const now = new Date();
    const currentHour = now.getHours();
    const currentDayOfWeek = now.getDay(); // 0 = Sunday, 6 = Saturday

    // Get current average wait time
    const currentAvgWait = await this.getCurrentAverageWaitTime(parkId);

    if (currentAvgWait === null) {
      return {
        current: 0,
        trend: "stable",
        comparedToTypical: 0,
        comparisonStatus: "typical",
        baseline90thPercentile: 0,
        updatedAt: now.toISOString(),
        breakdown: {
          currentAvgWait: 0,
          typicalAvgWait: 0,
          activeAttractions: 0,
        },
      };
    }

    // Calculate 90th percentile for this hour/weekday over last 1 year
    const p90Baseline = await this.get90thPercentileOneYear(
      parkId,
      currentHour,
      currentDayOfWeek,
      "park",
    );

    if (p90Baseline === 0) {
      this.logger.warn(
        `No historical data for park ${parkId} at hour ${currentHour}, day ${currentDayOfWeek}`,
      );
      return {
        current: 50, // Default to 50% if no historical data
        trend: "stable",
        comparedToTypical: 0,
        comparisonStatus: "typical",
        baseline90thPercentile: 0,
        updatedAt: now.toISOString(),
        breakdown: {
          currentAvgWait,
          typicalAvgWait: 0,
          activeAttractions: 0,
        },
      };
    }

    // Calculate occupancy as percentage of P90
    const occupancyPercentage = (currentAvgWait / p90Baseline) * 100;

    // Calculate trend (compare to previous reading - approx 1 hour ago)
    const twoHoursAgo = new Date();
    twoHoursAgo.setHours(twoHoursAgo.getHours() - 2);

    const previousData = await this.queueDataRepository
      .createQueryBuilder("qd")
      .innerJoin("qd.attraction", "a")
      .select("AVG(qd.waitTime)", "avgWait")
      .where("a.parkId = :parkId", { parkId })
      .andWhere("qd.timestamp >= :start", { start: twoHoursAgo })
      .andWhere("qd.timestamp < :now", { now: new Date() })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .andWhere("qd.queueType = :queueType", { queueType: "STANDBY" })
      .getRawOne();

    const previousAvgWait = previousData?.avgWait
      ? parseFloat(previousData.avgWait)
      : null;
    let trend: "up" | "down" | "stable" = "stable";
    if (previousAvgWait !== null) {
      const change = currentAvgWait - previousAvgWait;
      if (Math.abs(change) > 5) {
        // Significant change threshold: 5 minutes
        trend = change > 0 ? "up" : "down";
      }
    }

    // Get typical wait time for this day of week (average over last year)
    const yearAgo = new Date(now);
    yearAgo.setFullYear(yearAgo.getFullYear() - 1);

    const typicalAvgWait = await this.getDailyAverageWaitTime(
      parkId,
      yearAgo,
      now,
    );

    // Compare to typical
    const comparedToTypical = currentAvgWait - (typicalAvgWait || 0);
    let comparisonStatus: "higher" | "lower" | "typical" = "typical";
    if (Math.abs(comparedToTypical) > 10) {
      comparisonStatus = comparedToTypical > 0 ? "higher" : "lower";
    }

    return {
      current: Math.round(occupancyPercentage),
      trend,
      comparedToTypical: Math.round(comparedToTypical),
      comparisonStatus,
      baseline90thPercentile: Math.round(p90Baseline),
      updatedAt: now.toISOString(),
      breakdown: {
        currentAvgWait: Math.round(currentAvgWait),
        typicalAvgWait: Math.round(typicalAvgWait || 0),
        activeAttractions: await this.getActiveAttractionsCount(parkId),
      },
    };
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
    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);

    try {
      // Query aggregates for all attractions in this park today
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
        return null; // No data available for today
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
    iqr: number;
    sampleCount: number;
  } | null> {
    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);

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
   * Get rolling percentiles for an attraction over a time window
   * Used for historical comparisons and ML features
   *
   * @param attractionId - Attraction ID
   * @param days - Number of days to look back (default: 7)
   */
  async getAttractionRollingPercentiles(
    attractionId: string,
    days: number = 7,
  ): Promise<{
    p50: number;
    p90: number;
    iqr: number;
  } | null> {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);

    try {
      const result = await this.queueDataAggregateRepository
        .createQueryBuilder("agg")
        .select("percentile_cont(0.50) WITHIN GROUP (ORDER BY agg.p50)", "p50")
        .addSelect(
          "percentile_cont(0.90) WITHIN GROUP (ORDER BY agg.p90)",
          "p90",
        )
        .addSelect("AVG(agg.iqr)", "iqr")
        .where("agg.attractionId = :attractionId", { attractionId })
        .andWhere("agg.hour >= :cutoff", { cutoff })
        .getRawOne();

      if (!result || result.p50 === null) {
        return null;
      }

      return {
        p50: Math.round(parseFloat(result.p50)),
        p90: Math.round(parseFloat(result.p90)),
        iqr: Math.round(parseFloat(result.iqr || "0")),
      };
    } catch (error) {
      this.logger.warn(
        `Failed to get rolling percentiles for ${attractionId}:`,
        error,
      );
      return null;
    }
  }

  /**
   * Get current average wait time across all operating attractions in a park
   */
  private async getCurrentAverageWaitTime(
    parkId: string,
  ): Promise<number | null> {
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);

    const result = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("AVG(qd.waitTime)", "avgWait")
      .innerJoin("qd.attraction", "attraction")
      .where("attraction.parkId = :parkId", { parkId })
      .andWhere("qd.timestamp >= :fiveMinutesAgo", { fiveMinutesAgo })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .andWhere("qd.waitTime IS NOT NULL")
      .andWhere("qd.waitTime > 0")
      .andWhere("qd.queueType = 'STANDBY'") // Only consider standby queues
      .getRawOne();

    return result?.avgWait ? parseFloat(result.avgWait) : null;
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
      .andWhere("qd.waitTime > 0")
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
   * Detect trend by comparing average wait times over last 3 hours
   */
  private async detectTrend(
    parkId: string,
  ): Promise<"increasing" | "stable" | "decreasing"> {
    const now = new Date();
    const threeHoursAgo = new Date(now.getTime() - 3 * 60 * 60 * 1000);
    const twoHoursAgo = new Date(now.getTime() - 2 * 60 * 60 * 1000);
    const oneHourAgo = new Date(now.getTime() - 1 * 60 * 60 * 1000);

    const getAvgForPeriod = async (start: Date, end: Date): Promise<number> => {
      const result = await this.queueDataRepository
        .createQueryBuilder("qd")
        .select("AVG(qd.waitTime)", "avgWait")
        .innerJoin("qd.attraction", "attraction")
        .where("attraction.parkId = :parkId", { parkId })
        .andWhere("qd.timestamp BETWEEN :start AND :end", { start, end })
        .andWhere("qd.status = :status", { status: "OPERATING" })
        .andWhere("qd.waitTime IS NOT NULL")
        .andWhere("qd.queueType = 'STANDBY'")
        .getRawOne();

      return result?.avgWait ? parseFloat(result.avgWait) : 0;
    };

    const avgThreeToTwo = await getAvgForPeriod(threeHoursAgo, twoHoursAgo);
    const avgTwoToOne = await getAvgForPeriod(twoHoursAgo, oneHourAgo);
    const avgLastHour = await getAvgForPeriod(oneHourAgo, now);

    if (avgThreeToTwo === 0 || avgTwoToOne === 0 || avgLastHour === 0) {
      return "stable";
    }

    // Calculate average change
    const change1 = avgTwoToOne - avgThreeToTwo;
    const change2 = avgLastHour - avgTwoToOne;
    const avgChange = (change1 + change2) / 2;

    // Threshold: 5 minutes average change
    if (avgChange > 5) return "increasing";
    if (avgChange < -5) return "decreasing";
    return "stable";
  }

  /**
   * Get count of currently operating attractions
   */
  private async getActiveAttractionsCount(parkId: string): Promise<number> {
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);

    const result = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("COUNT(DISTINCT qd.attractionId)", "count")
      .innerJoin("qd.attraction", "attraction")
      .where("attraction.parkId = :parkId", { parkId })
      .andWhere("qd.timestamp >= :fiveMinutesAgo", { fiveMinutesAgo })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .getRawOne();

    return result?.count ? parseInt(result.count) : 0;
  }

  /**
   * Get park-wide statistics
   */
  async getParkStatistics(parkId: string): Promise<ParkStatisticsDto> {
    const now = new Date();
    const startOfDay = new Date(now);
    startOfDay.setHours(0, 0, 0, 0);

    // Get current average wait time
    const avgWaitTime = await this.getCurrentAverageWaitTime(parkId);

    // Get daily average wait time
    const avgWaitToday = await this.getDailyAverageWaitTime(
      parkId,
      startOfDay,
      now,
    );

    // Get peak hour today
    const peakHour = await this.getPeakHourToday(parkId, startOfDay, now);

    // Get attraction counts by status
    const { total, operating, closed } = await this.getAttractionCounts(parkId);

    // Determine crowd level based on occupancy
    const occupancy = await this.calculateParkOccupancy(parkId);
    const crowdLevel = this.determineCrowdLevel(occupancy.current);

    return {
      avgWaitTime: avgWaitTime ? Math.round(avgWaitTime) : 0,
      avgWaitToday: avgWaitToday ? Math.round(avgWaitToday) : 0,
      peakHour,
      crowdLevel,
      totalAttractions: total,
      operatingAttractions: operating,
      closedAttractions: closed,
      timestamp: now,
    };
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
      .andWhere("qd.waitTime > 0")
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
   * Get attraction counts by status
   * Matches logic in ParksController.buildIntegratedParkResponse:
   * - Attractions WITH queue_data: use status from queue_data
   * - Attractions WITHOUT queue_data: considered CLOSED
   */
  private async getAttractionCounts(
    parkId: string,
  ): Promise<{ total: number; operating: number; closed: number }> {
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
        ORDER BY qd."attractionId", qd.timestamp DESC
      )
      SELECT COUNT(*) as operating_count
      FROM attractions a
      LEFT JOIN latest_queue lq ON lq."attractionId" = a.id
      WHERE a."parkId" = $1
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
   * Determine crowd level from occupancy score
   */
  private determineCrowdLevel(
    occupancy: number,
  ): "very_low" | "low" | "moderate" | "high" | "very_high" {
    if (occupancy < 30) return "very_low";
    if (occupancy < 50) return "low";
    if (occupancy < 75) return "moderate";
    if (occupancy < 95) return "high";
    return "very_high";
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
   * Get attraction-specific statistics
   */
  async getAttractionStatistics(
    attractionId: string,
  ): Promise<AttractionStatisticsDto> {
    const now = new Date();
    const startOfDay = new Date(now);
    startOfDay.setHours(0, 0, 0, 0);

    const currentHour = now.getHours();
    const currentDayOfWeek = now.getDay();

    // Get today's statistics
    const todayStats = await this.getAttractionStatsForPeriod(
      attractionId,
      startOfDay,
      now,
    );

    // Get typical wait for this hour/weekday (2-year average)
    const typicalWait = await this.getTypicalWaitForHour(
      attractionId,
      currentHour,
      currentDayOfWeek,
    );

    // Get 95th percentile for this hour/weekday
    const p95ThisHour = await this.get95thPercentileForAttraction(
      attractionId,
      currentHour,
      currentDayOfWeek,
    );

    // Calculate current vs typical
    const currentVsTypical =
      todayStats.avg && typicalWait
        ? Math.round(((todayStats.avg - typicalWait) / typicalWait) * 100)
        : null;

    return {
      avgWaitToday: todayStats.avg,
      peakWaitToday: todayStats.max,
      minWaitToday: todayStats.min,
      typicalWaitThisHour: typicalWait,
      percentile95ThisHour: p95ThisHour,
      currentVsTypical,
      dataPoints: todayStats.count,
      timestamp: now,
    };
  }

  /**
   * Get attraction statistics for a period
   */
  private async getAttractionStatsForPeriod(
    attractionId: string,
    start: Date,
    end: Date,
  ): Promise<{
    avg: number | null;
    max: number | null;
    min: number | null;
    count: number;
  }> {
    const result = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("AVG(qd.waitTime)", "avg")
      .addSelect("MAX(qd.waitTime)", "max")
      .addSelect("MIN(qd.waitTime)", "min")
      .addSelect("COUNT(*)", "count")
      .where("qd.attractionId = :attractionId", { attractionId })
      .andWhere("qd.timestamp BETWEEN :start AND :end", { start, end })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .andWhere("qd.waitTime IS NOT NULL")
      .andWhere("qd.queueType = 'STANDBY'")
      .getRawOne();

    return {
      avg: result?.avg ? Math.round(parseFloat(result.avg)) : null,
      max: result?.max ? Math.round(parseFloat(result.max)) : null,
      min: result?.min ? Math.round(parseFloat(result.min)) : null,
      count: result?.count ? parseInt(result.count) : 0,
    };
  }

  /**
   * Get typical wait time for specific hour/weekday (2-year average)
   */
  private async getTypicalWaitForHour(
    attractionId: string,
    hour: number,
    dayOfWeek: number,
  ): Promise<number | null> {
    const twoYearsAgo = new Date();
    twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);

    const result = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("AVG(qd.waitTime)", "avgWait")
      .where("qd.attractionId = :attractionId", { attractionId })
      .andWhere("qd.timestamp >= :twoYearsAgo", { twoYearsAgo })
      .andWhere("EXTRACT(HOUR FROM qd.timestamp) = :hour", { hour })
      .andWhere("EXTRACT(DOW FROM qd.timestamp) = :dayOfWeek", { dayOfWeek })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .andWhere("qd.waitTime IS NOT NULL")
      .andWhere("qd.queueType = 'STANDBY'")
      .getRawOne();

    return result?.avgWait ? Math.round(parseFloat(result.avgWait)) : null;
  }

  /**
   * Get 95th percentile for specific attraction/hour/weekday
   */
  private async get95thPercentileForAttraction(
    attractionId: string,
    hour: number,
    dayOfWeek: number,
  ): Promise<number | null> {
    const twoYearsAgo = new Date();
    twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);

    const waitTimes = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select("qd.waitTime", "waitTime")
      .where("qd.attractionId = :attractionId", { attractionId })
      .andWhere("qd.timestamp >= :twoYearsAgo", { twoYearsAgo })
      .andWhere("EXTRACT(HOUR FROM qd.timestamp) = :hour", { hour })
      .andWhere("EXTRACT(DOW FROM qd.timestamp) = :dayOfWeek", { dayOfWeek })
      .andWhere("qd.status = :status", { status: "OPERATING" })
      .andWhere("qd.waitTime IS NOT NULL")
      .andWhere("qd.queueType = 'STANDBY'")
      .getRawMany();

    if (waitTimes.length === 0) return null;

    const sortedWaitTimes = waitTimes
      .map((wt) => parseFloat(wt.waitTime))
      .sort((a, b) => a - b);

    const percentileIndex = Math.ceil(sortedWaitTimes.length * 0.95) - 1;
    return Math.round(sortedWaitTimes[percentileIndex]);
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
  ): Promise<{
    trend: "increasing" | "stable" | "decreasing";
    changeRate: number; // Minutes per hour
    recentAverage: number | null; // Last hour average
    previousAverage: number | null; // 2-3 hours ago average
  }> {
    const now = new Date();
    const threeHoursAgo = new Date(now.getTime() - 3 * 60 * 60 * 1000);
    const twoHoursAgo = new Date(now.getTime() - 2 * 60 * 60 * 1000);
    const oneHourAgo = new Date(now.getTime() - 1 * 60 * 60 * 1000);

    const getAvgForPeriod = async (
      start: Date,
      end: Date,
    ): Promise<number | null> => {
      const result = await this.queueDataRepository
        .createQueryBuilder("qd")
        .select("AVG(qd.waitTime)", "avgWait")
        .where("qd.attractionId = :attractionId", { attractionId })
        .andWhere("qd.timestamp BETWEEN :start AND :end", { start, end })
        .andWhere("qd.status = :status", { status: "OPERATING" })
        .andWhere("qd.waitTime IS NOT NULL")
        .andWhere("qd.queueType = :queueType", { queueType })
        .getRawOne();

      return result?.avgWait ? parseFloat(result.avgWait) : null;
    };

    const avgThreeToTwo = await getAvgForPeriod(threeHoursAgo, twoHoursAgo);
    const avgTwoToOne = await getAvgForPeriod(twoHoursAgo, oneHourAgo);
    const avgLastHour = await getAvgForPeriod(oneHourAgo, now);

    // Not enough data
    if (avgLastHour === null || avgTwoToOne === null) {
      return {
        trend: "stable",
        changeRate: 0,
        recentAverage: avgLastHour,
        previousAverage: avgTwoToOne,
      };
    }

    // Calculate change rate (minutes per hour)
    const changeRate = avgLastHour - avgTwoToOne;

    // Threshold: 10% change (user specified)
    const threshold = avgTwoToOne * 0.1;

    let trend: "increasing" | "stable" | "decreasing" = "stable";

    // If we have 3-hour data, use weighted average for more accurate trend
    if (avgThreeToTwo !== null) {
      const change1 = avgTwoToOne - avgThreeToTwo;
      const change2 = avgLastHour - avgTwoToOne;
      const avgChange = (change1 + change2) / 2;

      // Use both absolute threshold (5 min) and relative threshold (10%)
      if (avgChange > Math.max(5, threshold)) {
        trend = "increasing";
      } else if (avgChange < -Math.max(5, threshold)) {
        trend = "decreasing";
      }
    } else {
      // Use simple comparison for 2-hour data
      if (changeRate > Math.max(5, threshold)) {
        trend = "increasing";
      } else if (changeRate < -Math.max(5, threshold)) {
        trend = "decreasing";
      }
    }

    return {
      trend,
      changeRate: Math.round(changeRate * 10) / 10, // Round to 1 decimal
      recentAverage: Math.round(avgLastHour),
      previousAverage: Math.round(avgTwoToOne),
    };
  }
  /**
   * Calculate 90th percentile wait time for specific hour/weekday over last 1 year
   * Uses Redis caching to avoid expensive DB queries
   * Implements cascading fallback strategy for sparse data
   */
  async get90thPercentileOneYear(
    entityId: string,
    hour: number,
    dayOfWeek: number,
    type: "park" | "attraction",
  ): Promise<number> {
    const cacheKey = `analytics:percentile:${type}:${entityId}:${hour}:${dayOfWeek}`;

    // Try cache first
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      return parseInt(cached, 10);
    }

    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);

    let waitTimes: any[] = [];

    if (type === "attraction") {
      // Strategy 1: Exact hour + day of week (last year) - The "Gold Standard"
      const strict = await this.queueDataRepository
        .createQueryBuilder("qd")
        .select("qd.waitTime", "waitTime")
        .where("qd.attractionId = :entityId", { entityId })
        .andWhere("qd.timestamp >= :oneYearAgo", { oneYearAgo })
        .andWhere("EXTRACT(HOUR FROM qd.timestamp) = :hour", { hour })
        .andWhere("EXTRACT(DOW FROM qd.timestamp) = :dayOfWeek", { dayOfWeek })
        .andWhere("qd.status = :status", { status: "OPERATING" })
        .andWhere("qd.waitTime IS NOT NULL")
        .andWhere("qd.queueType = 'STANDBY'")
        .getRawMany();

      if (strict.length >= 5) {
        waitTimes = strict;
      } else {
        // Strategy 2: Same hour, any day of week (last year)
        const sameHour = await this.queueDataRepository
          .createQueryBuilder("qd")
          .select("qd.waitTime", "waitTime")
          .where("qd.attractionId = :entityId", { entityId })
          .andWhere("qd.timestamp >= :oneYearAgo", { oneYearAgo })
          .andWhere("EXTRACT(HOUR FROM qd.timestamp) = :hour", { hour })
          .andWhere("qd.status = :status", { status: "OPERATING" })
          .andWhere("qd.waitTime IS NOT NULL")
          .andWhere("qd.queueType = 'STANDBY'")
          .getRawMany();

        if (sameHour.length >= 20) {
          waitTimes = sameHour;
        } else {
          // Strategy 3: Same day of week, any hour (last year)
          const sameDay = await this.queueDataRepository
            .createQueryBuilder("qd")
            .select("qd.waitTime", "waitTime")
            .where("qd.attractionId = :entityId", { entityId })
            .andWhere("qd.timestamp >= :oneYearAgo", { oneYearAgo })
            .andWhere("EXTRACT(DOW FROM qd.timestamp) = :dayOfWeek", {
              dayOfWeek,
            })
            .andWhere("qd.status = :status", { status: "OPERATING" })
            .andWhere("qd.waitTime IS NOT NULL")
            .andWhere("qd.queueType = 'STANDBY'")
            .getRawMany();

          // Validation / Selection Logic
          // Prefer Strict > SameHour > SameDay if they have *any* data
          if (strict.length > 0) {
            waitTimes = strict;
          } else if (sameHour.length > 0) {
            waitTimes = sameHour;
          } else if (sameDay.length > 0) {
            waitTimes = sameDay;
          } else {
            // Last resort: Any data from last 1 year (expanded from 30 days)
            const anyData = await this.queueDataRepository
              .createQueryBuilder("qd")
              .select("qd.waitTime", "waitTime")
              .where("qd.attractionId = :entityId", { entityId })
              .andWhere("qd.timestamp >= :oneYearAgo", { oneYearAgo }) // KEY CHANGE: 30 days -> 1 year
              .andWhere("qd.status = :status", { status: "OPERATING" })
              .andWhere("qd.waitTime IS NOT NULL")
              .andWhere("qd.queueType = 'STANDBY'")
              .getRawMany();
            waitTimes = anyData;
          }
        }
      }
    } else {
      // Park Logic (Same Waterfall)
      const strict = await this.queueDataRepository
        .createQueryBuilder("qd")
        .select("qd.waitTime", "waitTime")
        .innerJoin("qd.attraction", "attraction")
        .where("attraction.parkId = :entityId", { entityId })
        .andWhere("qd.timestamp >= :oneYearAgo", { oneYearAgo })
        .andWhere("EXTRACT(HOUR FROM qd.timestamp) = :hour", { hour })
        .andWhere("EXTRACT(DOW FROM qd.timestamp) = :dayOfWeek", { dayOfWeek })
        .andWhere("qd.status = :status", { status: "OPERATING" })
        .andWhere("qd.waitTime IS NOT NULL")
        .andWhere("qd.queueType = 'STANDBY'")
        .getRawMany();

      if (strict.length >= 10) {
        waitTimes = strict;
      } else {
        const sameHour = await this.queueDataRepository
          .createQueryBuilder("qd")
          .select("qd.waitTime", "waitTime")
          .innerJoin("qd.attraction", "attraction")
          .where("attraction.parkId = :entityId", { entityId })
          .andWhere("qd.timestamp >= :oneYearAgo", { oneYearAgo })
          .andWhere("EXTRACT(HOUR FROM qd.timestamp) = :hour", { hour })
          .andWhere("qd.status = :status", { status: "OPERATING" })
          .andWhere("qd.waitTime IS NOT NULL")
          .andWhere("qd.queueType = 'STANDBY'")
          .getRawMany();

        if (sameHour.length >= 50) {
          waitTimes = sameHour;
        } else {
          const sameDay = await this.queueDataRepository
            .createQueryBuilder("qd")
            .select("qd.waitTime", "waitTime")
            .innerJoin("qd.attraction", "attraction")
            .where("attraction.parkId = :entityId", { entityId })
            .andWhere("qd.timestamp >= :oneYearAgo", { oneYearAgo })
            .andWhere("EXTRACT(DOW FROM qd.timestamp) = :dayOfWeek", {
              dayOfWeek,
            })
            .andWhere("qd.status = :status", { status: "OPERATING" })
            .andWhere("qd.waitTime IS NOT NULL")
            .andWhere("qd.queueType = 'STANDBY'")
            .getRawMany();

          if (strict.length > 0) waitTimes = strict;
          else if (sameHour.length > 0) waitTimes = sameHour;
          else if (sameDay.length > 0) waitTimes = sameDay;
          else {
            const anyData = await this.queueDataRepository
              .createQueryBuilder("qd")
              .select("qd.waitTime", "waitTime")
              .innerJoin("qd.attraction", "attraction")
              .where("attraction.parkId = :entityId", { entityId })
              .andWhere("qd.timestamp >= :oneYearAgo", { oneYearAgo })
              .andWhere("qd.status = :status", { status: "OPERATING" })
              .andWhere("qd.waitTime IS NOT NULL")
              .andWhere("qd.queueType = 'STANDBY'")
              .getRawMany();
            waitTimes = anyData;
          }
        }
      }
    }

    let value = 0;
    if (waitTimes.length > 0) {
      const sorted = waitTimes
        .map((w) => parseFloat(w.waitTime))
        .sort((a, b) => a - b);
      const idx = Math.ceil(sorted.length * 0.9) - 1;
      value = Math.round(sorted[idx]);
    }

    // Cache result in Redis (even if 0) - 24h TTL for stable historical data
    await this.redis.set(
      cacheKey,
      value.toString(),
      "EX",
      this.TTL_PERCENTILES,
    );

    return value;
  }

  /**
   * Calculate load rating based on current wait vs 90th percentile baseline
   */
  getLoadRating(
    current: number,
    baseline: number,
  ): {
    rating: "very_low" | "low" | "normal" | "higher" | "high" | "extreme";
    baseline: number;
  } {
    // If baseline is 0 (no historical data), use absolute thresholds
    if (baseline === 0) {
      let rating: "very_low" | "low" | "normal" | "higher" | "high" | "extreme";

      if (current === 0) rating = "very_low";
      else if (current <= 15) rating = "low";
      else if (current <= 30) rating = "normal";
      else if (current <= 45) rating = "higher";
      else if (current <= 60) rating = "high";
      else rating = "extreme";

      return { rating, baseline };
    }

    const ratio = current / baseline;

    let rating: "very_low" | "low" | "normal" | "higher" | "high" | "extreme" =
      "normal";

    // Adjusted thresholds: ratio = 1.0 (current == baseline) should be "normal"
    if (ratio <= 0.3) rating = "very_low";
    else if (ratio <= 0.6) rating = "low";
    else if (ratio <= 1.05)
      rating = "normal"; // Up to 5% above baseline is still normal
    else if (ratio <= 1.3) rating = "higher";
    else if (ratio <= 1.6) rating = "high";
    else rating = "extreme";

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
  async getGlobalRealtimeStats(): Promise<GlobalStatsDto> {
    const cacheKey = "analytics:global_stats:v2";
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      return JSON.parse(cached);
    }

    // 1. Get Park Statuses & Average Waits concurrently
    // We check parks that have had queue updates in the last 60 minutes to consider them "active"
    // or active schedule. For simplicity & speed, we use the last updated queue_data.
    const activeParksResult = await this.queueDataRepository.query(`
      WITH latest_updates AS (
        SELECT DISTINCT ON (qd."attractionId")
          qd."attractionId",
          qd."waitTime",
          a."parkId",
          qd.timestamp
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        WHERE qd.timestamp > NOW() - INTERVAL '60 minutes'
        AND qd.status = 'OPERATING'
        ORDER BY qd."attractionId", qd.timestamp DESC
      ),
      park_stats AS (
        SELECT
          p.id,
          p.name,
          p.slug,
          p."continentSlug",
          p."countrySlug",
          p."citySlug",
          AVG(lu."waitTime") as avg_wait,
          COUNT(*) as active_rides
        FROM latest_updates lu
        JOIN parks p ON p.id = lu."parkId"
        GROUP BY p.id, p.name, p.slug, p."continentSlug", p."countrySlug", p."citySlug"
      )
      SELECT * FROM park_stats
    `);

    // Count open parks (those with > 0 active rides)
    const openParks = activeParksResult;

    // Parallel count queries for all entities
    const [
      totalParksCount,
      totalAttractionsCount,
      totalShowsCount,
      totalRestaurantsCount,
      queueDataCount,
      weatherDataCount,
      scheduleEntriesCount,
      restaurantLiveDataCount,
      showLiveDataCount,
      waitTimePredictionCount,
    ] = await Promise.all([
      this.parkRepository.count(),
      this.attractionRepository.count(),
      this.showRepository.count(),
      this.restaurantRepository.count(),
      this.queueDataRepository.count(),
      this.weatherDataRepository.count(),
      this.scheduleEntryRepository.count(),
      this.restaurantLiveDataRepository.count(),
      this.showLiveDataRepository.count(),
      this.waitTimePredictionRepository.count(),
    ]);

    const openParksCount = openParks.length;
    const closedParksCount = Math.max(0, totalParksCount - openParksCount);

    // 2. Find Most/Least Crowded Park (by Avg Wait)
    openParks.sort((a: any, b: any) => b.avg_wait - a.avg_wait);

    const mostCrowdedPark =
      openParks.length > 0
        ? {
          id: openParks[0].id,
          name: openParks[0].name,
          slug: openParks[0].slug,
          averageWaitTime: Math.round(openParks[0].avg_wait),
          url: buildParkUrl(openParks[0]) || `/v1/parks/${openParks[0].slug}`,
          crowdLevel: null,
        }
        : null;

    const leastCrowdedPark =
      openParks.length > 0
        ? {
          id: openParks[openParks.length - 1].id,
          name: openParks[openParks.length - 1].name,
          slug: openParks[openParks.length - 1].slug,
          averageWaitTime: Math.round(
            openParks[openParks.length - 1].avg_wait,
          ),
          url:
            buildParkUrl(openParks[openParks.length - 1]) ||
            `/v1/parks/${openParks[openParks.length - 1].slug}`,
          crowdLevel: null,
        }
        : null;

    // 3. Find Longest/Shortest Wait Ride (Global)
    // We already have latest updates in activeParksResult implicit query,
    // but we can just query the raw `queue_data` for top/bottom 1 globally.
    // Or reuse the CTE approach if we want to be consistent. Let's do a fast distinct query.

    const rideStats = await this.queueDataRepository.query(`
      SELECT DISTINCT ON (qd."attractionId")
        qd."attractionId",
        qd."waitTime",
        a.name,
        a.slug,
        p.name as "parkName",
        p.slug as "parkSlug",
        p."continentSlug",
        p."countrySlug",
        p."citySlug"
      FROM queue_data qd
      JOIN attractions a ON a.id = qd."attractionId"
      JOIN parks p ON p.id = a."parkId"
      WHERE qd.timestamp > NOW() - INTERVAL '60 minutes'
      AND qd.status = 'OPERATING'
      ORDER BY qd."attractionId", qd.timestamp DESC
    `);

    // Sort in JS
    rideStats.sort((a: any, b: any) => b.waitTime - a.waitTime);

    const longestWaitRide =
      rideStats.length > 0
        ? {
          id: rideStats[0].attractionId,
          name: rideStats[0].name,
          slug: rideStats[0].slug,
          parkName: rideStats[0].parkName,
          parkSlug: rideStats[0].parkSlug,
          waitTime: rideStats[0].waitTime,
          url: `/v1/parks/${rideStats[0].parkSlug}/attractions/${rideStats[0].slug}`,
          crowdLevel: null,
        }
        : null;

    const shortestWaitRide =
      rideStats.length > 0
        ? {
          id: rideStats[rideStats.length - 1].attractionId,
          name: rideStats[rideStats.length - 1].name,
          slug: rideStats[rideStats.length - 1].slug,
          parkName: rideStats[rideStats.length - 1].parkName,
          parkSlug: rideStats[rideStats.length - 1].parkSlug,
          waitTime: rideStats[rideStats.length - 1].waitTime,
          url: `/v1/parks/${rideStats[rideStats.length - 1].parkSlug}/attractions/${rideStats[rideStats.length - 1].slug}`,
          crowdLevel: null,
        }
        : null;

    // 4. Calculate Details for Top/Bottom Stats (Parallel)
    const [
      mostCrowdedParkDetails,
      leastCrowdedParkDetails,
      longestWaitRideDetails,
      shortestWaitRideDetails,
    ] = await Promise.all([
      // Most Crowded Park
      (async () => {
        if (!mostCrowdedPark) return null;
        try {
          const occupancy = await this.calculateParkOccupancy(
            mostCrowdedPark.id,
          );
          return {
            ...mostCrowdedPark,
            crowdLevel: this.determineCrowdLevel(occupancy.current),
          };
        } catch (_e) {
          return mostCrowdedPark;
        }
      })(),

      // Least Crowded Park
      (async () => {
        if (!leastCrowdedPark) return null;
        try {
          const occupancy = await this.calculateParkOccupancy(
            leastCrowdedPark.id,
          );
          return {
            ...leastCrowdedPark,
            crowdLevel: this.determineCrowdLevel(occupancy.current),
          };
        } catch (_e) {
          return leastCrowdedPark;
        }
      })(),

      // Longest Wait Ride
      (async () => {
        if (!longestWaitRide) return null;
        try {
          const now = new Date();
          const p90 = await this.get90thPercentileOneYear(
            longestWaitRide.id,
            now.getHours(),
            now.getDay(),
            "attraction",
          );
          const rating = this.getLoadRating(longestWaitRide.waitTime, p90);
          return {
            ...longestWaitRide,
            crowdLevel: rating.rating,
          };
        } catch (_e) {
          return longestWaitRide;
        }
      })(),

      // Shortest Wait Ride
      (async () => {
        if (!shortestWaitRide) return null;
        try {
          const now = new Date();
          const p90 = await this.get90thPercentileOneYear(
            shortestWaitRide.id,
            now.getHours(),
            now.getDay(),
            "attraction",
          );
          const rating = this.getLoadRating(shortestWaitRide.waitTime, p90);
          return {
            ...shortestWaitRide,
            crowdLevel: rating.rating,
          };
        } catch (_e) {
          return shortestWaitRide;
        }
      })(),
    ]);

    const response: GlobalStatsDto = {
      counts: {
        openParks: openParksCount,
        closedParks: closedParksCount,
        parks: totalParksCount,
        attractions: totalAttractionsCount,
        shows: totalShowsCount,
        restaurants: totalRestaurantsCount,
        queueDataRecords: queueDataCount,
        weatherDataRecords: weatherDataCount,
        scheduleEntries: scheduleEntriesCount,
        restaurantLiveDataRecords: restaurantLiveDataCount,
        showLiveDataRecords: showLiveDataCount,
        waitTimePredictions: waitTimePredictionCount,
      },
      mostCrowdedPark: mostCrowdedParkDetails as any,
      leastCrowdedPark: leastCrowdedParkDetails as any,
      longestWaitRide: longestWaitRideDetails as any,
      shortestWaitRide: shortestWaitRideDetails as any,
      lastUpdated: new Date().toISOString(),
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
   * Get rolling percentiles for a park over a time window
   * Aggregates across all attractions in the park
   */
  async getParkRollingPercentiles(
    parkId: string,
    days: number = 7,
  ): Promise<{
    p50: number;
    p90: number;
    iqr: number;
  } | null> {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);

    try {
      const result = await this.queueDataAggregateRepository
        .createQueryBuilder("agg")
        .select("percentile_cont(0.50) WITHIN GROUP (ORDER BY agg.p50)", "p50")
        .addSelect(
          "percentile_cont(0.90) WITHIN GROUP (ORDER BY agg.p90)",
          "p90",
        )
        .addSelect("AVG(agg.iqr)", "iqr")
        .where("agg.parkId = :parkId", { parkId })
        .andWhere("agg.hour >= :cutoff", { cutoff })
        .getRawOne();

      if (!result || result.p50 === null) {
        return null;
      }

      return {
        p50: Math.round(parseFloat(result.p50)),
        p90: Math.round(parseFloat(result.p90)),
        iqr: Math.round(parseFloat(result.iqr || "0")),
      };
    } catch (error) {
      this.logger.warn(
        `Failed to get park rolling percentiles for ${parkId}:`,
        error,
      );
      return null;
    }
  }

  /**
   * Get complete percentile analytics for a park (orchestrator)
   */
  async getParkPercentiles(parkId: string) {
    const [today, rolling7d, rolling30d] = await Promise.all([
      this.getParkPercentilesToday(parkId),
      this.getParkRollingPercentiles(parkId, 7),
      this.getParkRollingPercentiles(parkId, 30),
    ]);

    return {
      today: today ? { ...today, timestamp: new Date() } : null,
      rolling7d,
      rolling30d,
    };
  }

  /**
   * Get hourly percentiles for an attraction (last N hours)
   */
  async getAttractionHourlyPercentiles(
    attractionId: string,
    hours: number = 24,
  ): Promise<
    Array<{
      hour: Date;
      p50: number;
      p90: number;
      iqr: number;
    }>
  > {
    const cutoff = new Date();
    cutoff.setHours(cutoff.getHours() - hours);

    try {
      const results = await this.queueDataAggregateRepository
        .createQueryBuilder("agg")
        .select(["hour", "p50", "p90", "iqr"])
        .where("agg.attractionId = :attractionId", { attractionId })
        .andWhere("agg.hour >= :cutoff", { cutoff })
        .orderBy("hour", "DESC")
        .getRawMany();

      return results.map((r) => ({
        hour: r.hour,
        p50: Math.round(r.p50),
        p90: Math.round(r.p90),
        iqr: Math.round(r.iqr || 0),
      }));
    } catch (error) {
      this.logger.warn(
        `Failed to get hourly percentiles for ${attractionId}:`,
        error,
      );
      return [];
    }
  }

  /**
   * Get complete percentile analytics for an attraction (orchestrator)
   */
  async getAttractionPercentiles(attractionId: string) {
    const [today, hourly, rolling7d, rolling30d] = await Promise.all([
      this.getAttractionPercentilesToday(attractionId),
      this.getAttractionHourlyPercentiles(attractionId, 24),
      this.getAttractionRollingPercentiles(attractionId, 7),
      this.getAttractionRollingPercentiles(attractionId, 30),
    ]);

    return {
      today: today ? { ...today, timestamp: new Date() } : null,
      hourly,
      rolling: {
        last7d: rolling7d,
        last30d: rolling30d,
      },
    };
  }
}
