import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between, LessThanOrEqual, MoreThanOrEqual } from "typeorm";
import { QueueData } from "./entities/queue-data.entity";
import { ForecastData } from "./entities/forecast-data.entity";
import {
  EntityLiveResponse,
  QueueType,
} from "../external-apis/themeparks/themeparks.types";

/**
 * Queue Data Service
 *
 * Handles storing wait times and live data for attractions.
 * Supports all 6 queue types from ThemeParks.wiki.
 *
 * Delta Strategy:
 * - Only store when waitTime changes by > 5 minutes
 * - Always store status changes (OPERATING → CLOSED, etc.)
 * - Store all virtual queue updates (return time windows)
 */
@Injectable()
export class QueueDataService {
  private readonly logger = new Logger(QueueDataService.name);

  constructor(
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
    @InjectRepository(ForecastData)
    private forecastDataRepository: Repository<ForecastData>,
  ) {}

  /**
   * Saves queue data for an attraction from ThemeParks.wiki live API response.
   *
   * @param attractionId - Our internal attraction ID (UUID)
   * @param liveData - Live data from ThemeParks.wiki API
   */
  async saveLiveData(
    attractionId: string,
    liveData: EntityLiveResponse,
  ): Promise<number> {
    let savedCount = 0;

    if (!liveData.queue) {
      // If no queue data, but status is explicitly CLOSED/DOWN/REFURBISHMENT, we should record that.
      // Otherwise we miss the "Close" signal if the API just sends status + empty queue.
      if (liveData.status && liveData.status !== "OPERATING") {
        // Force a save for STANDBY queue to record the status change
        const queueData: Partial<QueueData> = {
          attractionId,
          queueType: QueueType.STANDBY,
          status: liveData.status,
          lastUpdated: liveData.lastUpdated
            ? new Date(liveData.lastUpdated)
            : new Date(),
          waitTime: 0, // Closed = 0 wait
        };

        const shouldSave = await this.shouldSaveQueueData(
          attractionId,
          QueueType.STANDBY,
          queueData,
        );
        if (shouldSave) {
          const queueEntry = this.queueDataRepository.create(queueData);
          await this.queueDataRepository.save(queueEntry);
          return 1;
        }
      }
      // No queue data available (attraction might not have wait times)
      return savedCount;
    }

    // Process each queue type
    const queueTypes = Object.keys(liveData.queue) as QueueType[];

    for (const queueType of queueTypes) {
      const queueInfo =
        liveData.queue[queueType as keyof typeof liveData.queue];

      if (!queueInfo) continue;

      // Prepare queue data based on type
      const queueData: Partial<QueueData> = {
        attractionId,
        queueType,
        status: liveData.status,
        lastUpdated: liveData.lastUpdated
          ? new Date(liveData.lastUpdated)
          : null,
      };

      // Map fields based on queue type
      switch (queueType) {
        case QueueType.STANDBY:
        case QueueType.SINGLE_RIDER:
        case QueueType.PAID_STANDBY:
          if ("waitTime" in queueInfo) {
            queueData.waitTime = queueInfo.waitTime;
          }
          break;

        case QueueType.RETURN_TIME:
        case QueueType.PAID_RETURN_TIME:
          if ("state" in queueInfo) {
            queueData.state = queueInfo.state;
            queueData.returnStart = queueInfo.returnStart
              ? new Date(queueInfo.returnStart)
              : undefined;
            queueData.returnEnd = queueInfo.returnEnd
              ? new Date(queueInfo.returnEnd)
              : undefined;
          }
          if (
            queueType === QueueType.PAID_RETURN_TIME &&
            "price" in queueInfo
          ) {
            queueData.price = queueInfo.price;
          }
          break;

        case QueueType.BOARDING_GROUP:
          if ("allocationStatus" in queueInfo) {
            queueData.allocationStatus = queueInfo.allocationStatus;
            queueData.currentGroupStart = queueInfo.currentGroupStart;
            queueData.currentGroupEnd = queueInfo.currentGroupEnd;
            queueData.estimatedWait = queueInfo.estimatedWait;
          }
          break;
      }

      // Check if we should save (delta strategy)
      const shouldSave = await this.shouldSaveQueueData(
        attractionId,
        queueType,
        queueData,
      );

      if (shouldSave) {
        try {
          // Use create() to ensure @BeforeInsert hooks are triggered
          const queueEntry = this.queueDataRepository.create(queueData);
          await this.queueDataRepository.save(queueEntry);
          savedCount++;
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : String(error);
          this.logger.error(
            `❌ Failed to save ${queueType} queue data: ${errorMessage}`,
          );
        }
      }
    }

    return savedCount;
  }

  /**
   * Saves forecast data for an attraction from ThemeParks.wiki live API response.
   *
   * Strategy:
   * - Keep ALL forecasts (historical + future) for ML model training
   * - Update existing forecasts if prediction changes (to have correct data for the current day)
   * - Never delete old forecasts (needed to compare predicted vs actual wait times)
   *
   * Use cases:
   * - Training ML model: Compare ThemeParks.wiki forecasts against actual queue_data
   * - Forecast accuracy tracking: Measure how good external predictions are
   * - Weather correlation: Join with weather data to improve our own predictions
   *
   * @param attractionId - Our internal attraction ID (UUID)
   * @param liveData - Live data from ThemeParks.wiki API containing forecast array
   */
  async saveForecastData(
    attractionId: string,
    liveData: EntityLiveResponse,
  ): Promise<number> {
    if (!liveData.forecast || liveData.forecast.length === 0) {
      // Most attractions don't have forecast data, this is normal
      return 0;
    }

    let savedCount = 0;
    let updatedCount = 0;
    let _skippedCount = 0;

    const now = new Date();
    const historicalForecasts: string[] = [];
    const futureForecasts: string[] = [];

    for (const forecast of liveData.forecast) {
      const forecastData: Partial<ForecastData> = {
        attractionId,
        predictedTime: new Date(forecast.time),
        predictedWaitTime: forecast.waitTime,
        confidencePercentage: forecast.percentage || null,
        source: "themeparks_wiki",
      };

      // Track historical vs future forecasts
      if (forecastData.predictedTime! < now) {
        historicalForecasts.push(forecast.time);
      } else {
        futureForecasts.push(forecast.time);
      }

      try {
        // Upsert: Check if forecast for this exact time already exists
        // We keep forecasts forever for ML training, just update if prediction changes
        const existing = await this.forecastDataRepository.findOne({
          where: {
            attractionId,
            predictedTime: forecastData.predictedTime,
            source: "themeparks_wiki",
          },
        });

        if (!existing) {
          // Use create() to ensure @BeforeInsert hooks are triggered
          const forecastEntry =
            this.forecastDataRepository.create(forecastData);
          await this.forecastDataRepository.save(forecastEntry);
          savedCount++;
        } else {
          // Update if wait time prediction changed
          // This ensures we have the latest/most accurate forecast for the current day
          if (existing.predictedWaitTime !== forecastData.predictedWaitTime) {
            // ForecastData has composite PK (id + createdAt), must provide both
            await this.forecastDataRepository.update(
              { id: existing.id, createdAt: existing.createdAt },
              {
                predictedWaitTime: forecastData.predictedWaitTime,
                confidencePercentage: forecastData.confidencePercentage,
              },
            );
            updatedCount++;
          } else {
            _skippedCount++;
          }
        }
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.error(
          `Failed to save forecast for ${forecast.time}: ${errorMessage}`,
        );
      }
    }

    return savedCount + updatedCount;
  }

  /**
   * Delta strategy: Only save if data has changed significantly.
   *
   * Save when:
   * - No previous data exists
   * - Wait time changed by > 5 minutes
   * - Status changed (OPERATING → CLOSED, etc.)
   * - Virtual queue return time windows changed
   */
  private async shouldSaveQueueData(
    attractionId: string,
    queueType: QueueType,
    newData: Partial<QueueData>,
  ): Promise<boolean> {
    // Get latest entry for this attraction + queue type
    const latest = await this.queueDataRepository.findOne({
      where: { attractionId, queueType },
      order: { timestamp: "DESC" },
    });

    // No previous data → save
    if (!latest) {
      return true;
    }

    // Status changed → save
    if (latest.status !== newData.status) {
      return true;
    }

    // Wait time changed → save
    if (newData.waitTime !== undefined && latest.waitTime !== undefined) {
      if (Number(newData.waitTime) !== Number(latest.waitTime)) {
        return true;
      }
    }

    // Return time window changed → save
    if (
      queueType === QueueType.RETURN_TIME ||
      queueType === QueueType.PAID_RETURN_TIME
    ) {
      if (newData.returnStart && latest.returnStart) {
        if (
          latest.returnStart?.getTime() !== newData.returnStart?.getTime() ||
          latest.returnEnd?.getTime() !== newData.returnEnd?.getTime()
        ) {
          return true;
        }
      } else if (newData.returnStart !== latest.returnStart) {
        // One is null/undefined, the other is not -> changed
        // (handling null vs undefined equality manually if needed, but here simple mismatch is enough trigger?)
        // Actually, be careful: undefined (new) vs null (old) should be false if both mean "no value".
        const newStart = newData.returnStart ?? null;
        const oldStart = latest.returnStart ?? null;
        if (newStart !== oldStart) return true;
      }
    }

    // Boarding group allocation changed → save
    if (queueType === QueueType.BOARDING_GROUP) {
      // Normalize to null for comparison
      const newStatus = newData.allocationStatus ?? null;
      const oldStatus = latest.allocationStatus ?? null;
      if (newStatus !== oldStatus) return true;

      const newStart = newData.currentGroupStart ?? null;
      const oldStart = latest.currentGroupStart ?? null;
      if (newStart !== oldStart) return true;

      const newEnd = newData.currentGroupEnd ?? null;
      const oldEnd = latest.currentGroupEnd ?? null;
      if (newEnd !== oldEnd) return true;
    }

    // Date changed → save (ensure at least one data point per day)
    // This fixes the issue where "Closed" status persists from yesterday and we ignore today's "Closed" update
    if (latest.timestamp) {
      const latestDate = new Date(latest.timestamp);
      const currentDate = new Date();
      if (
        latestDate.getDate() !== currentDate.getDate() ||
        latestDate.getMonth() !== currentDate.getMonth() ||
        latestDate.getFullYear() !== currentDate.getFullYear()
      ) {
        return true;
      }
    }

    // No significant change
    return false;
  }

  /**
   * Find wait times for an attraction with optional date range and queue type filtering
   *
   * @param attractionId - Attraction ID
   * @param options - Query options (from, to, queueType, page, limit)
   * @returns Queue data with pagination info
   */
  async findWaitTimesByAttraction(
    attractionId: string,
    options: {
      from?: Date;
      to?: Date;
      queueType?: QueueType;
      page?: number;
      limit?: number;
    } = {},
  ): Promise<{ data: QueueData[]; total: number }> {
    const { from, to, queueType, page = 1, limit = 10 } = options;

    const whereClause: Record<string, unknown> = { attractionId };

    // Add date range filter
    if (from && to) {
      whereClause.timestamp = Between(from, to);
    } else if (from) {
      whereClause.timestamp = MoreThanOrEqual(from);
    } else if (to) {
      whereClause.timestamp = LessThanOrEqual(to);
    }

    // Add queue type filter
    if (queueType) {
      whereClause.queueType = queueType;
    }

    // Query with pagination
    const [data, total] = await this.queueDataRepository.findAndCount({
      where: whereClause,
      relations: ["attraction", "attraction.park"],
      order: { timestamp: "DESC" },
      skip: (page - 1) * limit,
      take: limit,
    });

    return { data, total };
  }

  /**
   * Find current status for an attraction (most recent queue data)
   *
   * @param attractionId - Attraction ID
   * @returns Most recent queue data for all queue types
   */
  async findCurrentStatusByAttraction(
    attractionId: string,
    maxAgeMinutes?: number,
  ): Promise<QueueData[]> {
    // Use DISTINCT ON optimization to get latest record per queueType efficiently
    // This replaces N queries (one per queue type) with a single query
    const query = this.queueDataRepository
      .createQueryBuilder("qd")
      .where("qd.attractionId = :attractionId", { attractionId })
      .distinctOn(["qd.queueType"])
      .orderBy("qd.queueType", "ASC")
      .addOrderBy("qd.timestamp", "DESC");

    if (maxAgeMinutes) {
      const cutoff = new Date(Date.now() - maxAgeMinutes * 60 * 1000);
      query.andWhere("qd.timestamp >= :cutoff", { cutoff });
    }

    return query.getMany();
  }

  /**
   * Find current status for multiple attractions (batch query)
   *
   * @param attractionIds - Array of Attraction IDs
   * @param maxAgeMinutes - Maximum age of queue data in minutes (optional)
   * @returns Map of attractionId -> QueueData[]
   */
  async findCurrentStatusByAttractionIds(
    attractionIds: string[],
    maxAgeMinutes?: number,
  ): Promise<Map<string, QueueData[]>> {
    if (attractionIds.length === 0) {
      return new Map();
    }

    // Use DISTINCT ON optimization like findCurrentStatusByPark
    const query = this.queueDataRepository
      .createQueryBuilder("qd")
      .where("qd.attractionId IN (:...attractionIds)", { attractionIds })
      .distinctOn(["qd.attractionId", "qd.queueType"])
      .orderBy("qd.attractionId", "ASC")
      .addOrderBy("qd.queueType", "ASC")
      .addOrderBy("qd.timestamp", "DESC");

    if (maxAgeMinutes) {
      const cutoff = new Date(Date.now() - maxAgeMinutes * 60 * 1000);
      query.andWhere("qd.timestamp >= :cutoff", { cutoff });
    }

    const queueData = await query.getMany();

    const result = new Map<string, QueueData[]>();
    for (const data of queueData) {
      if (!result.has(data.attractionId)) {
        result.set(data.attractionId, []);
      }
      result.get(data.attractionId)!.push(data);
    }

    return result;
  }

  /**
   * Find current status for all attractions in a park (bulk query optimization)
   *
   * This is a performance-optimized version that fetches queue data for all attractions
   * in a single query instead of N queries (one per attraction).
   *
   * Uses PostgreSQL DISTINCT ON to get latest record per (attractionId, queueType) efficiently.
   * Requires composite index on (attractionId, queueType, timestamp) for optimal performance.
   *
   * @param parkId - Park ID
   * @returns Map of attractionId -> QueueData[] (current status for all queue types)
   */
  async findCurrentStatusByPark(
    parkId: string,
    maxAgeMinutes?: number,
  ): Promise<Map<string, QueueData[]>> {
    // Use DISTINCT ON to get latest timestamp for each (attractionId, queueType) combination
    // This replaces the O(n²) correlated subquery with a single index scan
    const query = this.queueDataRepository
      .createQueryBuilder("qd")
      .innerJoin("qd.attraction", "attraction")
      .where("attraction.parkId = :parkId", { parkId })
      .distinctOn(["qd.attractionId", "qd.queueType"])
      .orderBy("qd.attractionId", "ASC")
      .addOrderBy("qd.queueType", "ASC")
      .addOrderBy("qd.timestamp", "DESC"); // Latest first within each group

    if (maxAgeMinutes) {
      const cutoff = new Date(Date.now() - maxAgeMinutes * 60 * 1000);
      query.andWhere("qd.timestamp >= :cutoff", { cutoff });
    }

    const queueData = await query.getMany();

    // Group by attractionId
    const result = new Map<string, QueueData[]>();
    for (const data of queueData) {
      if (!result.has(data.attractionId)) {
        result.set(data.attractionId, []);
      }
      result.get(data.attractionId)!.push(data);
    }

    return result;
  }

  /**
   * Get prioritized queue data for attractions (STANDBY preferred, fallback to others)
   *
   * Returns one QueueData per attraction, prioritizing STANDBY queue type.
   * Falls back to other queue types if STANDBY not available.
   * This ensures rides with only virtual queues (RETURN_TIME, BOARDING_GROUP, etc.) are counted.
   *
   * @param parkId - Park ID
   * @param maxAgeMinutes - Maximum age of queue data in minutes (optional)
   * @returns Map of attractionId -> QueueData (single prioritized queue per attraction)
   */
  async findPrioritizedStatusByPark(
    parkId: string,
    maxAgeMinutes?: number,
  ): Promise<Map<string, QueueData>> {
    // Get all queue types
    const allQueues = await this.findCurrentStatusByPark(parkId, maxAgeMinutes);

    const result = new Map<string, QueueData>();

    for (const [attractionId, queues] of allQueues.entries()) {
      if (queues.length === 0) continue;

      // Prioritize STANDBY, fallback to first available queue type
      const standby = queues.find((q) => q.queueType === QueueType.STANDBY);
      result.set(attractionId, standby || queues[0]);
    }

    return result;
  }

  /**
   * Find forecasts for an attraction
   *
   * @param attractionId - Attraction ID
   * @param hours - Number of hours ahead to fetch (default: 24)
   * @returns Forecast data
   */
  async findForecastsByAttraction(
    attractionId: string,
    hours: number = 24,
  ): Promise<ForecastData[]> {
    const now = new Date();
    const futureTime = new Date(now.getTime() + hours * 60 * 60 * 1000);

    return this.forecastDataRepository.find({
      where: {
        attractionId,
        predictedTime: Between(now, futureTime),
      },
      relations: ["attraction", "attraction.park"],
      order: { predictedTime: "ASC" },
    });
  }

  /**
   * Find current wait times for all attractions in a park
   *
   * @param parkId - Park ID
   * @param queueType - Optional queue type filter
   * @returns Most recent queue data for each attraction in the park
   */
  async findWaitTimesByPark(
    parkId: string,
    queueType?: QueueType,
  ): Promise<QueueData[]> {
    const queryBuilder = this.queueDataRepository
      .createQueryBuilder("queue_data")
      .innerJoinAndSelect("queue_data.attraction", "attraction")
      .innerJoinAndSelect("attraction.park", "park")
      .where("park.id = :parkId", { parkId });

    if (queueType) {
      queryBuilder.andWhere("queue_data.queueType = :queueType", { queueType });
    }

    // Get the most recent entry for each attraction
    // This is a bit complex with TypeORM, so we'll use a subquery
    queryBuilder.andWhere(
      `queue_data.timestamp = (
        SELECT MAX(qd2.timestamp)
        FROM queue_data qd2
        INNER JOIN attractions a2 ON qd2."attractionId" = a2.id
        WHERE a2."parkId" = :parkId
          AND qd2."attractionId" = queue_data."attractionId"
          ${queueType ? 'AND qd2."queueType" = :queueType' : ""}
      )`,
    );

    queryBuilder.orderBy("attraction.name", "ASC");

    return queryBuilder.getMany();
  }
  /**
   * Find forecasts for all attractions in a park
   *
   * @param parkId - Park ID
   * @param hours - Number of hours ahead to fetch (default: 24)
   * @returns Forecast data grouped by attraction
   */
  async findForecastsByPark(
    parkId: string,
    hours: number = 24,
  ): Promise<ForecastData[]> {
    const now = new Date();
    const futureTime = new Date(now.getTime() + hours * 60 * 60 * 1000);

    return this.forecastDataRepository
      .createQueryBuilder("forecast")
      .innerJoinAndSelect("forecast.attraction", "attraction")
      .innerJoinAndSelect("attraction.park", "park")
      .where("park.id = :parkId", { parkId })
      .andWhere("forecast.predictedTime BETWEEN :now AND :futureTime", {
        now,
        futureTime,
      })
      .orderBy("forecast.predictedTime", "ASC")
      .getMany();
  }
}
