import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { Park } from "../../parks/entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { ParksService } from "../../parks/parks.service";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import { AttractionIntegrationService } from "../../attractions/services/attraction-integration.service";

/**
 * Cache Warmup Service
 *
 * Prepopulates Redis cache for parks and attractions to eliminate cold start delays.
 * Triggered automatically after data sync jobs (wait-times, predictions).
 *
 * Strategy:
 * - Parks: Warm up OPERATING parks or parks opening within 12h
 * - Attractions: Warm up top 100 most popular attractions (based on queue data frequency)
 * - Skip if cache is fresh (< 2 min old) to avoid redundant work
 */
@Injectable()
export class CacheWarmupService {
  private readonly logger = new Logger(CacheWarmupService.name);
  private readonly CACHE_FRESHNESS_THRESHOLD = 2 * 60; // 2 minutes in seconds

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    private readonly parksService: ParksService,
    private readonly parkIntegrationService: ParkIntegrationService,
    private readonly attractionIntegrationService: AttractionIntegrationService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Warm up cache for a single park
   *
   * @param parkId - Park ID
   * @returns true if warmed successfully, false if skipped or failed
   */
  async warmupParkCache(
    parkId: string,
    force: boolean = false,
  ): Promise<boolean> {
    try {
      const cacheKey = `park:integrated:${parkId}`;

      // Check if cache is already fresh
      if (!force) {
        const ttl = await this.redis.ttl(cacheKey);
        if (ttl > this.CACHE_FRESHNESS_THRESHOLD) {
          this.logger.verbose(
            `Cache for park ${parkId} is fresh (TTL: ${ttl}s), skipping`,
          );
          return false;
        }
      }

      // Fetch park
      const park = await this.parkRepository.findOne({
        where: { id: parkId },
        relations: ["attractions", "shows", "restaurants"],
      });

      if (!park) {
        this.logger.warn(`Park ${parkId} not found, skipping warmup`);
        return false;
      }

      // Warm up cache by calling integration service (bypass cache read if forced)
      await this.parkIntegrationService.buildIntegratedResponse(park, force);

      // this.logger.debug(
      //   `✓ Warmed cache for park: ${park.slug} (force=${force})`,
      // );
      return true;
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to warm cache for park ${parkId}: ${errorMessage}`,
      );
      return false;
    }
  }

  /**
   * Warm up cache for currently OPERATING parks
   *
   * Used after wait-times sync (every 5 minutes).
   * Typically affects 10-20 parks.
   *
   * @returns Number of parks warmed
   */
  /**
   * Warm up cache for currently OPERATING parks
   *
   * Used after wait-times sync (every 5 minutes).
   * Typically affects 10-20 parks.
   *
   * @returns Number of parks warmed
   */
  async warmupOperatingParks(): Promise<number> {
    const startTime = Date.now();
    this.logger.verbose("🔥 Starting cache warmup for OPERATING parks...");

    try {
      // Get all parks
      const parks = await this.parkRepository.find();

      if (parks.length === 0) {
        this.logger.warn("No parks found, skipping warmup");
        return 0;
      }

      // Get batch park status
      const parkIds = parks.map((p) => p.id);
      const statusMap = await this.parksService.getBatchParkStatus(parkIds);

      // Filter to only OPERATING parks
      const operatingParkIds = Array.from(statusMap.entries())
        .filter(([, status]) => status === "OPERATING")
        .map(([parkId]) => parkId);

      this.logger.verbose(
        `Found ${operatingParkIds.length}/${parks.length} OPERATING parks`,
      );

      // Warm up in batches to avoid rate limits (OpenMeteo via MLService)
      const BATCH_SIZE = 3; // Reduced from 5 to avoid 429s (Open-Meteo limit)
      let warmedCount = 0;

      for (let i = 0; i < operatingParkIds.length; i += BATCH_SIZE) {
        const batch = operatingParkIds.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map((parkId) => this.warmupParkCache(parkId, true)),
        );

        results.forEach((result) => {
          if (result.status === "fulfilled" && result.value) {
            warmedCount++;
          }
        });

        // Delay between batches to be nice to APIs
        if (i + BATCH_SIZE < operatingParkIds.length) {
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      this.logger.log(
        `✅ Cache warmup complete: ${warmedCount}/${operatingParkIds.length} parks in ${duration}s`,
      );

      return warmedCount;
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`Cache warmup failed: ${errorMessage}`);
      return 0;
    }
  }

  /**
   * Warm up cache for parks opening within next 12 hours
   *
   * Used after hourly predictions sync.
   * Important for trip planning - users look at tomorrow's data.
   * Typically affects 30-50 parks.
   *
   * @returns Number of parks warmed
   */
  async warmupUpcomingParks(): Promise<number> {
    const startTime = Date.now();
    this.logger.verbose(
      "🔥 Starting cache warmup for parks opening in next 12h...",
    );

    try {
      const now = new Date();
      const next12h = new Date(now.getTime() + 12 * 60 * 60 * 1000);

      // Find parks with opening time in next 12h
      const upcomingParks = await this.parkRepository
        .createQueryBuilder("park")
        .innerJoin("park.schedules", "schedule")
        .where("schedule.scheduleType = :type", { type: "OPERATING" })
        .andWhere("schedule.openingTime >= :now", { now })
        .andWhere("schedule.openingTime <= :next12h", { next12h })
        .leftJoinAndSelect("park.attractions", "attractions")
        .leftJoinAndSelect("park.shows", "shows")
        .leftJoinAndSelect("park.restaurants", "restaurants")
        .getMany();

      this.logger.verbose(`Found ${upcomingParks.length} parks opening soon`);

      // Warm up in batches to avoid rate limits (OpenMeteo via MLService)
      const BATCH_SIZE = 3; // Reduced from 5
      let warmedCount = 0;

      for (let i = 0; i < upcomingParks.length; i += BATCH_SIZE) {
        const batch = upcomingParks.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map((park) => this.warmupParkCache(park.id)),
        );

        results.forEach((result) => {
          if (result.status === "fulfilled" && result.value) {
            warmedCount++;
          }
        });

        // Delay between batches
        if (i + BATCH_SIZE < upcomingParks.length) {
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      this.logger.log(
        `✅ Cache warmup complete: ${warmedCount}/${upcomingParks.length} parks in ${duration}s`,
      );

      return warmedCount;
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`Cache warmup failed: ${errorMessage}`);
      return 0;
    }
  }

  /**
   * Warm up cache for a single attraction
   *
   * @param attractionId - Attraction ID
   * @returns true if warmed successfully, false if skipped or failed
   */
  async warmupAttractionCache(
    attractionId: string,
    force: boolean = false,
  ): Promise<boolean> {
    try {
      const cacheKey = `attraction:integrated:${attractionId}`;

      // Check if cache is already fresh
      if (!force) {
        const ttl = await this.redis.ttl(cacheKey);
        if (ttl > this.CACHE_FRESHNESS_THRESHOLD) {
          this.logger.verbose(
            `Cache for attraction ${attractionId} is fresh (TTL: ${ttl}s), skipping`,
          );
          return false;
        }
      }

      // Fetch attraction
      const attraction = await this.attractionRepository.findOne({
        where: { id: attractionId },
        relations: ["park"],
      });

      if (!attraction) {
        this.logger.warn(
          `Attraction ${attractionId} not found, skipping warmup`,
        );
        return false;
      }

      // Warm up cache by calling integration service
      await this.attractionIntegrationService.buildIntegratedResponse(
        attraction,
      );

      // this.logger.debug(`✓ Warmed cache for attraction: ${attraction.slug}`);
      return true;
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to warm cache for attraction ${attractionId}: ${errorMessage}`,
      );
      return false;
    }
  }

  /**
   * Warm up cache for top N most popular attractions
   *
   * Popularity based on queue data frequency in last 7 days.
   * Used after wait-times sync (every 5 minutes).
   *
   * @param limit - Number of top attractions (default: 100)
   * @returns Number of attractions warmed
   */
  async warmupTopAttractions(limit: number = 100): Promise<number> {
    const startTime = Date.now();
    this.logger.verbose(
      `🔥 Starting cache warmup for top ${limit} attractions...`,
    );

    try {
      // Query top attractions by queue data frequency (last 7 days)
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

      const topAttractions = await this.attractionRepository
        .createQueryBuilder("attraction")
        .innerJoin("attraction.queueData", "qd")
        .where("qd.timestamp > :since", { since: sevenDaysAgo })
        .groupBy("attraction.id")
        .orderBy("COUNT(qd.id)", "DESC")
        .limit(limit)
        .getMany();

      this.logger.verbose(`Found ${topAttractions.length} top attractions`);

      // Batch processing to avoid connection timeouts and rate limits
      // 100 concurrent requests can exhaust the connection pool
      const BATCH_SIZE = 5; // Reduced from 10
      let warmedCount = 0;

      for (let i = 0; i < topAttractions.length; i += BATCH_SIZE) {
        const batch = topAttractions.slice(i, i + BATCH_SIZE);

        await Promise.all(
          batch.map(async (attraction) => {
            const success = await this.warmupAttractionCache(
              attraction.id,
              true,
            );
            if (success) warmedCount++;
          }),
        );

        // Log progress every batch
        const progress = Math.min(i + BATCH_SIZE, topAttractions.length);
        this.logger.verbose(
          `Progress: ${progress}/${topAttractions.length} attractions warmed`,
        );

        // Small delay between batches to be nice to APIs
        if (i + BATCH_SIZE < topAttractions.length) {
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      this.logger.log(
        `✅ Cache warmup complete: ${warmedCount}/${topAttractions.length} attractions in ${duration}s`,
      );

      return warmedCount;
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`Cache warmup failed: ${errorMessage}`);
      return 0;
    }
  }

  /**
   * Warm up park occupancy cache for all parks
   *
   * Reuses AnalyticsService.calculateParkOccupancy for each park
   * and caches result in Redis for fast batch retrieval
   *
   * @param parkIds - Array of park IDs to warm up
   * @returns Number of parks successfully warmed
   */
  async warmupParkOccupancy(parkIds: string[]): Promise<number> {
    if (parkIds.length === 0) {
      return 0;
    }

    const startTime = Date.now();
    this.logger.verbose(
      `🔥 Warming occupancy cache for ${parkIds.length} parks...`,
    );

    let successCount = 0;
    const analyticsService = this.parkIntegrationService["analyticsService"];

    for (const parkId of parkIds) {
      try {
        const occupancy = await analyticsService.calculateParkOccupancy(parkId);
        const cacheKey = `park:occupancy:${parkId}`;
        await this.redis.setex(
          cacheKey,
          5 * 60, // 5 minutes TTL (same as wait times sync interval)
          JSON.stringify(occupancy),
        );
        successCount++;
      } catch (error) {
        this.logger.warn(`Failed to warm occupancy for park ${parkId}`, error);
      }
    }

    const duration = Date.now() - startTime;
    this.logger.verbose(
      `✓ Occupancy warmup complete: ${successCount}/${parkIds.length} parks in ${duration}ms`,
    );

    return successCount;
  }
}
