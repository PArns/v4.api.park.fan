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
import { DiscoveryService } from "../../discovery/discovery.service";

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
    @Inject(REDIS_CLIENT)
    private readonly redis: Redis,
    private readonly parksService: ParksService,
    private readonly parkIntegrationService: ParkIntegrationService,
    private readonly attractionIntegrationService: AttractionIntegrationService,
    private readonly discoveryService: DiscoveryService,
  ) {}

  // ... existing methods

  /**
   * Generic batch processor for warmup tasks
   * Handles concurrency limits and progress logging
   */
  private async processBatch<T>(
    items: T[],
    batchSize: number,
    label: string,
    processFn: (item: T) => Promise<boolean>,
  ): Promise<number> {
    let successCount = 0;

    for (let i = 0; i < items.length; i += batchSize) {
      const batch = items.slice(i, i + batchSize);

      const results = await Promise.allSettled(
        batch.map(async (item) => {
          const success = await processFn(item);
          if (success) return true;
          return false;
        }),
      );

      const batchSuccess = results.filter(
        (r) => r.status === "fulfilled" && r.value === true,
      ).length;
      successCount += batchSuccess;

      // Log progress for large sets
      if (items.length > 20) {
        const progress = Math.min(i + batchSize, items.length);
        this.logger.verbose(`[${label}] Progress: ${progress}/${items.length}`);
      }

      // Delay between batches
      if (i + batchSize < items.length) {
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }

    return successCount;
  }

  /**
   * Warm up discovery geo structure and live stats
   * Triggered during wait-times sync to clear cold start on /discovery/geo
   */
  async warmupDiscovery(): Promise<void> {
    try {
      this.logger.verbose("🔥 Warming Discovery Geo Structure...");
      const startTime = Date.now();

      // triggering getGeoStructure warms up:
      // 1. Structure Cache (24h)
      // 2. Live Stats Cache (5m) via hydrateStructure -> getLiveStats
      await this.discoveryService.getGeoStructure();

      const duration = Date.now() - startTime;
      this.logger.log(`✅ Discovery Geo Structure warmed in ${duration}ms`);
    } catch (error) {
      this.logger.error("Failed to warm Discovery Geo Structure", error);
    }
  }

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
   * Warm up cache for ALL parks (Operating + Closed)
   *
   * - OPERATING: Force refresh to get latest wait times
   * - CLOSED: Only warm if cache missing/expired (respect TTL)
   *
   * Triggered every 5 minutes by wait-times sync.
   */
  async warmupOperatingParks(): Promise<number> {
    const startTime = Date.now();
    this.logger.verbose("🔥 Starting cache warmup for ALL parks...");

    try {
      // Get all parks
      const parks = await this.parkRepository.find();

      if (parks.length === 0) {
        this.logger.warn("No parks found, skipping warmup");
        return 0;
      }

      // Get batch park status for decision making
      const parkIds = parks.map((p) => p.id);
      const statusMap = await this.parksService.getBatchParkStatus(parkIds);

      this.logger.verbose(
        `Found ${parks.length} parks to verify in cache (Smart Warmup)`,
      );

      // Warm up in batches (Smart Warmup decision logic is inside callback)
      const warmedCount = await this.processBatch(
        parkIds,
        3,
        "OperatingParks",
        async (parkId) => {
          const status = statusMap.get(parkId);
          const shouldForce = status === "OPERATING";
          return this.warmupParkCache(parkId, shouldForce);
        },
      );

      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      this.logger.log(
        `✅ Cache warmup complete: ${warmedCount}/${parks.length} parks refreshed/verified in ${duration}s`,
      );

      // Warm up global stats (async, don't block return)
      this.warmupGlobalStats().catch((err) =>
        this.logger.error("Failed to trigger global stats warmup", err),
      );

      // Warm up discovery geo (async)
      this.warmupDiscovery().catch((err) =>
        this.logger.error("Failed to trigger discovery warmup", err),
      );

      // Warm up park statistics (async)
      const operatingParkIds = Array.from(statusMap.entries())
        .filter(([_, status]) => status === "OPERATING")
        .map(([id]) => id);

      if (operatingParkIds.length > 0) {
        this.warmupParkStatistics(operatingParkIds).catch((err) =>
          this.logger.error("Failed to trigger statistics warmup", err),
        );
      }

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
        .innerJoin("schedule_entries", "schedule", "schedule.parkId = park.id")
        .where("schedule.scheduleType = :type", { type: "OPERATING" })
        .andWhere("schedule.openingTime >= :now", { now })
        .andWhere("schedule.openingTime <= :next12h", { next12h })
        .leftJoinAndSelect("park.attractions", "attractions")
        .leftJoinAndSelect("park.shows", "shows")
        .leftJoinAndSelect("park.restaurants", "restaurants")
        .getMany();

      this.logger.verbose(`Found ${upcomingParks.length} parks opening soon`);

      this.logger.verbose(`Found ${upcomingParks.length} parks opening soon`);

      const warmedCount = await this.processBatch(
        upcomingParks,
        3,
        "UpcomingParks",
        async (park) => this.warmupParkCache(park.id),
      );

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

      // Batch processing using helper
      const warmedCount = await this.processBatch(
        topAttractions,
        5,
        "TopAttractions",
        async (attraction) => this.warmupAttractionCache(attraction.id, true),
      );

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

    const analyticsService = this.parkIntegrationService["analyticsService"];

    const successCount = await this.processBatch(
      parkIds,
      5,
      "ParkOccupancy",
      async (parkId) => {
        try {
          const occupancy =
            await analyticsService.calculateParkOccupancy(parkId);
          const cacheKey = `park:occupancy:${parkId}`;
          await this.redis.setex(
            cacheKey,
            5 * 60, // 5 minutes TTL
            JSON.stringify(occupancy),
          );
          return true;
        } catch (error) {
          this.logger.warn(
            `Failed to warm occupancy for park ${parkId}`,
            error,
          );
          return false;
        }
      },
    );

    const duration = Date.now() - startTime;
    this.logger.verbose(
      `✓ Occupancy warmup complete: ${successCount}/${parkIds.length} parks in ${duration}ms`,
    );

    return successCount;
  }

  /**
   * Warm up global realtime stats (API: /v1/analytics/realtime)
   * Expensive query involving count(*) on heavy tables, so we pre-warm it.
   */
  async warmupGlobalStats(): Promise<void> {
    try {
      this.logger.verbose("🔥 Warming Global Realtime Stats...");
      const startTime = Date.now();

      const analyticsService = this.parkIntegrationService["analyticsService"]; // Access via existing service to avoid circular dependency
      if (
        analyticsService &&
        typeof analyticsService.getGlobalRealtimeStats === "function"
      ) {
        await analyticsService.getGlobalRealtimeStats();
        const duration = Date.now() - startTime;
        this.logger.log(`✅ Global Stats warmed in ${duration}ms`);
      } else {
        this.logger.warn(
          "Could not access AnalyticsService for global stats warmup",
        );
      }
    } catch (error) {
      this.logger.error("Failed to warm global stats", error);
    }
  }
  /**
   * Warm up park statistics cache for multiple parks
   */
  async warmupParkStatistics(parkIds: string[]): Promise<void> {
    if (parkIds.length === 0) return;

    try {
      this.logger.verbose(
        `🔥 Warming statistics cache for ${parkIds.length} parks...`,
      );
      const startTime = Date.now();

      const analyticsService = this.parkIntegrationService["analyticsService"];

      const successCount = await this.processBatch(
        parkIds,
        5,
        "ParkStatistics",
        async (parkId) => {
          try {
            // PERFORMANCE: Only select timezone, not entire park entity
            const park = await this.parkRepository.findOne({
              where: { id: parkId },
              select: ["id", "timezone"],
            });

            if (!park) {
              this.logger.warn(
                `Park ${parkId} not found, skipping cache warmup`,
              );
              return false;
            }

            const startTime = await analyticsService.getEffectiveStartTime(
              parkId,
              park.timezone,
            );

            await analyticsService.getParkStatistics(
              parkId,
              park.timezone,
              startTime,
            );

            return true;
          } catch (error) {
            this.logger.warn(
              `Failed to warm statistics for park ${parkId}`,
              error,
            );
            return false;
          }
        },
      );

      const duration = Date.now() - startTime;
      this.logger.verbose(
        `✅ Statistics warmup complete: ${successCount}/${parkIds.length} parks in ${duration}ms`,
      );
    } catch (error) {
      this.logger.error("Failed to warm park statistics", error);
    }
  }
}
