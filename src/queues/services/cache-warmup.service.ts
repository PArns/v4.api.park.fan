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
import { CalendarService } from "../../parks/services/calendar.service";
import { DiscoveryService } from "../../discovery/discovery.service";
import { SearchService } from "../../search/search.service";
import { getCurrentDateInTimezone } from "../../common/utils/date.util";
import { PopularityService } from "../../popularity/popularity.service";

/**
 * Cache Warmup Service
 *
 * Prepopulates Redis cache for parks and attractions to eliminate cold start delays.
 * Triggered automatically after data sync jobs (wait-times, predictions).
 *
 * Strategy:
 * - Parks: Warm up OPERATING parks or parks opening within 12h
 * - Calendar: Warm up calendar (current + next month) once per day for all parks (warmup-calendar-daily job)
 * - Attractions: Warm up top 100 most popular attractions (based on queue data frequency)
 * - Skip if cache is fresh (< 2 min old) to avoid redundant work
 */
@Injectable()
export class CacheWarmupService {
  private readonly logger = new Logger(CacheWarmupService.name);
  private readonly CACHE_FRESHNESS_THRESHOLD = 2 * 60; // 2 minutes in seconds
  private statsWarmupRunning = false;

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
    private readonly calendarService: CalendarService,
    private readonly discoveryService: DiscoveryService,
    private readonly searchService: SearchService,
    private readonly popularityService: PopularityService,
  ) {}

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
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }

    return successCount;
  }

  /**
   * Warm up calendar cache for one park: -1 month to +3 months (park timezone).
   * Covers typical user range: last month (e.g. recap) through 3 months ahead (planning).
   * Called from warmupCalendarForAllParks (daily warmup at 5am).
   */
  private async warmupCalendarForPark(park: Park): Promise<void> {
    try {
      const tz = park.timezone || "UTC";
      const todayStr = getCurrentDateInTimezone(tz);
      const [y, m] = todayStr.split("-").map(Number); // m = 1..12
      // From: 1st of (current - 1 month)
      let fromM = m - 1;
      let fromY = y;
      if (fromM < 1) {
        fromM += 12;
        fromY -= 1;
      }
      const fromStr = `${fromY}-${String(fromM).padStart(2, "0")}-01`;
      // To: last day of (current + 3 months)
      let endM = m + 3;
      let endY = y;
      while (endM > 12) {
        endM -= 12;
        endY += 1;
      }
      const lastDay = new Date(endY, endM, 0).getDate();
      const toStr = `${endY}-${String(endM).padStart(2, "0")}-${String(lastDay).padStart(2, "0")}`;
      const fromDate = new Date(`${fromStr}T12:00:00.000Z`);
      const toDate = new Date(`${toStr}T12:00:00.000Z`);
      await this.calendarService.buildCalendarResponse(
        park,
        fromDate,
        toDate,
        "today+tomorrow",
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.debug(`Calendar warmup skipped for ${park.slug}: ${msg}`);
    }
  }

  /**
   * Warm up calendar cache (-1 to +3 months) for all parks.
   * Called once per day by warmup-calendar-daily job (e.g. 5am), not every 5 min with park warmup.
   *
   * @returns Number of parks for which calendar was warmed (or attempted)
   */
  async warmupCalendarForAllParks(): Promise<number> {
    const startTime = Date.now();
    this.logger.verbose(
      "🔥 Starting calendar warmup for all parks (once daily)...",
    );

    try {
      // Load only IDs upfront — fetch each park with its relations inside the
      // batch callback so only 2 parks are fully loaded in memory at a time
      const parkIds = await this.parkRepository
        .createQueryBuilder("park")
        .select("park.id", "id")
        .getRawMany<{ id: string }>();

      if (parkIds.length === 0) {
        this.logger.warn("No parks found, skipping calendar warmup");
        return 0;
      }

      const warmedCount = await this.processBatch(
        parkIds,
        2,
        "CalendarWarmup",
        async ({ id }) => {
          const park = await this.parkRepository.findOne({
            where: { id },
            relations: ["influencingRegions"],
          });
          if (!park) return false;
          await this.warmupCalendarForPark(park);
          return true;
        },
      );

      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      this.logger.log(
        `✅ Calendar warmup complete: ${warmedCount}/${parkIds.length} parks in ${duration}s`,
      );
      return warmedCount;
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : String(error);
      this.logger.error(`Calendar warmup failed: ${msg}`);
      return 0;
    }
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

      // Fetch park + relations via parallel queries (avoids Cartesian product JOIN)
      // Use findBySlug which already includes all necessary relations for integrated response
      const parkBase = await this.parkRepository.findOne({
        where: { id: parkId },
        select: ["id", "slug", "continent", "country", "city"],
      });

      if (!parkBase) {
        this.logger.warn(`Park ${parkId} not found, skipping warmup`);
        return false;
      }

      // Load with full relations (shows, restaurants, attractions)
      const park = await this.parksService.findByGeographicPathWithRelations(
        parkBase.continent,
        parkBase.country,
        parkBase.city,
        parkBase.slug,
      );

      if (!park) {
        this.logger.warn(
          `Park ${parkBase.slug} could not be fully loaded, skipping warmup`,
        );
        return false;
      }

      // Warm up cache by calling integration service (bypass cache read if forced)
      await this.parkIntegrationService.buildIntegratedResponse(park, force);

      // Calendar is warmed once per day via warmup-calendar-daily job, not every 5 min
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
      // 1. Get all parks and their current status + popularity
      const [parks, popularParkIds] = await Promise.all([
        this.parkRepository.find({ select: ["id", "name"] }),
        this.popularityService.getTopParks(50),
      ]);

      if (parks.length === 0) {
        this.logger.warn("No parks found, skipping warmup");
        return 0;
      }

      // Get batch park status for decision making
      const parkIds = parks.map((p) => p.id);
      const statusMap = await this.parksService.getBatchParkStatus(parkIds);

      this.logger.verbose(
        `Found ${parks.length} parks to verify in cache (Priority: Operating -> Popular)`,
      );

      // 2. Sort Logic:
      // - Priority 1: OPERATING parks (High priority for active users)
      // - Priority 2: Popular (Hot) parks
      // - Priority 3: All others (Ensures no cold start when parks open)
      const popularSet = new Set(popularParkIds);
      const sortedParkIds = [...parkIds].sort((a, b) => {
        const statusA = statusMap.get(a) === "OPERATING" ? 0 : 1;
        const statusB = statusMap.get(b) === "OPERATING" ? 0 : 1;

        if (statusA !== statusB) return statusA - statusB;

        // If status same, check popularity
        const popA = popularSet.has(a) ? 0 : 1;
        const popB = popularSet.has(b) ? 0 : 1;

        if (popA !== popB) return popA - popB;

        return 0;
      });

      this.logger.verbose(
        `Found ${parks.length} parks to verify in cache (Full Warmup: Operating -> Popular -> Rest)`,
      );

      // Warm up in batches (Smart Warmup decision logic is inside callback)
      const warmedCount = await this.processBatch(
        sortedParkIds,
        10,
        "OperatingParks",
        async (parkId) => {
          const status = statusMap.get(parkId);
          // Force refresh for OPERATING parks to keep wait times fresh
          // Only warm others if cache expired
          const shouldForce = status === "OPERATING";
          return this.warmupParkCache(parkId, shouldForce);
        },
      );
      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      this.logger.log(
        `✅ Cache warmup complete: ${warmedCount}/${parks.length} parks refreshed/verified in ${duration}s`,
      );

      // PERFORMANCE: Execute secondary warmups sequentially to avoid DB connection contention
      // and "client.query() already executing" warnings from overlapping raw SQL queries.
      try {
        await this.warmupGlobalStats();
        await this.warmupDiscovery();
        await this.searchService.warmupSearch();

        const operatingParkIds = Array.from(statusMap.entries())
          .filter(([_, status]) => status === "OPERATING")
          .map(([id]) => id);

        if (operatingParkIds.length > 0 && !this.statsWarmupRunning) {
          this.statsWarmupRunning = true;
          try {
            await this.warmupParkStatistics(operatingParkIds);
          } finally {
            this.statsWarmupRunning = false;
          }
        }
      } catch (err) {
        this.logger.error("Secondary warmup tasks failed:", err);
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

      // Find parks with opening time in next 12h — select only IDs to avoid
      // loading thousands of attraction/show/restaurant entities into memory
      const upcomingParkIds = await this.parkRepository
        .createQueryBuilder("park")
        .select("park.id", "id")
        .innerJoin("schedule_entries", "schedule", "schedule.parkId = park.id")
        .where("schedule.scheduleType = :type", { type: "OPERATING" })
        .andWhere("schedule.openingTime >= :now", { now })
        .andWhere("schedule.openingTime <= :next12h", { next12h })
        .distinct(true)
        .getRawMany<{ id: string }>();

      this.logger.verbose(`Found ${upcomingParkIds.length} parks opening soon`);

      const warmedCount = await this.processBatch(
        upcomingParkIds,
        3,
        "UpcomingParks",
        async ({ id }) => this.warmupParkCache(id),
      );

      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      this.logger.log(
        `✅ Cache warmup complete: ${warmedCount}/${upcomingParkIds.length} parks in ${duration}s`,
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
   * Strategy: Combined popularity
   * 1. Top attractions by user traffic (PopularityService)
   * 2. Top attractions by queue data density (Database)
   *
   * @param limit - Total number of top attractions to warm (default: 200)
   * @returns Number of attractions warmed
   */
  async warmupTopAttractions(limit: number = 1000): Promise<number> {
    const startTime = Date.now();
    try {
      this.logger.verbose(
        `🔥 Starting cache warmup for top ${limit} attractions...`,
      );

      // 1. Get top attraction IDs from Redis (User traffic)
      const hotAttractionIds =
        await this.popularityService.getTopAttractions(limit);

      // 2. Get top attractions from DB (Queue density proxy)
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
      const dbTopAttractions = await this.attractionRepository
        .createQueryBuilder("attraction")
        .innerJoin("attraction.queueData", "qd")
        .where("qd.timestamp > :since", { since: sevenDaysAgo })
        .groupBy("attraction.id")
        .orderBy("COUNT(qd.id)", "DESC")
        .limit(limit)
        .getMany();

      // Merge and deduplicate
      const combinedIds = new Set([
        ...hotAttractionIds,
        ...dbTopAttractions.map((a) => a.id),
      ]);

      const targetIds = Array.from(combinedIds).slice(0, limit);

      this.logger.verbose(
        `Found ${targetIds.length} unique top attractions to warm`,
      );

      // Batch processing using helper
      const warmedCount = await this.processBatch(
        targetIds,
        25, // Increased from 15
        "TopAttractions",
        async (id) => this.warmupAttractionCache(id, true),
      );

      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      this.logger.log(
        `✅ Cache warmup complete: ${warmedCount}/${targetIds.length} attractions in ${duration}s`,
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
