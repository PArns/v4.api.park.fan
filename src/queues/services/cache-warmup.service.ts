import {
  Injectable,
  Logger,
  Inject,
  OnApplicationBootstrap,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { CacheKeys } from "../../common/cache/cache-keys";
import { Repository } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { Park } from "../../parks/entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { ParksService } from "../../parks/parks.service";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import { AttractionIntegrationService } from "../../attractions/services/attraction-integration.service";
import { CalendarService } from "../../parks/services/calendar.service";
import { BestDaysService } from "../../parks/services/best-days.service";
import { RevalidationService } from "../../common/revalidation/revalidation.service";
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
export class CacheWarmupService implements OnApplicationBootstrap {
  private readonly logger = new Logger(CacheWarmupService.name);
  private readonly CACHE_FRESHNESS_THRESHOLD = 2 * 60; // 2 minutes in seconds
  // Keep warmup-built calendar month caches alive until the next 12h warmup.
  // buildCalendarResponse caches them at CALENDAR_CACHE_TTL (15–30 min), which
  // expires ~11.5h before the next warmup — so the best-days widget then hits a
  // cold rebuild. 13h spans the 12h cadence with a buffer.
  private readonly WARMUP_MONTH_TTL = 13 * 60 * 60;
  // Startup warmup pacing — gentle on the cold, just-restarted postgres so the
  // top-parks warmup doesn't saturate the connection pool on boot (each park's
  // calendar build fans out into many parallel queries).
  private readonly STARTUP_WARMUP_DELAY_MS = 5000; // settle before first batch
  private readonly STARTUP_WARMUP_BATCH_SIZE = 3; // parks built concurrently
  private readonly STARTUP_WARMUP_BATCH_DELAY_MS = 1000; // gap between batches
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
    private readonly bestDaysService: BestDaysService,
    private readonly revalidationService: RevalidationService,
    private readonly discoveryService: DiscoveryService,
    private readonly searchService: SearchService,
    private readonly popularityService: PopularityService,
  ) {}

  /**
   * Triggered on application startup.
   * Warms up most popular parks immediately to avoid cold-start latency.
   */
  async onApplicationBootstrap(): Promise<void> {
    this.logger.log(
      "🚀 Application started. Triggering initial cache warmup...",
    );
    // Trigger in background to not block startup
    this.warmupTopParksOnStartup().catch((err) =>
      this.logger.error("Startup warmup failed", err),
    );
  }

  /**
   * Specifically warms up the top 20 most popular parks.
   */
  private async warmupTopParksOnStartup(): Promise<void> {
    try {
      const topParkIds = await this.popularityService.getTopParks(20);
      if (topParkIds.length === 0) return;

      // Let the just-restarted postgres settle first. On a fresh container its
      // buffer cache is empty AND TypeORM's connection pool is cold, while boot
      // also fires the fuzzy-index ensure + search-index load. Warming straight
      // away piled onto that and saturated the 30-conn pool — trivial queries
      // then queued for tens of seconds (post-deploy cold-start spike). A short
      // delay lets the boot DB work finish and the pool establish.
      await new Promise((r) => setTimeout(r, this.STARTUP_WARMUP_DELAY_MS));

      this.logger.log(
        `🔥 Warming up top ${topParkIds.length} parks (staggered, cold-start friendly)...`,
      );
      // Priority warmup, but NOT forced: Redis persists across a deploy, so a park whose
      // integrated cache is still fresh (TTL > 2min) is skipped instead of re-fetched. This
      // is what stops a redeploy from re-warming everything and spiking DB/ML load — only
      // genuinely-cold parks get rebuilt.
      // Low concurrency + inter-batch delay: each park's calendar build fans out into many
      // parallel queries, so a big batch against the cold DB exhausts the pool. Smaller
      // batches keep headroom for organic traffic and the boot-time index/search work.
      const warmed = await this.processBatch(
        topParkIds,
        this.STARTUP_WARMUP_BATCH_SIZE,
        "StartupWarmup",
        async (id) => this.warmupParkCache(id, false, true),
        this.STARTUP_WARMUP_BATCH_DELAY_MS,
      );
      this.logger.log(
        `✅ Initial startup warmup complete. ${warmed} parks ready.`,
      );
    } catch (_err) {
      this.logger.warn("Initial warmup failed (likely Redis not ready yet)");
    }
  }

  /**
   * Generic batch processor for warmup tasks
   * Handles concurrency limits and progress logging
   */
  private async processBatch<T>(
    items: T[],
    batchSize: number,
    label: string,
    processFn: (item: T) => Promise<boolean>,
    delayMs: number = 1000,
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
      if (delayMs > 0 && i + batchSize < items.length) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }

    return successCount;
  }

  /**
   * Warm up calendar cache for one park: -1 month to +3 months (park timezone).
   * Covers typical user range: last month (e.g. recap) through 3 months ahead (planning).
   * Called from warmupCalendarForAllParks (background warmup every 12h).
   *
   * @param force When true, evict the daily serving-prediction cache and the calendar
   *   month caches first, so the rebuild regenerates fresh predictions instead of just
   *   reading the still-warm cache. This is how the 12h background refresh actually
   *   refreshes the data (weather/model) while keeping users off the ~15s cold path.
   * @returns the park slug when the best-days snapshot was materialized (for batched
   *   frontend revalidation), or null on failure / when the calendar build was skipped.
   */
  private async warmupCalendarForPark(
    park: Park,
    force: boolean = false,
  ): Promise<string | null> {
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

      // The "none" month-cache keys for every month in [-1, +3] — the variant the
      // FE calendar/best-days widget reads (the proxy forwards includeHourly=none).
      const monthKeys: string[] = [];
      for (let mm = fromM, yy = fromY; ; ) {
        monthKeys.push(
          CacheKeys.calendarMonth(
            park.id,
            `${yy}-${String(mm).padStart(2, "0")}`,
            "none",
          ),
        );
        if (yy === endY && mm === endM) break;
        mm += 1;
        if (mm > 12) {
          mm = 1;
          yy += 1;
        }
      }

      if (force) {
        // Evict the cold-path daily ML cache + the calendar month caches in range so the
        // rebuild below regenerates fresh (otherwise buildCalendarResponse just returns
        // the still-warm month cache and the 12h refresh would be a no-op).
        // The yearly ML cache is intentionally NOT evicted: only the read path
        // (getParkPredictionsYearly) rebuilds it, so evicting it here just left
        // it cold until the next visitor paid the ~15s cold path. It refreshes
        // via its own TTL instead.
        await this.redis
          .del(
            CacheKeys.mlParkPredictions(park.id, "daily", todayStr),
            ...monthKeys,
          )
          .catch(() => undefined);
      }

      // Warm the SAME variant the frontend park page requests (includeHourly="none").
      // The month-cache key is `calendar:month:{parkId}:{ym}:{includeHourly}`, so warming
      // "today+tomorrow" populated a key the FE never reads → every real calendar request
      // missed the warmed cache and rebuilt from cold (incl. the ~15s cold daily ML call).
      //
      // BEST-EFFORT + ISOLATED: this warms the −1..+3-month calendar GRID (a perf nicety).
      // It must NEVER block the best-days precompute below — that (its own 90-day build)
      // is what the user-facing "Prognose heute" reads. Until 2026-07-17 both lived in one
      // try, so a grid-build throw (the ~150-day window trips a limit the ≤90-day read path
      // never hits) skipped precompute for EVERY park → all /best-days snapshots empty.
      // Logged at warn (not debug) so the underlying grid-build failure stays visible.
      try {
        await this.calendarService.buildCalendarResponse(
          park,
          fromDate,
          toDate,
          "none",
        );

        // Keep the freshly-warmed month caches alive until the next 12h warmup.
        // buildCalendarResponse stores them at CALENDAR_CACHE_TTL (15–30 min), which
        // would expire ~11.5h before the next warmup and leave the best-days widget's
        // far month (current+2) cold for almost the whole cycle. expire() is a no-op
        // for any month key that wasn't (re)built, so this never resurrects nothing.
        await Promise.all(
          monthKeys.map((k) => this.redis.expire(k, this.WARMUP_MONTH_TTL)),
        ).catch(() => undefined);
      } catch (gridErr) {
        const gm = gridErr instanceof Error ? gridErr.message : String(gridErr);
        this.logger.warn(`Calendar grid warm failed for ${park.slug}: ${gm}`);
      }

      // Materialize the lean best-days snapshot (today → +90d) — INDEPENDENT of the grid
      // warm above (does its own 90-day build), so it runs even when the grid build throws.
      // This is what lets /best-days serve a single Redis GET instead of the cold path.
      return this.bestDaysService.precomputeForPark(park);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.warn(`Calendar warmup skipped for ${park.slug}: ${msg}`);
      return null;
    }
  }

  /**
   * Warm up calendar cache (-1 to +3 months) for all parks, force-refreshing the daily
   * serving predictions. Called every 12h (08:00 + 20:00 UTC) by the warmup-calendar-daily
   * job — not every 5 min with park warmup — so the cold ~15s daily-ML cost is absorbed by
   * the background instead of by the first visitor, while data stays fresh (weather/model).
   *
   * @returns Number of parks for which calendar was warmed (or attempted)
   */
  async warmupCalendarForAllParks(): Promise<number> {
    const startTime = Date.now();
    this.logger.verbose(
      "🔥 Starting calendar warmup for all parks (every 12h, force-refresh)...",
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

      // Serial (batch 1) with a 2.5s pause: a single force-refreshed park build is
      // already heavy (5 months of per-ride-day CTEs over queue_data + a cold daily
      // ML call), and two of them in parallel saturated disk IO — every concurrent
      // query stalled uniformly for 24-48s in the 08:00/20:00 warmup windows
      // (slow-query log 2026-06-11). Serializing halves the peak IO pressure and
      // the pause lets queued user queries drain between parks; the only cost is
      // wall time on a 2×/day background job.
      const revalidatedSlugs: string[] = [];
      const warmedCount = await this.processBatch(
        parkIds,
        1,
        "CalendarWarmup",
        async ({ id }) => {
          const park = await this.parkRepository.findOne({
            where: { id },
            relations: ["influencingRegions"],
          });
          if (!park) return false;
          const slug = await this.warmupCalendarForPark(park, true);
          if (slug) revalidatedSlugs.push(slug);
          return true;
        },
        2500,
      );

      // One batched revalidation for every park whose best-days snapshot was just
      // refreshed — the frontend drops its (day-long) best-days cache immediately
      // instead of waiting out the TTL. No-op unless the webhook is configured.
      if (revalidatedSlugs.length > 0) {
        await this.revalidationService
          .revalidateBestDays(revalidatedSlugs)
          .catch((err) =>
            this.logger.warn(
              `Best-days revalidation failed: ${
                err instanceof Error ? err.message : String(err)
              }`,
            ),
          );
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      this.logger.log(
        `✅ Calendar warmup complete: ${warmedCount}/${parkIds.length} parks in ${duration}s (best-days: ${revalidatedSlugs.length})`,
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
    includeCalendar: boolean = false,
  ): Promise<boolean> {
    try {
      const cacheKey = CacheKeys.parkIntegrated(parkId);

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

      // Fetch park with its destination relation directly via ID
      const parkBase = await this.parksService.findById(parkId);

      if (!parkBase) {
        this.logger.warn(`Park ${parkId} not found, skipping warmup`);
        return false;
      }

      // Load with full relations (shows, restaurants, attractions) using efficient parallel queries
      const park = await this.parksService.loadParkRelations(parkBase);

      if (!park) {
        this.logger.warn(
          `Park ${parkBase.slug} relations could not be loaded, skipping warmup`,
        );
        return false;
      }

      // Warm up cache by calling integration service (bypass cache read if forced).
      // countHit=false: warmup must not pollute the popularity ranking.
      await this.parkIntegrationService.buildIntegratedResponse(
        park,
        force,
        false,
      );

      if (includeCalendar) {
        await this.warmupCalendarForPark(park);
      }
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
      // - Combined Priority: Popular OPERATING parks > Other OPERATING parks > Popular CLOSED parks > Rest
      const popularSet = new Set(popularParkIds);
      const sortedParkIds = [...parkIds].sort((a, b) => {
        const isOpA = statusMap.get(a) === "OPERATING";
        const isOpB = statusMap.get(b) === "OPERATING";
        const isPopA = popularSet.has(a);
        const isPopB = popularSet.has(b);

        // Weighting: Operating = 2 points, Popular = 1 point
        const scoreA = (isOpA ? 2 : 0) + (isPopA ? 1 : 0);
        const scoreB = (isOpB ? 2 : 0) + (isPopB ? 1 : 0);

        if (scoreA !== scoreB) return scoreB - scoreA; // Descending by score

        return 0;
      });

      this.logger.verbose(
        `Found ${parks.length} parks to verify in cache (Full Warmup: Operating -> Popular -> Rest)`,
      );

      // Warm up in batches (Smart Warmup decision logic is inside callback).
      // Batch size 5 (not 10): each park warmup fires several heavy queue_data queries
      // (status + occupancy + statistics), so 10 parks × that = ~30+ concurrent heavy scans,
      // which periodically saturated the DB (search/etc. stalled for seconds). 5 halves the
      // peak; the 1s inter-batch delay (processBatch default) further spreads the load.
      const warmedCount = await this.processBatch(
        sortedParkIds,
        5,
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
      const cacheKey = CacheKeys.attractionIntegrated(attractionId);

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
          const cacheKey = CacheKeys.parkOccupancy(parkId);
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
