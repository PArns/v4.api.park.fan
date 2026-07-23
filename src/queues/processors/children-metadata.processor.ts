import { Processor, Process, InjectQueue } from "@nestjs/bull";
import { CacheKeys } from "../../common/cache/cache-keys";
import { Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Job, Queue } from "bull";
import { AttractionsService } from "../../attractions/attractions.service";
import { ShowsService } from "../../shows/shows.service";
import { RestaurantsService } from "../../restaurants/restaurants.service";
import { ParksService } from "../../parks/parks.service";
import { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../../external-apis/themeparks/themeparks.mapper";
import { EntityResponse } from "../../external-apis/themeparks/themeparks.types";
import { generateSlug, generateUniqueSlug } from "../../common/utils/slug.util";
import { MANUAL_ATTRACTION_METADATA } from "../../attractions/data/manual-attraction-metadata";
import { extractQueueTimesNumericId } from "../../common/utils/external-id.util";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { QueueTimesDataSource } from "../../external-apis/queue-times/queue-times-data-source";
import { THEMEPARKS_EXCLUSIONS } from "../../external-apis/themeparks/themeparks.exclusions";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RevalidationService } from "../../common/revalidation/revalidation.service";

/**
 * Children Metadata Processor (Combined)
 *
 * OPTIMIZATION: Instead of 3 separate processors calling getEntityChildren(),
 * this processor calls it ONCE per park and syncs ALL entity types:
 * - Attractions
 * - Shows
 * - Restaurants
 *
 * Request Reduction: 315 requests → 105 requests (67% reduction!)
 *
 * Phase 6.2: Performance Optimization
 */
@Processor("children-metadata")
export class ChildrenMetadataProcessor {
  private readonly logger = new Logger(ChildrenMetadataProcessor.name);

  constructor(
    private attractionsService: AttractionsService,
    private showsService: ShowsService,
    private restaurantsService: RestaurantsService,
    private parksService: ParksService,
    private themeParksClient: ThemeParksClient,
    private themeParksMapper: ThemeParksMapper,
    private qtSource: QueueTimesDataSource,
    @InjectRepository(ExternalEntityMapping)
    private mappingRepository: Repository<ExternalEntityMapping>,
    @InjectQueue("entity-mappings")
    private entityMappingsQueue: Queue,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private revalidationService: RevalidationService,
  ) {}

  @Process("fetch-all-children")
  async handleFetchChildren(_job: Job): Promise<void> {
    this.logger.log(
      "🎢 Starting COMBINED children metadata sync (Attractions + Shows + Restaurants)...",
    );

    try {
      // Ensure parks are synced first
      let parks = await this.parksService.findAll();

      if (parks.length === 0) {
        this.logger.warn("No parks found. Syncing parks first...");
        await this.parksService.syncParks();
        parks = await this.parksService.findAll();
      }

      const totalParks = parks.length;
      let syncedAttractions = 0;
      let syncedShows = 0;
      let syncedRestaurants = 0;

      this.logger.log(`📊 Total parks to process: ${totalParks}`);

      // Process parks in batches to avoid rate limiting
      const BATCH_SIZE = 10;
      const BATCH_DELAY_MS = 2000; // 2 seconds between batches

      for (let i = 0; i < parks.length; i += BATCH_SIZE) {
        const batch = parks.slice(i, Math.min(i + BATCH_SIZE, parks.length));
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(parks.length / BATCH_SIZE);

        this.logger.log(
          `📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} parks)...`,
        );

        // Process batch in parallel
        const batchResults = await Promise.all(
          batch.map(async (park, index) => {
            const parkIndex = i + index + 1;

            // Fallback: If no Wiki ID, try Queue-Times
            if (!park.wikiEntityId) {
              if (park.queueTimesEntityId) {
                // Fetch from Queue-Times
                try {
                  const qtEntities = await this.qtSource.fetchParkEntities(
                    park.queueTimesEntityId,
                  );
                  let qtAttractions = 0;

                  for (const entity of qtEntities) {
                    if (entity.entityType === "ATTRACTION") {
                      // Map QT entity to internal entity structure manually or via helper
                      // Since QT data is thinner, we use a simplified sync
                      await this.syncQtAttraction(entity, park.id);
                      qtAttractions++;
                    }
                  }
                  return {
                    attractions: qtAttractions,
                    shows: 0,
                    restaurants: 0,
                  };
                } catch (e) {
                  this.logger.error(
                    `Failed to fetch from QT for ${park.name}: ${e}`,
                  );
                  return { attractions: 0, shows: 0, restaurants: 0 };
                }
              }
              return { attractions: 0, shows: 0, restaurants: 0 };
            }

            try {
              // SINGLE API CALL for all children using explicit Wiki ID
              const childrenResponse =
                await this.themeParksClient.getEntityChildren(
                  park.wikiEntityId,
                );

              // Filter by entity type and EXCLUSIONS
              const attractions = childrenResponse.children.filter(
                (child) =>
                  child.entityType === "ATTRACTION" &&
                  !THEMEPARKS_EXCLUSIONS.includes(child.id),
              );
              const shows = childrenResponse.children.filter(
                (child) =>
                  child.entityType === "SHOW" &&
                  !THEMEPARKS_EXCLUSIONS.includes(child.id),
              );
              const restaurants = childrenResponse.children.filter(
                (child) =>
                  child.entityType === "RESTAURANT" &&
                  !THEMEPARKS_EXCLUSIONS.includes(child.id),
              );

              let parkAttractions = 0;
              let parkShows = 0;
              let parkRestaurants = 0;

              // Sync Attractions
              for (const attractionEntity of attractions) {
                await this.syncAttraction(attractionEntity, park.id);
                parkAttractions++;
              }

              // Sync Shows
              for (const showEntity of shows) {
                await this.syncShow(showEntity, park.id);
                parkShows++;
              }

              // Sync Restaurants
              for (const restaurantEntity of restaurants) {
                await this.syncRestaurant(restaurantEntity, park.id);
                parkRestaurants++;
              }

              // Phase 6.6.3: Queue Entity Mapping Job (to match with Queue-Times)
              try {
                await this.entityMappingsQueue.add(
                  "sync-park-mappings",
                  {
                    parkId: park.id,
                  },
                  {
                    removeOnComplete: true,
                    attempts: 3,
                  },
                );
              } catch (e) {
                this.logger.error(
                  `Failed to queue mapping job for ${park.name}: ${e}`,
                );
              }

              // Invalidate integrated park cache to ensure new data (shows/restaurants) is visible immediately
              try {
                await this.redis.del(CacheKeys.parkIntegrated(park.id));
                // this.logger.debug(
                //   `🧹 Invalidated integrated cache for ${park.name} after sync`,
                // );
              } catch (e) {
                this.logger.warn(
                  `Failed to invalidate cache for ${park.name}: ${e}`,
                );
              }

              return {
                attractions: parkAttractions,
                shows: parkShows,
                restaurants: parkRestaurants,
              };
            } catch (error) {
              this.logger.error(
                `❌ [${parkIndex}/${totalParks}] Failed to sync ${park.name}:`,
                error,
              );
              return { attractions: 0, shows: 0, restaurants: 0 };
            }
          }),
        );

        // Aggregate batch results and show progress
        batchResults.forEach((result) => {
          syncedAttractions += result.attractions;
          syncedShows += result.shows;
          syncedRestaurants += result.restaurants;
        });

        // Log progress after each batch
        const processed = Math.min(i + BATCH_SIZE, parks.length);
        const percent = Math.round((processed / totalParks) * 100);
        this.logger.log(
          `Progress: ${processed}/${totalParks} (${percent}%) - ` +
            `${syncedAttractions} attractions, ${syncedShows} shows, ${syncedRestaurants} restaurants`,
        );

        // Delay between batches (except for last batch)
        if (i + BATCH_SIZE < parks.length) {
          // this.logger.verbose(
          //   `⏸️  Pausing ${BATCH_DELAY_MS}ms before next batch...`,
          // );
          await this.sleep(BATCH_DELAY_MS);
        }
      }

      this.logger.log("🎉 Combined children metadata sync complete!");
      this.logger.log(`📊 Final Stats:`);
      this.logger.log(`   - Attractions: ${syncedAttractions}`);
      this.logger.log(`   - Shows: ${syncedShows}`);
      this.logger.log(`   - Restaurants: ${syncedRestaurants}`);
      this.logger.log(
        `   - Total Children: ${syncedAttractions + syncedShows + syncedRestaurants}`,
      );
      this.logger.log(
        `   - API Requests: ${totalParks} (vs. ${totalParks * 3} with old approach)`,
      );
      this.logger.log(
        `   - Request Reduction: ${Math.round(((totalParks * 3 - totalParks) / (totalParks * 3)) * 100)}%`,
      );
    } catch (error) {
      this.logger.error("❌ Combined children metadata sync failed", error);
      throw error; // Bull will retry
    }
  }

  /**
   * Attraction detail sync: minimum rider height + manual metadata overrides.
   *
   * The /children bulk endpoint used by fetch-all-children does NOT carry
   * `minimumHeight` — only the per-entity document (GET /v1/entity/{id}) does.
   * Height restrictions change rarely, so this runs as its own low-frequency
   * job instead of bloating the daily children sync with ~5k extra requests.
   *
   * Afterwards MANUAL_ATTRACTION_METADATA is applied:
   * - rcdbId is always taken from the seed (no upstream source exists)
   * - minimumHeightCm only fills attractions the wiki left at NULL, so the
   *   upstream value wins whenever the parks' own apps publish one
   */
  @Process("sync-attraction-details")
  async handleSyncAttractionDetails(_job: Job): Promise<void> {
    this.logger.log("📏 Starting attraction detail sync (minimumHeight)...");

    const repo = this.attractionsService.getRepository();
    const attractions = await repo.find({
      select: [
        "id",
        "externalId",
        "minimumHeight",
        "maximumHeight",
        "mayGetWet",
      ],
    });

    // Only ThemeParks.wiki entities have per-entity documents (UUID ids);
    // Queue-Times-only attractions (e.g. "qt-ride-8") are skipped.
    const UUID_RE =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    const wikiAttractions = attractions.filter((a) =>
      UUID_RE.test(a.externalId),
    );

    this.logger.log(
      `📊 ${wikiAttractions.length}/${attractions.length} attractions have wiki entity documents`,
    );

    // Deliberately slow: ~4 requests/s stays under the wiki's per-IP rate
    // limit (the first run at 10-parallel/250ms tripped a 429 after ~1k
    // requests and the distributed block failed everything after it).
    const BATCH_SIZE = 4;
    const BATCH_DELAY_MS = 1000;
    const RATE_LIMIT_RE = /blocked for (\d+)s/;
    const MAX_ATTEMPTS = 5;
    let updated = 0;
    let failed = 0;

    for (let i = 0; i < wikiAttractions.length; i += BATCH_SIZE) {
      const batch = wikiAttractions.slice(i, i + BATCH_SIZE);
      await Promise.all(
        batch.map(async (attraction) => {
          for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
              const entity = await this.themeParksClient.getEntity(
                attraction.externalId,
              );
              const toCm = (v: unknown): number | null =>
                typeof v === "number" && v > 0 ? Math.round(v) : null;
              const minHeight = toCm(entity.minimumHeight);
              const maxHeight = toCm(entity.maximumHeight);
              const mayGetWet =
                typeof entity.mayGetWet === "boolean" ? entity.mayGetWet : null;

              const update: Partial<{
                minimumHeight: number;
                maximumHeight: number;
                mayGetWet: boolean;
              }> = {};
              if (minHeight !== null && minHeight !== attraction.minimumHeight)
                update.minimumHeight = minHeight;
              if (maxHeight !== null && maxHeight !== attraction.maximumHeight)
                update.maximumHeight = maxHeight;
              if (mayGetWet !== null && mayGetWet !== attraction.mayGetWet)
                update.mayGetWet = mayGetWet;

              if (Object.keys(update).length > 0) {
                await repo.update(attraction.id, update);
                updated++;
              }
              return;
            } catch (e) {
              // The client surfaces 429s as "... (blocked for Xs)" — wait out
              // the distributed block and retry instead of counting a failure.
              const blockMatch =
                e instanceof Error ? e.message.match(RATE_LIMIT_RE) : null;
              if (blockMatch && attempt < MAX_ATTEMPTS) {
                const waitS = Math.min(parseInt(blockMatch[1], 10) || 15, 120);
                await this.sleep((waitS + 1) * 1000);
                continue;
              }
              failed++; // individual entity failures shouldn't kill the run
              return;
            }
          }
        }),
      );

      if (i % 500 === 0 && i > 0) {
        this.logger.log(
          `Progress: ${i}/${wikiAttractions.length} (${updated} updated, ${failed} failed)`,
        );
      }
      if (i + BATCH_SIZE < wikiAttractions.length) {
        await this.sleep(BATCH_DELAY_MS);
      }
    }

    this.logger.log(
      `📏 Wiki detail sync done: ${updated} heights updated, ${failed} failed`,
    );

    await this.applyManualAttractionMetadata();

    // The frontend caches the park/attraction structure payloads for a day
    // (Vercel Data Cache, tags 'parks'/'attractions') — bust them so the new
    // metadata (heights, RCDB ids) shows up without waiting out the TTL.
    await this.revalidationService.revalidateTags(["parks", "attractions"]);

    this.logger.log("🎉 Attraction detail sync complete!");
  }

  /**
   * Apply MANUAL_ATTRACTION_METADATA (RCDB ids from Wikidata P2751/CC0 and
   * curated minimum heights for parks without upstream height data).
   */
  private async applyManualAttractionMetadata(): Promise<void> {
    const repo = this.attractionsService.getRepository();
    let rcdbApplied = 0;
    let heightsApplied = 0;

    for (const entry of MANUAL_ATTRACTION_METADATA) {
      const attraction = await repo
        .createQueryBuilder("attraction")
        .innerJoin("attraction.park", "park")
        .where("park.citySlug = :citySlug", { citySlug: entry.citySlug })
        .andWhere("park.slug = :parkSlug", { parkSlug: entry.parkSlug })
        .andWhere("attraction.slug = :attractionSlug", {
          attractionSlug: entry.attractionSlug,
        })
        .select([
          "attraction.id",
          "attraction.minimumHeight",
          "attraction.rcdbId",
        ])
        .getOne();

      if (!attraction) continue; // slugs drift as parks rename rides — skip silently

      const update: Partial<{
        rcdbId: number;
        minimumHeight: number;
      }> = {};
      if (entry.rcdbId && attraction.rcdbId !== entry.rcdbId) {
        update.rcdbId = entry.rcdbId;
        rcdbApplied++;
      }
      // Curated height is a FALLBACK — never overwrite an upstream value
      if (entry.minimumHeightCm && attraction.minimumHeight === null) {
        update.minimumHeight = entry.minimumHeightCm;
        heightsApplied++;
      }
      if (Object.keys(update).length > 0) {
        await repo.update(attraction.id, update);
      }
    }

    this.logger.log(
      `🔗 Manual metadata applied: ${rcdbApplied} RCDB ids, ${heightsApplied} fallback heights`,
    );
  }

  /**
   * Sync a single attraction (extracted from AttractionsService)
   */
  private async syncAttraction(
    attractionEntity: EntityResponse,
    parkId: string,
  ): Promise<void> {
    const mappedData = this.themeParksMapper.mapAttraction(
      attractionEntity,
      parkId,
    );

    // Check if attraction exists (by externalId)
    const existing = await this.attractionsService.getRepository().findOne({
      where: { externalId: mappedData.externalId },
    });

    if (existing) {
      // Update existing attraction (keep existing slug)
      await this.attractionsService.getRepository().update(existing.id, {
        name: mappedData.name,
        latitude: mappedData.latitude,
        longitude: mappedData.longitude,
        attractionType: mappedData.attractionType,
      });
    } else {
      // Generate unique slug for this park
      const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

      // Get all existing slugs for this park
      const existingAttractions = await this.attractionsService
        .getRepository()
        .find({
          where: { parkId },
          select: ["slug"],
        });
      const existingSlugs = existingAttractions.map((a) => a.slug);

      // Generate unique slug
      const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
      mappedData.slug = uniqueSlug;

      // Insert new attraction
      const saved = await this.attractionsService
        .getRepository()
        .save(mappedData);

      // Create mapping for themeparks-wiki
      await this.createMapping(
        saved.id,
        "attraction",
        "themeparks-wiki",
        saved.externalId,
      );
    }
  }

  /**
   * Sync a single show (extracted from ShowsService)
   */
  private async syncShow(
    showEntity: EntityResponse,
    parkId: string,
  ): Promise<void> {
    const mappedData = this.themeParksMapper.mapShow(showEntity, parkId);

    // Check if show exists (by externalId)
    const existing = await this.showsService.getRepository().findOne({
      where: { externalId: mappedData.externalId },
    });

    if (existing) {
      // Update existing show (keep existing slug)
      await this.showsService.getRepository().update(existing.id, {
        name: mappedData.name,
        latitude: mappedData.latitude,
        longitude: mappedData.longitude,
      });
    } else {
      // Generate unique slug for this park
      const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

      // Get all existing slugs for this park
      const existingShows = await this.showsService.getRepository().find({
        where: { parkId },
        select: ["slug"],
      });
      const existingSlugs = existingShows.map((s) => s.slug);

      // Generate unique slug
      const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
      mappedData.slug = uniqueSlug;

      // Insert new show
      await this.showsService.getRepository().save(mappedData);
    }
  }

  /**
   * Sync a single restaurant (extracted from RestaurantsService)
   */
  private async syncRestaurant(
    restaurantEntity: EntityResponse,
    parkId: string,
  ): Promise<void> {
    const mappedData = this.themeParksMapper.mapRestaurant(
      restaurantEntity,
      parkId,
    );

    // Check if restaurant exists (by externalId)
    const existing = await this.restaurantsService.getRepository().findOne({
      where: { externalId: mappedData.externalId },
    });

    if (existing) {
      // Update existing restaurant (keep existing slug)
      await this.restaurantsService.getRepository().update(existing.id, {
        name: mappedData.name,
        latitude: mappedData.latitude,
        longitude: mappedData.longitude,
        cuisineType: mappedData.cuisineType,
        cuisines: mappedData.cuisines,
        requiresReservation: mappedData.requiresReservation,
      });
    } else {
      // Generate unique slug for this park
      const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

      // Get all existing slugs for this park
      const existingRestaurants = await this.restaurantsService
        .getRepository()
        .find({
          where: { parkId },
          select: ["slug"],
        });
      const existingSlugs = existingRestaurants.map((r) => r.slug);

      // Generate unique slug
      const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
      mappedData.slug = uniqueSlug;

      // Insert new restaurant
      await this.restaurantsService.getRepository().save(mappedData);
    }
  }

  /**
   * Sync a single attraction from Queue-Times (Simplified)
   */
  private async syncQtAttraction(entity: any, parkId: string): Promise<void> {
    // Extract numeric Queue-Times ID (e.g., "8" from "qt-ride-8")
    const qtNumericId = extractQueueTimesNumericId(entity.externalId);

    // Check if attraction exists (by externalId)
    // Note: QT externalId is different from Wiki
    const existing = await this.attractionsService.getRepository().findOne({
      where: { externalId: entity.externalId },
    });

    if (existing) {
      // Update name and queueTimesEntityId if needed
      const updateData: any = {};
      if (existing.name !== entity.name) {
        updateData.name = entity.name;
      }
      if (qtNumericId && !existing.queueTimesEntityId) {
        updateData.queueTimesEntityId = qtNumericId;
      }

      if (Object.keys(updateData).length > 0) {
        await this.attractionsService
          .getRepository()
          .update(existing.id, updateData);
      }
    } else {
      // Generate slug
      const baseSlug = generateSlug(entity.name);

      // Get existing slugs
      const existingAttractions = await this.attractionsService
        .getRepository()
        .find({
          where: { parkId },
          select: ["slug"],
        });
      const existingSlugs = existingAttractions.map((a) => a.slug);
      const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);

      const newAttraction = await this.attractionsService.getRepository().save({
        externalId: entity.externalId,
        name: entity.name,
        slug: uniqueSlug,
        parkId: parkId,
        latitude: entity.latitude || undefined,
        longitude: entity.longitude || undefined,
        queueTimesEntityId: qtNumericId || null,
      } as any);

      // Create mapping
      await this.createMapping(
        newAttraction.id,
        "attraction",
        "queue-times",
        newAttraction.externalId,
      );
    }
  }

  /**
   * Create external entity mapping (with duplicate check)
   */
  private async createMapping(
    internalEntityId: string,
    internalEntityType: "attraction" | "show" | "restaurant",
    externalSource: string,
    externalEntityId: string,
  ): Promise<void> {
    // Check if mapping already exists
    const existing = await this.mappingRepository.findOne({
      where: {
        externalSource,
        externalEntityId,
      },
    });

    if (!existing) {
      await this.mappingRepository.save({
        internalEntityId,
        internalEntityType,
        externalSource,
        externalEntityId,
        matchConfidence: 1.0,
        matchStrategy: "exact",
      });
    }
  }

  /**
   * Sleep utility for batch delays
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
