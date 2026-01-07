import { Processor, Process, InjectQueue } from "@nestjs/bull";
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
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { QueueTimesDataSource } from "../../external-apis/queue-times/queue-times-data-source";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

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

              // Filter by entity type
              const attractions = childrenResponse.children.filter(
                (child) => child.entityType === "ATTRACTION",
              );
              const shows = childrenResponse.children.filter(
                (child) => child.entityType === "SHOW",
              );
              const restaurants = childrenResponse.children.filter(
                (child) => child.entityType === "RESTAURANT",
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
                await this.redis.del(`park:integrated:${park.id}`);
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
    const qtNumericId = this.extractQueueTimesNumericId(entity.externalId);

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
   * Extract numeric Queue-Times ID from external ID
   * Examples:
   * - "qt-ride-8" -> "8"
   * - "qt-park-56" -> "56"
   * - "8" -> "8" (already numeric)
   */
  private extractQueueTimesNumericId(externalId: string): string | null {
    if (!externalId) return null;

    // Handle prefixed IDs like "qt-ride-8" or "qt-park-56"
    if (externalId.startsWith("qt-ride-")) {
      return externalId.replace("qt-ride-", "");
    }
    if (externalId.startsWith("qt-park-")) {
      return externalId.replace("qt-park-", "");
    }

    // If already numeric, return as-is
    if (/^\d+$/.test(externalId)) {
      return externalId;
    }

    return null;
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
