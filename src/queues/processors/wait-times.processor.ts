import { Processor, Process, InjectQueue } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Job, Queue } from "bull";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { Park } from "../../parks/entities/park.entity";
import { ParksService } from "../../parks/parks.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { ShowsService } from "../../shows/shows.service";
import { RestaurantsService } from "../../restaurants/restaurants.service";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { MultiSourceOrchestrator } from "../../external-apis/data-sources/multi-source-orchestrator.service";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { CacheWarmupService } from "../services/cache-warmup.service";
import { PredictionDeviationService } from "../../ml/services/prediction-deviation.service";
import {
  EntityLiveResponse,
  EntityType,
  LiveStatus,
  QueueType,
} from "../../external-apis/themeparks/themeparks.types";
import { EntityLiveData } from "../../external-apis/data-sources/interfaces/data-source.interface";
import { In } from "typeorm";
import { Inject } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { formatInParkTimezone } from "../../common/utils/date.util";

/**
 * Wait Times Processor (OPTIMIZED + ENTITY ROUTING + CACHE WARMUP)
 *
 * Processes jobs in the 'wait-times' queue.
 * Fetches live data from ThemeParks.wiki for ALL entity types (attractions, shows, restaurants).
 *
 * OPTIMIZATIONS (98% API Call Reduction):
 * 1. Park-Level Live Data: One API call per park (not per entity)
 * 2. Only Open Parks: Skip closed parks entirely
 * 3. Closed Park Handling: Mark all entities in closed parks as CLOSED (no API call)
 *
 * ENTITY ROUTING (Phase 6.4):
 * - ATTRACTION → queue_data + forecast_data
 * - SHOW → show_live_data (showtimes)
 * - RESTAURANT → restaurant_live_data (dining availability)
 *
 * CACHE WARMUP (Phase 6.7):
 * - After sync: Warm up cache for OPERATING parks + Top 100 attractions
 * - Eliminates cold start delays on first API request
 *
 * Result:
 * - Before: 4,017 API calls (per attraction) = 803 calls/min
 * - After: ~30-50 API calls (open parks only) = ~10 calls/min
 * - Reduction: 98%!
 *
 * Scheduled: Every 5 minutes
 */
@Processor("wait-times")
export class WaitTimesProcessor {
  private readonly logger = new Logger(WaitTimesProcessor.name);

  constructor(
    @InjectQueue("wait-times") private waitTimesQueue: Queue,
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
    @InjectRepository(ExternalEntityMapping)
    private mappingRepository: Repository<ExternalEntityMapping>,
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    private parksService: ParksService,
    private attractionsService: AttractionsService,
    private showsService: ShowsService,
    private restaurantsService: RestaurantsService,
    private queueDataService: QueueDataService,
    private readonly orchestrator: MultiSourceOrchestrator,
    private readonly cacheWarmupService: CacheWarmupService,
    private readonly predictionDeviationService: PredictionDeviationService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  @Process("fetch-wait-times")
  async handleSyncWaitTimes(_job: Job): Promise<void> {
    this.logger.log("🎢 Starting OPTIMIZED wait times sync...");

    try {
      const parks = await this.parksService.findAll();

      if (parks.length === 0) {
        this.logger.warn("No parks found. Run park-metadata sync first.");
        return;
      }

      this.logger.debug(`Checking ${parks.length} parks...`);

      // Processed counters (how many entities we looked at)
      let totalAttractions = 0;
      let totalShows = 0;
      let totalRestaurants = 0;

      // Saved counters (how many were actually updated - delta based)
      let savedAttractions = 0;
      let savedShows = 0;
      let savedRestaurants = 0;

      let openParksCount = 0;
      let apiCallsCount = 0;
      const totalParks = parks.length;

      // Source counters
      const sourceStats: Record<string, number> = {};

      for (let parkIdx = 0; parkIdx < parks.length; parkIdx++) {
        const park = parks[parkIdx];
        try {
          // ALWAYS fetch ALL parks - ensures parks without schedules get updates
          const isOperatingToday = true; // Disabled schedule check

          if (isOperatingToday) {
            // Park is OPERATING TODAY: Fetch live data
            openParksCount++;

            try {
              // OPTIMIZATION 2: Multi-source park-level API call
              // Use explicit ID columns from Park entity (Step 845)
              const parkExternalIdMap = new Map<string, string>();
              if (park.wikiEntityId) {
                parkExternalIdMap.set("themeparks-wiki", park.wikiEntityId);
              }
              if (park.queueTimesEntityId) {
                parkExternalIdMap.set("queue-times", park.queueTimesEntityId);
              }
              if (park.wartezeitenEntityId) {
                parkExternalIdMap.set(
                  "wartezeiten-app",
                  park.wartezeitenEntityId,
                );
              }

              // OPTIMIZATION 3: Pre-fetch ALL entity mappings for this park
              // This enables O(1) lookup for live data from ANY source (Wiki, QT, etc.)
              const [pAttractions, pShows, pRestaurants] = await Promise.all([
                this.attractionsService.getRepository().find({
                  select: ["id", "externalId"],
                  where: { parkId: park.id },
                }),
                this.showsService.getRepository().find({
                  select: ["id", "externalId"],
                  where: { parkId: park.id },
                }),
                this.restaurantsService.getRepository().find({
                  select: ["id", "externalId"],
                  where: { parkId: park.id },
                }),
              ]);

              const allInternalIds = [
                ...pAttractions.map((e) => e.id),
                ...pShows.map((e) => e.id),
                ...pRestaurants.map((e) => e.id),
              ];

              let entityMappings: ExternalEntityMapping[] = [];
              if (allInternalIds.length > 0) {
                entityMappings = await this.mappingRepository.find({
                  where: { internalEntityId: In(allInternalIds) },
                });
              }

              // Build fast lookup map: "source:externalId" -> "internalId"
              const mappingLookup = new Map<string, string>();

              // First, add ALL external mappings from database (including QT)
              entityMappings.forEach((m) => {
                mappingLookup.set(
                  `${m.externalSource}:${m.externalEntityId}`,
                  m.internalEntityId,
                );
              });

              // Then add default Wiki IDs as fallback (Source of Truth)
              // This ensures Wiki entities work even without explicit mappings
              pAttractions.forEach((a) => {
                if (a.externalId)
                  mappingLookup.set(`themeparks-wiki:${a.externalId}`, a.id);
              });
              pShows.forEach((s) => {
                if (s.externalId)
                  mappingLookup.set(`themeparks-wiki:${s.externalId}`, s.id);
              });
              pRestaurants.forEach((r) => {
                if (r.externalId)
                  mappingLookup.set(`themeparks-wiki:${r.externalId}`, r.id);
              });

              // Fetch live data from all applicable sources via orchestrator
              const liveData = await this.orchestrator.fetchParkLiveData(
                park.id,
                parkExternalIdMap,
              );
              apiCallsCount++;

              // EXPERIMENTAL: Save crowd level from Wartezeiten.app (if available)
              // NOTE: Data quality unverified - stored for analysis/comparison only
              // Not exposed in API until validated against ML predictions
              if (
                liveData.crowdLevel !== undefined &&
                liveData.crowdLevel !== null
              ) {
                try {
                  await this.parkRepository.update(park.id, {
                    currentCrowdLevel: liveData.crowdLevel,
                  });
                  this.logger.verbose(
                    `📊 Updated crowd level for ${park.name}: ${liveData.crowdLevel.toFixed(1)}`,
                  );
                } catch (error) {
                  this.logger.warn(
                    `Failed to update crowd level for ${park.name}: ${error}`,
                  );
                }
              }

              // Persist Operating Hours (from Wartezeiten or Wiki live data)
              // This ensures we have schedule data even if the bulk schedule sync missed it
              if (
                liveData.operatingHours &&
                liveData.operatingHours.length > 0
              ) {
                try {
                  const scheduleUpdates = liveData.operatingHours.map(
                    (window) => ({
                      date: window.open, // ISO string acts as date
                      type: window.type,
                      openingTime: window.open,
                      closingTime: window.close,
                      description: "Live update",
                    }),
                  );

                  await this.parksService.saveScheduleData(
                    park.id,
                    scheduleUpdates,
                  );
                } catch (error) {
                  this.logger.warn(
                    `Failed to update schedule from live data for ${park.name}: ${error}`,
                  );
                }
              }

              // Process land data if available
              if (liveData.lands && liveData.lands.length > 0) {
                const landUpdates = await this.processLandData(
                  liveData.lands,
                  park,
                );
                if (landUpdates > 0) {
                  sourceStats["queue-times"] =
                    (sourceStats["queue-times"] || 0) + landUpdates;
                }
              }

              // Parse aggregated live data
              if (liveData.entities && liveData.entities.length > 0) {
                // Process each entity's live data
                for (const entityLiveData of liveData.entities) {
                  try {
                    let savedCount = 0;

                    // Route by entity type
                    switch (entityLiveData.entityType) {
                      case EntityType.ATTRACTION:
                        const attractionSaved =
                          await this.processAttractionLiveData(
                            entityLiveData,
                            mappingLookup,
                          );
                        totalAttractions++;
                        savedAttractions += attractionSaved;
                        savedCount = attractionSaved;
                        break;

                      case EntityType.SHOW:
                        const showSaved = await this.processShowLiveData(
                          entityLiveData,
                          mappingLookup,
                        );
                        totalShows++;
                        savedShows += showSaved;
                        savedCount = showSaved;
                        break;

                      case EntityType.RESTAURANT:
                        const restaurantSaved =
                          await this.processRestaurantLiveData(
                            entityLiveData,
                            mappingLookup,
                          );
                        totalRestaurants++;
                        savedRestaurants += restaurantSaved;
                        savedCount = restaurantSaved;
                        break;
                    }

                    // Track status changes for downtime feature (Phase 2)
                    if (
                      savedCount > 0 &&
                      entityLiveData.entityType === EntityType.ATTRACTION
                    ) {
                      await this.trackDowntime(
                        entityLiveData,
                        mappingLookup,
                        park.timezone,
                      );
                    }

                    if (savedCount > 0) {
                      const src = entityLiveData.source || "unknown";
                      sourceStats[src] = (sourceStats[src] || 0) + 1;

                      // Check for prediction deviations (only for attractions)
                      if (entityLiveData.entityType === EntityType.ATTRACTION) {
                        await this.checkAndFlagDeviation(
                          entityLiveData,
                          mappingLookup,
                        );
                      }
                    }
                  } catch (error) {
                    const errorMessage =
                      error instanceof Error ? error.message : String(error);
                    this.logger.error(
                      `❌ Failed to process ${entityLiveData.entityType} live data: ${errorMessage}`,
                    );
                  }
                }
              } else {
                // Park is OPEN but API returned no data
                // → Mark all entities as OPERATING (fallback)
                // this.logger.verbose(
                //   `⚠️  Park ${park.name} returned no live data - marking all entities as OPERATING (fallback)`,
                // );

                const [parkAttractions, parkShows, parkRestaurants] =
                  await Promise.all([
                    this.attractionsService.getRepository().find({
                      where: { parkId: park.id },
                    }),
                    this.showsService.getRepository().find({
                      where: { parkId: park.id },
                    }),
                    this.restaurantsService.getRepository().find({
                      where: { parkId: park.id },
                    }),
                  ]);

                // Mark each attraction as OPERATING
                for (const attraction of parkAttractions) {
                  try {
                    const fallbackData: EntityLiveResponse = {
                      id: attraction.externalId,
                      name: attraction.name,
                      entityType: EntityType.ATTRACTION,
                      status: LiveStatus.OPERATING,
                      lastUpdated: new Date().toISOString(),
                    };

                    const saved = await this.queueDataService.saveLiveData(
                      attraction.id,
                      fallbackData,
                    );
                    totalAttractions++;
                    savedAttractions += saved;
                  } catch (error) {
                    const errorMessage =
                      error instanceof Error ? error.message : String(error);
                    this.logger.error(
                      `❌ Failed to mark attraction ${attraction.name} as operating: ${errorMessage}`,
                    );
                  }
                }

                // Mark each show as OPERATING
                for (const show of parkShows) {
                  try {
                    const fallbackData: EntityLiveResponse = {
                      id: show.externalId,
                      name: show.name,
                      entityType: EntityType.SHOW,
                      status: LiveStatus.OPERATING,
                      lastUpdated: new Date().toISOString(),
                    };

                    const saved = await this.showsService.saveShowLiveData(
                      show.id,
                      fallbackData,
                    );
                    totalShows++;
                    savedShows += saved;
                  } catch (error) {
                    const errorMessage =
                      error instanceof Error ? error.message : String(error);
                    this.logger.error(
                      `❌ Failed to mark show ${show.name} as operating: ${errorMessage}`,
                    );
                  }
                }

                // Mark each restaurant as OPERATING
                for (const restaurant of parkRestaurants) {
                  try {
                    const fallbackData: EntityLiveResponse = {
                      id: restaurant.externalId,
                      name: restaurant.name,
                      entityType: EntityType.RESTAURANT,
                      status: LiveStatus.OPERATING,
                      lastUpdated: new Date().toISOString(),
                    };

                    const saved =
                      await this.restaurantsService.saveDiningAvailability(
                        restaurant.id,
                        fallbackData,
                      );
                    totalRestaurants++;
                    savedRestaurants += saved;
                  } catch (error) {
                    const errorMessage =
                      error instanceof Error ? error.message : String(error);
                    this.logger.error(
                      `❌ Failed to mark restaurant ${restaurant.name} as operating: ${errorMessage}`,
                    );
                  }
                }
              }
            } catch (error) {
              const errorMessage =
                error instanceof Error ? error.message : String(error);
              this.logger.error(
                `❌ Failed to fetch park-level live data for ${park.name}: ${errorMessage}`,
              );
              // Continue with next park
            }
          } else {
            // This should never be reached since isOperatingToday = true

            // Get all entities for this closed park
            const [
              parkAttractions,
              parkShows,
              parkRestaurants,
              lastShowDataMap,
              lastRestaurantDataMap,
            ] = await Promise.all([
              this.attractionsService.getRepository().find({
                where: { parkId: park.id },
              }),
              this.showsService.getRepository().find({
                where: { parkId: park.id },
              }),
              this.restaurantsService.getRepository().find({
                where: { parkId: park.id },
              }),
              this.showsService.findTodayOperatingDataByPark(
                park.id,
                park.timezone || "UTC",
              ),
              this.restaurantsService.findTodayOperatingDataByPark(
                park.id,
                park.timezone || "UTC",
              ),
            ]);

            // Mark each attraction as CLOSED
            for (const attraction of parkAttractions) {
              try {
                const closedData: EntityLiveResponse = {
                  id: attraction.externalId,
                  name: attraction.name,
                  entityType: EntityType.ATTRACTION,
                  status: LiveStatus.CLOSED,
                  lastUpdated: new Date().toISOString(),
                };

                const savedCount = await this.queueDataService.saveLiveData(
                  attraction.id,
                  closedData,
                );

                savedAttractions += savedCount;
              } catch (error) {
                const errorMessage =
                  error instanceof Error ? error.message : String(error);
                this.logger.error(
                  `❌ Failed to mark attraction ${attraction.name} as closed: ${errorMessage}`,
                );
              }
            }

            // Mark each show as CLOSED (preserve showtimes & operatingHours)
            for (const show of parkShows) {
              try {
                // Get last known live data from bulk fetch
                const lastLiveData = lastShowDataMap.get(show.id);

                const closedData: EntityLiveResponse = {
                  id: show.externalId,
                  name: show.name,
                  entityType: EntityType.SHOW,
                  status: LiveStatus.CLOSED,
                  lastUpdated: new Date().toISOString(),
                  // Preserve data from last known status if available
                  showtimes: lastLiveData?.showtimes || [],
                  operatingHours: lastLiveData?.operatingHours || [],
                };

                const savedCount = await this.showsService.saveShowLiveData(
                  show.id,
                  closedData,
                );

                savedShows += savedCount;
              } catch (error) {
                const errorMessage =
                  error instanceof Error ? error.message : String(error);
                this.logger.error(
                  `❌ Failed to mark show ${show.name} as closed: ${errorMessage}`,
                );
              }
            }

            // Mark each restaurant as CLOSED (preserve operatingHours)
            for (const restaurant of parkRestaurants) {
              try {
                // Get last known live data from bulk fetch
                const lastLiveData = lastRestaurantDataMap.get(restaurant.id);

                const closedData: EntityLiveResponse = {
                  id: restaurant.externalId,
                  name: restaurant.name,
                  entityType: EntityType.RESTAURANT,
                  status: LiveStatus.CLOSED,
                  lastUpdated: new Date().toISOString(),
                  // Preserve operatingHours from last known data (if available)
                  diningAvailability: undefined, // Clear live dining data
                  operatingHours: lastLiveData?.operatingHours || [],
                };

                const savedCount =
                  await this.restaurantsService.saveDiningAvailability(
                    restaurant.id,
                    closedData,
                  );

                savedRestaurants += savedCount;
              } catch (error) {
                const errorMessage =
                  error instanceof Error ? error.message : String(error);
                this.logger.error(
                  `❌ Failed to mark restaurant ${restaurant.name} as closed: ${errorMessage}`,
                );
              }
            }

            totalAttractions += parkAttractions.length;
            totalShows += parkShows.length;
            totalRestaurants += parkRestaurants.length;
          }

          // Log progress every 10 parks or at the end
          if ((parkIdx + 1) % 10 === 0 || parkIdx + 1 === totalParks) {
            const percent = Math.round(((parkIdx + 1) / totalParks) * 100);
            this.logger.log(
              `Progress: ${parkIdx + 1}/${totalParks} (${percent}%) - ` +
                `${openParksCount} parks - ` +
                `${totalAttractions} attractions processed`,
            );
          }
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : String(error);
          const errorStack = error instanceof Error ? error.stack : undefined;
          this.logger.error(
            `❌ Failed to process park ${park.name}: ${errorMessage}`,
          );
          if (errorStack) {
            this.logger.error(`Stack trace: ${errorStack}`);
          }
          // Continue with next park
        }
      }

      this.logger.log(`✅ Wait times sync complete!`);
      this.logger.log(
        `📊 Parks: ${openParksCount} fetched (${apiCallsCount} API calls)`,
      );
      this.logger.log(
        `💾 Processed: ${totalAttractions} attractions, ${totalShows} shows, ${totalRestaurants} restaurants`,
      );
      this.logger.log(
        `🔄 Updated: ${savedAttractions} attractions, ${savedShows} shows, ${savedRestaurants} restaurants (delta-based)`,
      );
      this.logger.log(
        `📡 Sources: ${
          Object.entries(sourceStats)
            .map(([k, v]) => `${k}=${v}`)
            .join(", ") || "none"
        }`,
      );

      // Cache Warmup: Prepopulate cache for OPERATING parks + Top 100 attractions + Occupancy data
      // This eliminates cold start delays on first API request after sync
      this.logger.verbose("🔥 Starting cache warmup..."); // Log -> Verbose
      try {
        await Promise.all([
          this.cacheWarmupService.warmupOperatingParks(),
          this.cacheWarmupService.warmupTopAttractions(100),
          this.cacheWarmupService.warmupParkOccupancy(parks.map((p) => p.id)),
        ]);
      } catch (error: unknown) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.warn(`Cache warmup failed: ${errorMessage}`);
        // Don't throw - warmup failure shouldn't fail the entire sync
      }
    } catch (error) {
      this.logger.error("❌ Wait times sync failed", error);
      throw error; // Bull will retry
    }
  }

  /**
   * Process attraction live data
   */
  /**
   * Process attraction live data
   */
  private async processAttractionLiveData(
    entityData: EntityLiveData,
    mappingLookup: Map<string, string>,
  ): Promise<number> {
    // 1. Find internal ID using lookup map (Source:ExternalID -> InternalID)
    const lookupKey = `${entityData.source}:${entityData.externalId}`;
    const internalId = mappingLookup.get(lookupKey);

    if (!internalId) {
      // Not found in our DB (or no mapping)
      return 0;
    }

    // 2. Adapt to legacy format
    const legacyData = this.adaptEntityLiveData(entityData);

    // 3. Save queue data
    const savedCount = await this.queueDataService.saveLiveData(
      internalId, // Use resolved internal ID
      legacyData,
    );

    return savedCount;
  }

  /**
   * Process show live data
   */
  /**
   * Process show live data
   */
  private async processShowLiveData(
    entityData: EntityLiveData,
    mappingLookup: Map<string, string>,
  ): Promise<number> {
    const lookupKey = `${entityData.source}:${entityData.externalId}`;
    const internalId = mappingLookup.get(lookupKey);

    if (!internalId) {
      return 0;
    }

    const legacyData = this.adaptEntityLiveData(entityData);

    const savedCount = await this.showsService.saveShowLiveData(
      internalId,
      legacyData,
    );

    return savedCount;
  }

  /**
   * Process restaurant live data
   */
  /**
   * Process restaurant live data
   */
  private async processRestaurantLiveData(
    entityData: EntityLiveData,
    mappingLookup: Map<string, string>,
  ): Promise<number> {
    const lookupKey = `${entityData.source}:${entityData.externalId}`;
    const internalId = mappingLookup.get(lookupKey);

    if (!internalId) {
      return 0;
    }

    const legacyData = this.adaptEntityLiveData(entityData);

    const savedCount = await this.restaurantsService.saveDiningAvailability(
      internalId,
      legacyData,
    );

    return savedCount;
  }

  /**
   * Create Queue-Times entity mapping
   *
   * Maps Queue-Times external IDs to internal entity IDs via name matching
   *
   * NOTE: This is a pragmatic solution. Queue-Times IDs are different from
   * ThemeParks.wiki IDs, so we match by name. This works for 80%+ of cases.
   * For perfect matching, we'd need a dedicated entity-matching processor.
   */
  private async createEntityMapping(
    externalEntityId: string,
    entityType: "attraction" | "show" | "restaurant",
    source: string,
  ): Promise<void> {
    // Check if mapping already exists
    const existing = await this.mappingRepository.findOne({
      where: {
        externalSource: source,
        externalEntityId,
      },
    });

    if (existing) {
      return; // Already mapped
    }

    // For Queue-Times, we need to find the entity by matching name in the live data
    // The entityLiveData has the name, and we stored it from ThemeParks.wiki
    // This is handled at the call site - we only create mappings if we found the entity
    // So this method should not be reached if entity doesn't exist

    // Actually, let's just skip this for now and handle mapping creation differently
    // Not creating QT mappings here - too complex without entity context
  }

  /**
   * Process land data from Queue-Times
   *
   * Assigns land names and IDs to attractions by matching names
   * Uses ExternalEntityMapping to link Queue-Times IDs to internal attractions
   */
  private async processLandData(lands: any[], park: any): Promise<number> {
    // Phase 6.6.3: Land Assignment Logic Enabled

    // Get all queue-times mappings for this park
    // This allows us to link QT IDs (from land data) to our internal UUIDs

    // Instead of bulk fetching mappings (which is hard without join),
    // let's iterate lands and attractions, and look up mappings.
    // Optimization: Bulk fetch mappings for all attractions in this park?

    // 1. Get all attractions in this park
    const parkAttractions = await this.attractionsService.getRepository().find({
      select: ["id", "name"],
      where: { parkId: park.id },
    });

    const attractionIds = parkAttractions.map((a) => a.id);

    if (attractionIds.length === 0) return 0;

    // 2. Fetch all QT mappings for these attractions
    const qtMappings = await this.mappingRepository
      .createQueryBuilder("mapping")
      .where("mapping.internalEntityId IN (:...ids)", { ids: attractionIds })
      .andWhere("mapping.externalSource = 'queue-times'")
      .getMany();

    // Create a Map: QT_ID -> Internal_Attraction_ID
    const qtIdMap = new Map<string, string>();
    qtMappings.forEach((m: ExternalEntityMapping) =>
      qtIdMap.set(m.externalEntityId, m.internalEntityId),
    );

    let updatedCount = 0;

    for (const land of lands) {
      if (!land.name) continue;

      // land.attractions contains QT IDs
      for (const qtAttractionId of land.attractions) {
        // Find internal ID
        const internalId = qtIdMap.get(qtAttractionId.toString());

        if (internalId) {
          // Update attraction with land info
          const changed = await this.attractionsService.updateLandInfo(
            internalId,
            land.name,
            land.id?.toString() || null,
          );

          if (changed) {
            updatedCount++;
          }
        }
      }
    }

    if (updatedCount > 0) {
      this.logger.debug(
        `🏰 Updated land assignment for ${updatedCount} attractions in ${park.name}`,
      );
    }

    return updatedCount;
  }

  /**
   * Adapt EntityLiveData to EntityLiveResponse
   *
   * Converts unified multi-source format to legacy ThemeParks format
   */
  private adaptEntityLiveData(entityData: any): EntityLiveResponse {
    // Build queue object properly
    // EntityLiveResponse expects: { STANDBY?: {...}, RETURN_TIME?: {...}, etc }
    // NOT an array
    let queue: any | undefined;

    if (
      entityData.queue &&
      typeof entityData.queue === "object" &&
      !Array.isArray(entityData.queue)
    ) {
      // Already in correct format from ThemeParks.wiki
      queue = entityData.queue;
    } else if (entityData.waitTime !== undefined) {
      // From Queue-Times: simple waitTime → convert to STANDBY queue
      queue = {
        [QueueType.STANDBY]: {
          waitTime: entityData.waitTime,
        },
      };
    }

    return {
      id: entityData.externalId,
      name: entityData.name,
      entityType: entityData.entityType,
      status: entityData.status,
      queue,
      showtimes: entityData.showtimes,
      diningAvailability: entityData.diningAvailability,
      lastUpdated: entityData.lastUpdated,
    };
  }

  /**
   * Check for prediction deviations and flag them
   *
   * This enables "Confidence Downgrade" strategy without regenerating predictions
   *
   * @param entityData - Live data from API
   * @param mappingLookup - Entity ID mapping
   */
  private async checkAndFlagDeviation(
    entityData: EntityLiveData,
    mappingLookup: Map<string, string>,
  ): Promise<void> {
    try {
      // Only check if we have a valid wait time
      if (entityData.status !== "OPERATING" || !entityData.waitTime) {
        return;
      }

      // Get internal attraction ID
      const lookupKey = `${entityData.source}:${entityData.externalId}`;
      const attractionId = mappingLookup.get(lookupKey);

      if (!attractionId) {
        return;
      }

      // Check for deviation
      const result = await this.predictionDeviationService.checkDeviation(
        attractionId,
        entityData.waitTime,
      );

      // Flag if deviation detected
      if (
        result.hasDeviation &&
        result.deviation &&
        result.percentageDeviation &&
        result.predictedWaitTime
      ) {
        await this.predictionDeviationService.flagDeviation(attractionId, {
          actualWaitTime: entityData.waitTime,
          predictedWaitTime: result.predictedWaitTime,
          deviation: result.deviation,
          percentageDeviation: result.percentageDeviation,
          detectedAt: new Date(),
        });
      }
    } catch (error) {
      // Don't fail the job if deviation check fails
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.warn(`Failed to check deviation: ${errorMessage}`);
    }
  }

  /**
   * Track status changes for downtime feature (Phase 2)
   *
   * Detects OPERATING → DOWN/CLOSED/REFURB transitions and accumulates daily downtime minutes
   * Stores in Redis: downtime:daily:{attractionId}:{date} with TTL of 25 hours
   *
   * @param entityData - Live entity data from API
   * @param mappingLookup - External ID to internal ID mapping
   */
  private async trackDowntime(
    entityData: EntityLiveData,
    mappingLookup: Map<string, string>,
    timezone: string = "UTC",
  ): Promise<void> {
    try {
      const lookupKey = `${entityData.source}:${entityData.externalId}`;
      const attractionId = mappingLookup.get(lookupKey);

      if (!attractionId) {
        return;
      }

      const currentStatus = entityData.status;
      const statusKey = `downtime:status:${attractionId}`;
      const downtimeStartKey = `downtime:start:${attractionId}`;

      // Get previous status from Redis
      const previousStatus = await this.redis.get(statusKey);

      // Update status
      await this.redis.set(statusKey, currentStatus, "EX", 3600); // 1h TTL

      // Check for status change: OPERATING → DOWN/CLOSED/REFURB
      if (
        previousStatus === LiveStatus.OPERATING &&
        (currentStatus === LiveStatus.DOWN ||
          currentStatus === LiveStatus.CLOSED ||
          currentStatus === LiveStatus.REFURBISHMENT)
      ) {
        // Start tracking downtime
        const now = Date.now();
        await this.redis.set(downtimeStartKey, now.toString(), "EX", 3600);
        this.logger.debug(
          `Downtime started for ${attractionId}: ${previousStatus} → ${currentStatus}`,
        );
      }

      // Check for status change: DOWN/CLOSED/REFURB → OPERATING
      if (
        (previousStatus === LiveStatus.DOWN ||
          previousStatus === LiveStatus.CLOSED ||
          previousStatus === LiveStatus.REFURBISHMENT) &&
        currentStatus === LiveStatus.OPERATING
      ) {
        // Calculate downtime duration
        const startTimeStr = await this.redis.get(downtimeStartKey);
        if (startTimeStr) {
          const startTime = parseInt(startTimeStr);
          const now = Date.now();
          const downtimeMinutes = Math.round((now - startTime) / 60000); // Convert ms to minutes

          if (downtimeMinutes > 0) {
            // Add to daily downtime total
            const todayStr = formatInParkTimezone(new Date(), timezone);
            const dailyKey = `downtime:daily:${attractionId}:${todayStr}`;

            const currentTotal = await this.redis.get(dailyKey);
            const newTotal = parseInt(currentTotal || "0") + downtimeMinutes;

            // Store with 25h TTL (covers timezone edge cases)
            await this.redis.set(
              dailyKey,
              newTotal.toString(),
              "EX",
              25 * 3600,
            );

            this.logger.debug(
              `Downtime ended for ${attractionId}: ${downtimeMinutes}min (total today: ${newTotal}min)`,
            );
          }

          // Clean up start timestamp
          await this.redis.del(downtimeStartKey);
        }
      }
    } catch (error) {
      // Don't fail the job if downtime tracking fails
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.warn(`Failed to track downtime: ${errorMessage}`);
    }
  }
}
