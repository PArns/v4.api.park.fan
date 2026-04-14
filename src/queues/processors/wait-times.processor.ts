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
import { PopularityService } from "../../popularity/popularity.service";
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
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";

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
    private readonly popularityService: PopularityService,
    private readonly predictionDeviationService: PredictionDeviationService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  @Process("fetch-wait-times")
  async handleSyncWaitTimes(_job: Job): Promise<void> {
    this.logger.log("🎢 Starting BATCH wait times sync...");
    const startTime = Date.now();

    try {
      const allParks = await this.parksService.findAll();

      if (allParks.length === 0) {
        this.logger.warn("No parks found. Run park-metadata sync first.");
        return;
      }

      // Prioritization: Top Parks first
      const topParkIds: string[] = await this.popularityService
        .getTopParks(50)
        .catch(() => []);

      const prioritizedParks = [...allParks].sort((a, b) => {
        const indexA = topParkIds.indexOf(a.id);
        const indexB = topParkIds.indexOf(b.id);
        if (indexA === -1 && indexB === -1) return 0;
        if (indexA === -1) return 1;
        if (indexB === -1) return -1;
        return indexA - indexB;
      });

      // Counters
      let savedAttractions = 0;
      let savedShows = 0;
      let savedRestaurants = 0;
      let openParksCount = 0;
      const sourceStats: Record<string, number> = {};

      // Batching
      const BATCH_SIZE = 5;
      for (let i = 0; i < prioritizedParks.length; i += BATCH_SIZE) {
        const batch = prioritizedParks.slice(i, i + BATCH_SIZE);

        await Promise.all(
          batch.map(async (park) => {
            try {
              openParksCount++;

              const parkExternalIdMap = new Map<string, string>();
              if (park.wikiEntityId)
                parkExternalIdMap.set("themeparks-wiki", park.wikiEntityId);
              if (park.queueTimesEntityId)
                parkExternalIdMap.set("queue-times", park.queueTimesEntityId);
              if (park.wartezeitenEntityId)
                parkExternalIdMap.set(
                  "wartezeiten-app",
                  park.wartezeitenEntityId,
                );

              // Pre-fetch entity mappings
              const [pAttractions, pShows, pRestaurants] = await Promise.all([
                this.attractionsService.getRepository().find({
                  where: { parkId: park.id },
                  select: ["id", "externalId", "name"],
                }),
                this.showsService.getRepository().find({
                  where: { parkId: park.id },
                  select: ["id", "externalId", "name"],
                }),
                this.restaurantsService.getRepository().find({
                  where: { parkId: park.id },
                  select: ["id", "externalId", "name"],
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

              const mappingLookup = new Map<string, string>();
              entityMappings.forEach((m) =>
                mappingLookup.set(
                  `${m.externalSource}:${m.externalEntityId}`,
                  m.internalEntityId,
                ),
              );
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

              // Fetch live data
              const liveData = await this.orchestrator.fetchParkLiveData(
                park.id,
                parkExternalIdMap,
              );

              // Update Crowd Level
              if (liveData.crowdLevel != null) {
                await this.parkRepository
                  .update(park.id, { currentCrowdLevel: liveData.crowdLevel })
                  .catch(() => {});
              }

              // Update Schedule from Live Data (Fallback)
              if (
                liveData.operatingHours &&
                liveData.operatingHours.length > 0
              ) {
                const todayWiki = await this.parksService.getTodaySchedule(
                  park.id,
                );
                if (!todayWiki || todayWiki.length === 0) {
                  const scheduleUpdates = liveData.operatingHours.map((w) => ({
                    date: w.open,
                    type: w.type,
                    openingTime: w.open,
                    closingTime: w.close,
                    description: "Live update",
                  }));
                  await this.parksService
                    .saveScheduleData(park.id, scheduleUpdates)
                    .catch(() => {});
                }
              }

              // Process Entities
              if (liveData.entities && liveData.entities.length > 0) {
                for (const entityLiveData of liveData.entities) {
                  try {
                    let savedCount = 0;
                    switch (entityLiveData.entityType) {
                      case EntityType.ATTRACTION:
                        savedCount = await this.processAttractionLiveData(
                          entityLiveData,
                          mappingLookup,
                        );
                        savedAttractions += savedCount;
                        break;
                      case EntityType.SHOW:
                        savedCount = await this.processShowLiveData(
                          entityLiveData,
                          mappingLookup,
                        );
                        savedShows += savedCount;
                        break;
                      case EntityType.RESTAURANT:
                        savedCount = await this.processRestaurantLiveData(
                          entityLiveData,
                          mappingLookup,
                        );
                        savedRestaurants += savedCount;
                        break;
                    }

                    if (savedCount > 0) {
                      const src = entityLiveData.source || "unknown";
                      sourceStats[src] = (sourceStats[src] || 0) + 1;

                      // Downtime Tracking & Deviations
                      if (entityLiveData.entityType === EntityType.ATTRACTION) {
                        let closingTime: Date | undefined;
                        if (
                          liveData.operatingHours &&
                          liveData.operatingHours.length > 0
                        ) {
                          const todayStr = getCurrentDateInTimezone(
                            park.timezone || "UTC",
                          );
                          const todayWindow = liveData.operatingHours.find(
                            (w) =>
                              formatInParkTimezone(
                                new Date(w.open),
                                park.timezone || "UTC",
                              ) === todayStr,
                          );
                          if (todayWindow?.close)
                            closingTime = new Date(todayWindow.close);
                        }
                        await this.trackDowntime(
                          entityLiveData,
                          mappingLookup,
                          park.timezone,
                          closingTime,
                        );
                        await this.checkAndFlagDeviation(
                          entityLiveData,
                          mappingLookup,
                        );
                      }
                    }
                  } catch (_e) {}
                }
              }
            } catch (_e) {
              this.logger.debug(`Failed to process park ${park.name}`);
            }
          }),
        );

        // Log Progress
        if (
          (i + BATCH_SIZE) % 10 === 0 ||
          i + BATCH_SIZE >= prioritizedParks.length
        ) {
          const current = Math.min(i + BATCH_SIZE, prioritizedParks.length);
          this.logger.log(
            `Progress: ${current}/${prioritizedParks.length} - ${openParksCount} parks fetched`,
          );
        }
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      this.logger.log(`✅ Wait times sync complete in ${duration}s!`);
      this.logger.log(
        `📊 Updated: ${savedAttractions} attractions, ${savedShows} shows, ${savedRestaurants} restaurants`,
      );

      // Warmup & Heartbeats
      try {
        await Promise.all([
          this.cacheWarmupService.warmupOperatingParks(),
          this.cacheWarmupService.warmupTopAttractions(1000),
          this.cacheWarmupService.warmupParkOccupancy(
            prioritizedParks.map((p) => p.id),
          ),
        ]);
        const hb = await this.writeHourlyHeartbeats();
        if (hb > 0) this.logger.log(`💓 Wrote ${hb} hourly heartbeats`);
      } catch (_e) {}
    } catch (error) {
      this.logger.error("❌ Wait times sync failed", error);
      throw error;
    }
  }

  private async processAttractionLiveData(
    entityData: EntityLiveData,
    mappingLookup: Map<string, string>,
  ): Promise<number> {
    const internalId = mappingLookup.get(
      `${entityData.source}:${entityData.externalId}`,
    );
    if (!internalId) return 0;
    return this.queueDataService.saveLiveData(
      internalId,
      this.adaptEntityLiveData(entityData),
    );
  }

  private async processShowLiveData(
    entityData: EntityLiveData,
    mappingLookup: Map<string, string>,
  ): Promise<number> {
    const internalId = mappingLookup.get(
      `${entityData.source}:${entityData.externalId}`,
    );
    if (!internalId) return 0;
    return this.showsService.saveShowLiveData(
      internalId,
      this.adaptEntityLiveData(entityData),
    );
  }

  private async processRestaurantLiveData(
    entityData: EntityLiveData,
    mappingLookup: Map<string, string>,
  ): Promise<number> {
    const internalId = mappingLookup.get(
      `${entityData.source}:${entityData.externalId}`,
    );
    if (!internalId) return 0;
    return this.restaurantsService.saveDiningAvailability(
      internalId,
      this.adaptEntityLiveData(entityData),
    );
  }

  private async processLandData(lands: any[], park: any): Promise<number> {
    const parkAttractions = await this.attractionsService
      .getRepository()
      .find({ select: ["id", "name"], where: { parkId: park.id } });
    const attractionIds = parkAttractions.map((a) => a.id);
    if (attractionIds.length === 0) return 0;

    const qtMappings = await this.mappingRepository
      .createQueryBuilder("mapping")
      .where("mapping.internalEntityId IN (:...ids)", { ids: attractionIds })
      .andWhere("mapping.externalSource = 'queue-times'")
      .getMany();

    const qtIdMap = new Map<string, string>();
    qtMappings.forEach((m) =>
      qtIdMap.set(m.externalEntityId, m.internalEntityId),
    );

    let updatedCount = 0;
    for (const land of lands) {
      if (!land.name) continue;
      for (const qtAttractionId of land.attractions) {
        const internalId = qtIdMap.get(qtAttractionId.toString());
        if (internalId) {
          const changed = await this.attractionsService.updateLandInfo(
            internalId,
            land.name,
            land.id?.toString() || null,
          );
          const qtNumericId = this.extractQueueTimesNumericId(
            qtAttractionId.toString(),
          );
          if (qtNumericId) {
            await this.attractionsService
              .getRepository()
              .update(internalId, { queueTimesEntityId: qtNumericId })
              .catch(() => {});
          }
          if (changed) updatedCount++;
        }
      }
    }
    return updatedCount;
  }

  private extractQueueTimesNumericId(externalId: string): string | null {
    if (!externalId) return null;
    if (externalId.startsWith("qt-ride-"))
      return externalId.replace("qt-ride-", "");
    if (externalId.startsWith("qt-park-"))
      return externalId.replace("qt-park-", "");
    return /^\d+$/.test(externalId) ? externalId : null;
  }

  private adaptEntityLiveData(entityData: any): EntityLiveResponse {
    let queue: any | undefined;
    if (
      entityData.queue &&
      typeof entityData.queue === "object" &&
      !Array.isArray(entityData.queue)
    ) {
      queue = entityData.queue;
    } else if (entityData.waitTime !== undefined) {
      queue = { [QueueType.STANDBY]: { waitTime: entityData.waitTime } };
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

  private async checkAndFlagDeviation(
    entityData: EntityLiveData,
    mappingLookup: Map<string, string>,
  ): Promise<void> {
    try {
      if (entityData.status !== "OPERATING" || !entityData.waitTime) return;
      const attractionId = mappingLookup.get(
        `${entityData.source}:${entityData.externalId}`,
      );
      if (!attractionId) return;
      const result = await this.predictionDeviationService.checkDeviation(
        attractionId,
        entityData.waitTime,
      );
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
    } catch (_e) {}
  }

  private async trackDowntime(
    entityData: EntityLiveData,
    mappingLookup: Map<string, string>,
    timezone: string = "UTC",
    closingTime?: Date,
  ): Promise<void> {
    try {
      const attractionId = mappingLookup.get(
        `${entityData.source}:${entityData.externalId}`,
      );
      if (!attractionId) return;
      const currentStatus = entityData.status;
      const statusKey = `downtime:status:${attractionId}`;
      const downtimeStartKey = `downtime:start:${attractionId}`;
      const previousStatus = await this.redis.get(statusKey);
      await this.redis.set(statusKey, currentStatus, "EX", 3600);
      if (
        previousStatus === LiveStatus.OPERATING &&
        (currentStatus === LiveStatus.DOWN ||
          currentStatus === LiveStatus.CLOSED ||
          currentStatus === LiveStatus.REFURBISHMENT)
      ) {
        if (closingTime) {
          const msUntilClose = closingTime.getTime() - Date.now();
          if (msUntilClose / 1000 / 60 <= 60) return;
        }
        await this.redis.set(
          downtimeStartKey,
          Date.now().toString(),
          "EX",
          3600,
        );
      }
      if (
        (previousStatus === LiveStatus.DOWN ||
          previousStatus === LiveStatus.CLOSED ||
          previousStatus === LiveStatus.REFURBISHMENT) &&
        currentStatus === LiveStatus.OPERATING
      ) {
        const startTimeStr = await this.redis.get(downtimeStartKey);
        if (startTimeStr) {
          const downtimeMinutes = Math.round(
            (Date.now() - parseInt(startTimeStr)) / 60000,
          );
          if (downtimeMinutes > 0) {
            const dailyKey = `downtime:daily:${attractionId}:${getCurrentDateInTimezone(timezone)}`;
            const currentTotal = await this.redis.get(dailyKey);
            await this.redis.set(
              dailyKey,
              (parseInt(currentTotal || "0") + downtimeMinutes).toString(),
              "EX",
              25 * 3600,
            );
          }
          await this.redis.del(downtimeStartKey);
        }
      }
    } catch (_e) {}
  }

  private async writeHourlyHeartbeats(): Promise<number> {
    let totalHeartbeats = 0;
    try {
      const parks = await this.parksService.findAll();
      for (const park of parks) {
        try {
          const todaySchedule = await this.parksService.getTodaySchedule(
            park.id,
          );
          const operating = todaySchedule.find(
            (s) => s.scheduleType === "OPERATING" && s.openingTime,
          );
          if (!operating?.openingTime) continue;
          const now = new Date();
          const open = new Date(operating.openingTime);
          const close = operating.closingTime
            ? new Date(operating.closingTime)
            : null;
          if (now < open || (close && now > close)) continue;
          const attractions = await this.attractionsService
            .getRepository()
            .find({
              where: { parkId: park.id },
              select: ["id", "name", "externalId"],
            });
          if (attractions.length === 0) continue;
          const latestData = await this.queueDataRepository
            .createQueryBuilder("qd")
            .where("qd.attractionId IN (:...ids)", {
              ids: attractions.map((a) => a.id),
            })
            .andWhere("qd.queueType = :qt", { queueType: QueueType.STANDBY })
            .distinctOn(["qd.attractionId"])
            .orderBy("qd.attractionId", "ASC")
            .addOrderBy("qd.timestamp", "DESC")
            .getMany();
          const latestMap = new Map(latestData.map((d) => [d.attractionId, d]));
          for (const a of attractions) {
            const last = latestMap.get(a.id);
            if (!last || now.getTime() - last.timestamp.getTime() > 3600000) {
              await this.queueDataRepository.save({
                attractionId: a.id,
                queueType: QueueType.STANDBY,
                status: last ? last.status : LiveStatus.CLOSED,
                waitTime: last ? last.waitTime : 0,
                lastUpdated: now,
              });
              totalHeartbeats++;
            }
          }
        } catch (_e) {}
      }
    } catch (_e) {}
    return totalHeartbeats;
  }
}
