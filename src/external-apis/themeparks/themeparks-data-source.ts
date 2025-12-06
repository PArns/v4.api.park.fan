import { Injectable, Logger } from "@nestjs/common";
import {
  IDataSource,
  ParkMetadata,
  LiveDataResponse,
  EntityType,
  DataRichness,
  EntityLiveData,
  LiveStatus,
} from "../data-sources/interfaces/data-source.interface";
import { ThemeParksClient } from "./themeparks.client";
import { EntityType as TPEntityType } from "./themeparks.types";

/**
 * ThemeParks.wiki Data Source
 *
 * Wraps existing ThemeParksClient as IDataSource implementation
 *
 * Completeness: 10/10 (RICHEST)
 * - ✅ Wait times
 * - ✅ Schedules
 * - ✅ Shows
 * - ✅ Restaurants
 * - ✅ Forecasts
 * - ✅ Multiple queue types
 * - ❌ Lands (Queue-Times exclusive)
 */
@Injectable()
export class ThemeParksDataSource implements IDataSource {
  private readonly logger = new Logger(ThemeParksDataSource.name);

  readonly name = "themeparks-wiki";
  readonly completeness = 10;

  constructor(private readonly client: ThemeParksClient) {}

  async fetchAllParks(): Promise<ParkMetadata[]> {
    const destinations = await this.client.getDestinations();

    const allParks: ParkMetadata[] = [];

    // For each destination, fetch detailed parks
    for (const destination of destinations.destinations) {
      for (const parkSummary of destination.parks) {
        // Fetch detailed park entity to get location/timezone
        try {
          const parkEntity = await this.client.getEntity(parkSummary.id);

          allParks.push({
            externalId: parkEntity.id,
            source: this.name,
            name: parkEntity.name,
            timezone: parkEntity.timezone,
            latitude: parkEntity.location?.latitude ?? undefined,
            longitude: parkEntity.location?.longitude ?? undefined,
            destinationId: parkEntity.destinationId,
          });
        } catch (error) {
          this.logger.warn(
            `Failed to fetch park ${parkSummary.name}: ${error}`,
          );
        }
      }
    }

    this.logger.log(`Fetched ${allParks.length} parks from ThemeParks.wiki`);
    return allParks;
  }

  async fetchParkLiveData(externalId: string): Promise<LiveDataResponse> {
    const liveDataArray = await this.client.getParkLiveData(externalId);

    const entities: EntityLiveData[] = liveDataArray.map((entity) => ({
      externalId: entity.id,
      source: this.name,
      entityType: this.mapEntityType(entity.entityType),
      name: entity.name,
      status: this.mapStatus(entity.status),
      waitTime: entity.queue?.STANDBY?.waitTime ?? undefined,
      lastUpdated: entity.lastUpdated,
      queue: entity.queue
        ? Object.entries(entity.queue).map(([queueType, data]) => ({
            queueType,
            state: data.state,
            returnStart: data.returnStart,
            returnEnd: data.returnEnd,
          }))
        : undefined,
      showtimes: entity.showtimes,
      operatingHours: entity.operatingHours,
      diningAvailability: entity.diningAvailability ? "available" : undefined,
    }));

    return {
      source: this.name,
      parkExternalId: externalId,
      entities,
      fetchedAt: new Date(),
    };
  }

  private mapEntityType(tpType: TPEntityType): EntityType {
    switch (tpType) {
      case TPEntityType.ATTRACTION:
        return EntityType.ATTRACTION;
      case TPEntityType.SHOW:
        return EntityType.SHOW;
      case TPEntityType.RESTAURANT:
        return EntityType.RESTAURANT;
      default:
        return EntityType.ATTRACTION;
    }
  }

  private mapStatus(tpStatus: string): LiveStatus {
    switch (tpStatus) {
      case "OPERATING":
        return LiveStatus.OPERATING;
      case "DOWN":
        return LiveStatus.DOWN;
      case "CLOSED":
        return LiveStatus.CLOSED;
      case "REFURBISHMENT":
        return LiveStatus.REFURBISHMENT;
      default:
        return LiveStatus.CLOSED;
    }
  }

  supportsEntityType(type: EntityType): boolean {
    return [
      EntityType.ATTRACTION,
      EntityType.SHOW,
      EntityType.RESTAURANT,
    ].includes(type);
  }

  getDataRichness(): DataRichness {
    return {
      hasSchedules: true,
      hasShows: true,
      hasRestaurants: true,
      hasLands: false,
      hasForecasts: true,
      hasMultipleQueueTypes: true,
    };
  }

  async fetchParkEntities(
    externalId: string,
  ): Promise<
    import("../data-sources/interfaces/data-source.interface").EntityMetadata[]
  > {
    try {
      const response = await this.client.getEntityChildren(externalId);
      const children = response.children || [];

      return children.map((child) => ({
        externalId: child.id,
        source: this.name,
        entityType: this.mapEntityType(child.entityType),
        name: child.name,
        latitude: child.location?.latitude ?? undefined,
        longitude: child.location?.longitude ?? undefined,
      }));
    } catch (error) {
      this.logger.error(
        `Failed to fetch entities for park ${externalId}: ${error}`,
      );
      return [];
    }
  }

  async isHealthy(): Promise<boolean> {
    try {
      await this.client.getDestinations();
      return true;
    } catch {
      return false;
    }
  }
}
