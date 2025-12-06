import { Injectable, Logger } from "@nestjs/common";
import {
  IDataSource,
  ParkMetadata,
  LiveDataResponse,
  EntityType,
  DataRichness,
  EntityLiveData,
  LandData,
  LiveStatus,
} from "../data-sources/interfaces/data-source.interface";
import { QueueTimesClient } from "./queue-times.client";

/**
 * Queue-Times.com Data Source
 *
 * Implements IDataSource for Queue-Times.com API
 *
 * Completeness: 5/10
 * - ✅ Wait times
 * - ✅ Lands (unique feature)
 * - ❌ Schedules
 * - ❌ Shows
 * - ❌ Restaurants
 * - ❌ Forecasts
 */
@Injectable()
export class QueueTimesDataSource implements IDataSource {
  private readonly logger = new Logger(QueueTimesDataSource.name);

  readonly name = "queue-times";
  readonly completeness = 5;

  constructor(private readonly client: QueueTimesClient) {}

  async fetchAllParks(): Promise<ParkMetadata[]> {
    const parkGroups = await this.client.getParks();

    const allParks: ParkMetadata[] = [];

    for (const group of parkGroups) {
      for (const park of group.parks) {
        allParks.push({
          externalId: park.id.toString(),
          source: this.name,
          name: park.name,
          country: park.country,
          continent: park.continent,
          timezone: park.timezone,
          latitude: parseFloat(park.latitude),
          longitude: parseFloat(park.longitude),
        });
      }
    }

    this.logger.log(`Fetched ${allParks.length} parks from Queue-Times`);
    return allParks;
  }

  async fetchParkLiveData(externalId: string): Promise<LiveDataResponse> {
    const parkId = parseInt(externalId, 10);
    const queueData = await this.client.getParkQueueTimes(parkId);

    const entities: EntityLiveData[] = [];
    const lands: LandData[] = [];

    // Process lands and their rides
    for (const land of queueData.lands) {
      // Store land metadata
      lands.push({
        externalId: land.id.toString(),
        source: this.name,
        name: land.name,
        attractions: land.rides.map((r) => r.id.toString()),
      });

      // Process rides in this land
      for (const ride of land.rides) {
        entities.push(this.transformRideToEntity(ride, land.id.toString()));
      }
    }

    // Process rides not in lands
    for (const ride of queueData.rides) {
      entities.push(this.transformRideToEntity(ride, undefined));
    }

    return {
      source: this.name,
      parkExternalId: externalId,
      entities,
      lands,
      fetchedAt: new Date(),
    };
  }

  private transformRideToEntity(
    ride: any,
    landExternalId?: string,
  ): EntityLiveData {
    return {
      externalId: ride.id.toString(),
      source: this.name,
      entityType: EntityType.ATTRACTION,
      name: ride.name,
      status: ride.is_open ? LiveStatus.OPERATING : LiveStatus.CLOSED,
      waitTime: ride.is_open ? ride.wait_time : undefined,
      landExternalId,
      lastUpdated: ride.last_updated,
    };
  }

  supportsEntityType(type: EntityType): boolean {
    return type === EntityType.ATTRACTION;
  }

  getDataRichness(): DataRichness {
    return {
      hasSchedules: false,
      hasShows: false,
      hasRestaurants: false,
      hasLands: true, // UNIQUE!
      hasForecasts: false,
      hasMultipleQueueTypes: false,
    };
  }

  async fetchParkEntities(
    externalId: string,
  ): Promise<
    import("../data-sources/interfaces/data-source.interface").EntityMetadata[]
  > {
    try {
      const parkId = parseInt(externalId, 10);
      const queueData = await this.client.getParkQueueTimes(parkId);

      const entities: import("../data-sources/interfaces/data-source.interface").EntityMetadata[] =
        [];

      // Process rides from lands
      for (const land of queueData.lands) {
        for (const ride of land.rides) {
          entities.push({
            externalId: ride.id.toString(),
            source: this.name,
            entityType: EntityType.ATTRACTION,
            name: ride.name,
            // QT API typically doesn't provide lat/lon in live data response
            // Check if it's available in ride object?
            latitude: undefined,
            longitude: undefined,
          });
        }
      }

      // Process orphan rides
      for (const ride of queueData.rides) {
        entities.push({
          externalId: ride.id.toString(),
          source: this.name,
          entityType: EntityType.ATTRACTION,
          name: ride.name,
          latitude: undefined,
          longitude: undefined,
        });
      }

      return entities;
    } catch (error) {
      this.logger.error(
        `Failed to fetch entities for park ${externalId}: ${error}`,
      );
      return [];
    }
  }

  async isHealthy(): Promise<boolean> {
    try {
      await this.client.getParks();
      return true;
    } catch {
      return false;
    }
  }
}
