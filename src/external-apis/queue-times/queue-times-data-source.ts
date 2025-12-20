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

  constructor(private readonly client: QueueTimesClient) { }

  async fetchAllParks(): Promise<ParkMetadata[]> {
    const parkGroups = await this.client.getParks();

    const allParks: ParkMetadata[] = [];

    for (const group of parkGroups) {
      for (const park of group.parks) {
        allParks.push({
          externalId: `qt-park-${park.id}`,
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
    const parkId = this.extractParkId(externalId);
    const queueData = await this.client.getParkQueueTimes(parkId);

    const entities: EntityLiveData[] = [];
    const lands: LandData[] = [];

    // Process lands and their rides
    for (const land of queueData.lands) {
      // Store land metadata
      lands.push({
        externalId: `qt-land-${land.id}`,
        source: this.name,
        name: land.name,
        attractions: land.rides.map((r) => `qt-ride-${r.id}`),
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
      externalId: `qt-ride-${ride.id}`,
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
      const parkId = this.extractParkId(externalId);
      const queueData = await this.client.getParkQueueTimes(parkId);

      const entities: import("../data-sources/interfaces/data-source.interface").EntityMetadata[] =
        [];

      // Process rides from lands
      for (const land of queueData.lands) {
        for (const ride of land.rides) {
          entities.push({
            externalId: `qt-ride-${ride.id}`,
            source: this.name,
            entityType: EntityType.ATTRACTION,
            name: ride.name,
            // QT API typically doesn't provide lat/lon in live data response
            // Check if it's available in ride object?
            latitude: undefined,
            longitude: undefined,
            landName: land.name,
            landId: `qt-land-${land.id}`,
          });
        }
      }

      // Process orphan rides
      for (const ride of queueData.rides) {
        entities.push({
          externalId: `qt-ride-${ride.id}`,
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

  /**
   * Extract numeric park ID from prefixed external ID
   * @param externalId - Prefixed ID like "qt-park-56" or legacy "56"
   * @returns Numeric park ID (56)
   */
  private extractParkId(externalId: string): number {
    // Handle prefixed IDs
    if (externalId.startsWith("qt-park-")) {
      return parseInt(externalId.replace("qt-park-", ""), 10);
    }
    // Fallback for legacy non-prefixed IDs (backward compatibility)
    return parseInt(externalId, 10);
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
