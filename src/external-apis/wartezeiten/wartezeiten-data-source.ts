import { Injectable, Logger } from "@nestjs/common";
import {
  IDataSource,
  ParkMetadata,
  LiveDataResponse,
  EntityType,
  DataRichness,
  EntityLiveData,
  LiveStatus,
  EntityMetadata,
} from "../data-sources/interfaces/data-source.interface";
import { WartezeitenClient } from "./wartezeiten.client";
import { WartezeitenAttractionStatus } from "./wartezeiten.types";
import { getCountryISOCode } from "../../common/constants/country-codes.constant";
import { getTimezoneForCountry } from "../../common/utils/timezone.util";
import { isWartezeitenParkExcluded } from "./wartezeiten-exclusions";

/**
 * Wartezeiten.app Data Source
 *
 * Implements IDataSource for Wartezeiten.app API
 *
 * Completeness: 6/10 (between Queue-Times=5 and Wiki=10)
 * - ✅ Wait times
 * - ✅ Opening times
 * - ✅ Crowd level (UNIQUE!)
 * - ❌ Schedules (only daily open/close)
 * - ❌ Shows
 * - ❌ Restaurants
 * - ❌ Lands
 * - ❌ Multiple queue types
 */
@Injectable()
export class WartezeitenDataSource implements IDataSource {
  private readonly logger = new Logger(WartezeitenDataSource.name);

  readonly name = "wartezeiten-app";
  readonly completeness = 6;

  constructor(private readonly client: WartezeitenClient) {}

  async fetchAllParks(): Promise<ParkMetadata[]> {
    const parks = await this.client.getParks("en");

    // Filter out excluded parks
    const filteredParks = parks.filter((park) => {
      if (isWartezeitenParkExcluded(park.name)) {
        this.logger.debug(`Excluding park from Wartezeiten: ${park.name}`);
        return false;
      }
      return true;
    });

    const allParks: ParkMetadata[] = filteredParks.map((park) => {
      let name = park.name;
      let countryCode = getCountryISOCode(park.land);

      // Smart Extraction: Check for "Name (XX)" pattern
      // Wartezeiten often uses this format: "Alton Towers (GB)", "Europa-Park (DE)"
      // We extract the country code and clean the name for better matching
      const countryMatch = name.match(/^(.*)\s+\(([A-Z]{2})\)$/);
      if (countryMatch) {
        name = countryMatch[1].trim(); // Clean name: "Alton Towers"
        const extractedCode = countryMatch[2]; // Extracted country: "GB"

        if (!countryCode) {
          countryCode = extractedCode;
        } else if (countryCode !== extractedCode) {
          // If extracted differs from explicit land, log it (but keep explicit land usually)
          // In this case, the suffix is likely more accurate for these specific entries
          this.logger.debug(
            `Country mismatch for ${name}: land=${countryCode}, suffix=${extractedCode}. Using suffix.`,
          );
          countryCode = extractedCode;
        }
      }

      return {
        externalId: park.uuid, // Use UUID (more stable than string ID)
        source: this.name,
        name: name,
        country: countryCode || park.land, // Convert to ISO code if possible
        timezone: countryCode
          ? (getTimezoneForCountry(countryCode) ?? undefined)
          : undefined,
        // Note: No lat/lng or continent in Wartezeiten API
        // These will be enriched from Wiki data during matching
      };
    });

    this.logger.log(`Fetched ${allParks.length} parks from Wartezeiten.app`);
    return allParks;
  }

  async fetchParkLiveData(externalId: string): Promise<LiveDataResponse> {
    // OPTIMIZATION: Only fetch wait times during live sync
    // CrowdLevel and OpeningTimes don't change frequently - fetch separately/less often
    // This reduces API calls from 3 per park to 1 per park
    // Result: 30 parks × 1 call = 30 calls/sync (instead of 90) = 6 calls/min
    const waitTimes = await this.client.getWaitTimes(externalId, "en");

    const entities: EntityLiveData[] = waitTimes.map((attraction) => ({
      externalId: attraction.uuid,
      source: this.name,
      entityType: EntityType.ATTRACTION, // Wartezeiten only has attractions
      name: attraction.name,
      status: this.mapStatus(attraction.status),
      waitTime: attraction.waitingtime > 0 ? attraction.waitingtime : undefined,
      lastUpdated: attraction.datetime,
    }));

    return {
      source: this.name,
      parkExternalId: externalId,
      entities,
      // Note: crowdLevel and operatingHours can be fetched separately
      // via dedicated jobs that run less frequently (e.g., hourly)
      fetchedAt: new Date(),
    };
  }

  async fetchParkEntities(externalId: string): Promise<EntityMetadata[]> {
    try {
      const waitTimes = await this.client.getWaitTimes(externalId, "en");

      return waitTimes.map((attraction) => ({
        externalId: attraction.uuid,
        source: this.name,
        entityType: EntityType.ATTRACTION,
        name: attraction.name,
        // No lat/lng in Wartezeiten API
      }));
    } catch (error) {
      this.logger.error(
        `Failed to fetch entities for park ${externalId}: ${error}`,
      );
      return [];
    }
  }

  /**
   * Map Wartezeiten status to our LiveStatus enum
   */
  private mapStatus(status: WartezeitenAttractionStatus): LiveStatus {
    switch (status) {
      case WartezeitenAttractionStatus.OPENED:
        return LiveStatus.OPERATING;

      case WartezeitenAttractionStatus.VIRTUAL_QUEUE:
        // Virtual queue means attraction is operating, just with special queue
        return LiveStatus.OPERATING;

      case WartezeitenAttractionStatus.MAINTENANCE:
        return LiveStatus.REFURBISHMENT;

      case WartezeitenAttractionStatus.CLOSED_ICE:
      case WartezeitenAttractionStatus.CLOSED_WEATHER:
      case WartezeitenAttractionStatus.CLOSED:
        return LiveStatus.CLOSED;

      default:
        this.logger.warn(`Unknown Wartezeiten status: ${status}`);
        return LiveStatus.CLOSED;
    }
  }

  supportsEntityType(type: EntityType): boolean {
    // Wartezeiten only has attractions (no shows or restaurants)
    return type === EntityType.ATTRACTION;
  }

  getDataRichness(): DataRichness {
    return {
      hasSchedules: true, // Only daily open/close (TODAY only)
      hasShows: false,
      hasRestaurants: false,
      hasLands: false,
      hasForecasts: false,
      hasMultipleQueueTypes: false,
    };
  }

  async isHealthy(): Promise<boolean> {
    return this.client.isHealthy();
  }
}
