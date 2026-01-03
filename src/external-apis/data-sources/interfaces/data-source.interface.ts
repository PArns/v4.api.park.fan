/**
 * Data Source Interface
 *
 * Unified interface for all external data sources (ThemeParks.wiki, Queue-Times, etc.)
 * Enables polymorphic handling of multiple sources in the orchestrator.
 */

export interface IDataSource {
  /**
   * Unique identifier for this data source
   */
  readonly name: string;

  /**
   * Data completeness score (1-10)
   * Higher = more comprehensive data (schedules, shows, restaurants, etc.)
   */
  readonly completeness: number;

  /**
   * Fetch all parks from this data source
   */
  fetchAllParks(): Promise<ParkMetadata[]>;

  /**
   * Fetch live data for a specific park
   * @param externalId - The park's ID in THIS data source's system
   */
  fetchParkLiveData(externalId: string): Promise<LiveDataResponse>;

  /**
   * Check if this source supports a specific entity type
   */
  supportsEntityType(type: EntityType): boolean;

  /**
   * Get detailed information about what data this source provides
   */
  getDataRichness(): DataRichness;

  /**
   * Fetch all entities (attractions, shows, restaurants) for a specific park
   * Used for entity matching/seeding
   * @param externalId - The park's ID in THIS data source's system
   */
  fetchParkEntities(externalId: string): Promise<EntityMetadata[]>;

  /**
   * Check if the external API is currently accessible
   */
  isHealthy(): Promise<boolean>;
}

/**
 * Park metadata from external source
 */
export interface ParkMetadata {
  externalId: string;
  source: string; // 'themeparks-wiki' | 'queue-times'
  name: string;
  country?: string;
  continent?: string;
  timezone?: string;
  latitude?: number;
  longitude?: number;
  destinationId?: string; // Only for ThemeParks.wiki
}

/**
 * Entity metadata for matching/seeding
 */
export interface EntityMetadata {
  externalId: string;
  source: string;
  entityType: EntityType;
  name: string;
  latitude?: number;
  longitude?: number;
  landName?: string;
  landId?: string;
}

/**
 * Live data response from external source
 */
export interface LiveDataResponse {
  source: string;
  parkExternalId: string;
  entities: EntityLiveData[];
  lands?: LandData[]; // Only if source supports lands
  crowdLevel?: number; // Only if source supports crowd level (Wartezeiten)
  operatingHours?: OperatingWindow[]; // Park operating hours
  fetchedAt: Date;
}

/**
 * Operating window for a park
 */
export interface OperatingWindow {
  open: string; // ISO timestamp or time string
  close: string; // ISO timestamp or time string
  type: string; // e.g., "OPERATING", "EXTRA_HOURS"
}

/**
 * Entity live data (attraction, show, restaurant)
 */
export interface EntityLiveData {
  externalId: string;
  source: string;
  entityType: EntityType;
  name: string;
  status: LiveStatus;
  waitTime?: number;
  landExternalId?: string; // For Queue-Times
  lastUpdated?: string;
  latitude?: number; // Optional location for verifying matches
  longitude?: number;

  // Queue-specific data
  // UPDATED: Support object format from ThemeParks.wiki for multiple queue types
  // Structure: { STANDBY: {...}, SINGLE_RIDER: {...}, RETURN_TIME: {...}, etc }
  // This enables storing waitTime, returnStart/End, price, boarding group data
  queue?: any; // QueueData from themeparks.types (avoid circular dependency)

  // Show-specific data
  showtimes?: Showtime[];

  // Restaurant-specific data
  diningAvailability?: string;
}

export interface Showtime {
  type: string;
  startTime: string;
  endTime?: string;
}

/**
 * Land/Area data (themed zones within parks)
 */
export interface LandData {
  externalId: string;
  source: string;
  name: string;
  attractions: string[]; // Array of attraction external IDs
}

/**
 * Entity types
 */
export enum EntityType {
  ATTRACTION = "ATTRACTION",
  SHOW = "SHOW",
  RESTAURANT = "RESTAURANT",
}

/**
 * Entity statuses
 */
export enum LiveStatus {
  OPERATING = "OPERATING",
  DOWN = "DOWN",
  CLOSED = "CLOSED",
  REFURBISHMENT = "REFURBISHMENT",
}

/**
 * Data richness capabilities
 */
export interface DataRichness {
  hasSchedules: boolean; // Operating hours
  hasShows: boolean; // Show entities
  hasRestaurants: boolean; // Restaurant entities
  hasLands: boolean; // Themed areas
  hasForecasts: boolean; // Future wait time predictions
  hasMultipleQueueTypes: boolean; // STANDBY, SINGLE_RIDER, RETURN_TIME, etc.
}
