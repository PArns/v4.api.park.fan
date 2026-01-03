/**
 * TypeScript Type Definitions for ThemeParks.wiki API
 *
 * The official 'themeparks' SDK does not provide TypeScript definitions.
 * These types are based on API analysis from https://api.themeparks.wiki/docs/v1/
 *
 * API Response Analysis (2025-11-17):
 * - GET /v1/destinations → { destinations: DestinationResponse[] }
 * - GET /v1/entity/{id} → EntityResponse
 * - GET /v1/entity/{id}/children → { children: EntityResponse[] }
 * - GET /v1/entity/{id}/live → EntityLiveResponse
 * - GET /v1/entity/{id}/schedule → { schedule: ScheduleEntry[] }
 */

// ===========================
// Entity Types (from API)
// ===========================

export enum EntityType {
  DESTINATION = "DESTINATION",
  PARK = "PARK",
  ATTRACTION = "ATTRACTION",
  SHOW = "SHOW",
  RESTAURANT = "RESTAURANT",
  HOTEL = "HOTEL",
}

export enum LiveStatus {
  OPERATING = "OPERATING",
  DOWN = "DOWN",
  CLOSED = "CLOSED",
  REFURBISHMENT = "REFURBISHMENT",
}

export enum QueueType {
  STANDBY = "STANDBY",
  SINGLE_RIDER = "SINGLE_RIDER",
  RETURN_TIME = "RETURN_TIME",
  PAID_RETURN_TIME = "PAID_RETURN_TIME",
  BOARDING_GROUP = "BOARDING_GROUP",
  PAID_STANDBY = "PAID_STANDBY",
  VIRTUAL_QUEUE = "VIRTUAL_QUEUE",
}

export enum ScheduleType {
  OPERATING = "OPERATING",
  TICKETED_EVENT = "TICKETED_EVENT",
  PRIVATE_EVENT = "PRIVATE_EVENT",
  EXTRA_HOURS = "EXTRA_HOURS",
  INFO = "INFO",
}

// ===========================
// Common Types
// ===========================

export interface LocationData {
  latitude: number | null;
  longitude: number | null;
}

export interface PriceData {
  amount: number;
  currency: string;
  formatted?: string;
}

// ===========================
// Destinations API Response
// ===========================

/**
 * Response from GET /v1/destinations
 *
 * Example:
 * {
 *   "destinations": [
 *     {
 *       "id": "259cf011-6195-42dd-bfdb-640969e0bfb9",
 *       "name": "Guangzhou Chimelong Tourist Resort",
 *       "slug": "chimelongguangzhou",
 *       "parks": [
 *         { "id": "73436fe5...", "name": "Chimelong Paradise" }
 *       ]
 *     }
 *   ]
 * }
 */
export interface DestinationsApiResponse {
  destinations: DestinationResponse[];
}

export interface DestinationResponse {
  id: string;
  name: string;
  slug: string;
  parks: ParkSummary[];
}

export interface ParkSummary {
  id: string;
  name: string;
}

// ===========================
// Entity API Response
// ===========================

/**
 * Response from GET /v1/entity/{id}
 *
 * Example (Park):
 * {
 *   "id": "73436fe5-1f14-400f-bfbf-ab6766269e70",
 *   "name": "Chimelong Paradise",
 *   "location": { "latitude": 23.005, "longitude": 113.327 },
 *   "parentId": "259cf011-6195-42dd-bfdb-640969e0bfb9",
 *   "timezone": "Asia/Shanghai",
 *   "entityType": "PARK",
 *   "destinationId": "259cf011-6195-42dd-bfdb-640969e0bfb9",
 *   "externalId": "park_GZ51"
 * }
 */
export interface EntityResponse {
  id: string;
  name: string;
  entityType: EntityType;
  slug: string | null;
  parentId?: string;
  destinationId?: string;
  externalId?: string;
  location?: LocationData;
  timezone?: string;
  // Detailed fields (available in /entity/{id} but not /children)
  cuisines?: string[];
  attractionType?: string;
}

/**
 * Response from GET /v1/entity/{id}/children
 */
export interface EntityChildrenResponse {
  children: EntityResponse[];
}

// ===========================
// Live Data API Response
// ===========================

/**
 * Response from GET /v1/entity/{id}/live
 *
 * Contains real-time data: wait times, status, showtimes, etc.
 */
export interface EntityLiveResponse {
  id: string;
  name: string;
  entityType: EntityType;
  parkId?: string;
  externalId?: string;
  status: LiveStatus;
  lastUpdated: string;
  queue?: QueueData;
  forecast?: ForecastData[];
  showtimes?: ShowtimeData[];
  operatingHours?: OperatingHoursData[];
  diningAvailability?: DiningAvailabilityData;
}

// ===========================
// Queue Data Types
// ===========================

export interface QueueData {
  [QueueType.STANDBY]?: StandbyQueue;
  [QueueType.SINGLE_RIDER]?: StandbyQueue;
  [QueueType.RETURN_TIME]?: ReturnTimeQueue;
  [QueueType.PAID_RETURN_TIME]?: PaidReturnTimeQueue;
  [QueueType.BOARDING_GROUP]?: BoardingGroupQueue;
  [QueueType.PAID_STANDBY]?: StandbyQueue;
}

export interface StandbyQueue {
  waitTime: number; // minutes
}

export interface ReturnTimeQueue {
  state: string;
  returnStart: string; // ISO timestamp
  returnEnd: string; // ISO timestamp
}

export interface PaidReturnTimeQueue extends ReturnTimeQueue {
  price: PriceData;
}

export interface BoardingGroupQueue {
  allocationStatus: string;
  currentGroupStart?: number;
  currentGroupEnd?: number;
  nextAllocationTime?: string;
  estimatedWait?: number;
}

// ===========================
// Forecast Data
// ===========================

export interface ForecastData {
  time: string; // ISO timestamp
  waitTime: number;
  percentage?: number;
}

// ===========================
// Showtime Data
// ===========================

export interface ShowtimeData {
  type: string; // e.g., "Performance", "Parade"
  startTime: string; // ISO timestamp
  endTime?: string; // ISO timestamp
}

// ===========================
// Operating Hours
// ===========================

export interface OperatingHoursData {
  type: string; // e.g., "OPERATING"
  startTime: string;
  endTime: string;
}

// ===========================
// Dining Availability
// ===========================

export interface DiningAvailabilityData {
  partySize: number;
  waitTime: number;
}

// ===========================
// Schedule API Response
// ===========================

/**
 * Response from GET /v1/entity/{id}/schedule
 */
export interface EntityScheduleResponse {
  schedule: ScheduleEntry[];
}

export interface ScheduleEntry {
  date: string; // YYYY-MM-DD
  openingTime?: string;
  closingTime?: string;
  type: ScheduleType;
  description?: string;
  purchases?: PurchaseData[];
}

export interface PurchaseData {
  type: string;
  price: PriceData;
}
