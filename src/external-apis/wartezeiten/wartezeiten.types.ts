/**
 * TypeScript interfaces for Wartezeiten.app API
 *
 * API Documentation: https://api.wartezeiten.app/api.json
 * Base URL: https://api.wartezeiten.app
 */

/**
 * Park response from /v1/parks endpoint
 */
export interface WartezeitenParkResponse {
  id: string; // String ID (e.g., "phantasialand")
  uuid: string; // UUID (more stable identifier)
  name: string; // Park name
  land: string; // Country name (not ISO code!)
}

/**
 * Wait time response from /v1/waitingtimes endpoint
 */
export interface WartezeitenWaitTimeResponse {
  datetime: string; // ISO 8601 timestamp
  date: string; // Date in YYYY-MM-DD format
  time: string; // Time in HH:mm format
  code: string; // Numeric code as string
  uuid: string; // Attraction UUID
  waitingtime: number; // Wait time in minutes
  status: WartezeitenAttractionStatus; // Attraction status
  name: string; // Attraction name
}

/**
 * Attraction status
 */
export enum WartezeitenAttractionStatus {
  OPENED = "opened",
  VIRTUAL_QUEUE = "virtualqueue",
  MAINTENANCE = "maintenance",
  CLOSED_ICE = "closedice",
  CLOSED_WEATHER = "closedweather",
  CLOSED = "closed",
}

/**
 * Opening times response from /v1/openingtimes endpoint
 */
export interface WartezeitenOpeningTimeResponse {
  opened_today: boolean; // Is the park open today?
  open_from: string; // ISO 8601 timestamp
  closed_from: string; // ISO 8601 timestamp
}

/**
 * Crowd level response from /v1/crowdlevel endpoint
 */
export interface WartezeitenCrowdLevelResponse {
  crowd_level: number; // Float (e.g., 56.67)
  timestamp: string; // ISO 8601 timestamp
}

/**
 * API Error Response
 */
export interface WartezeitenErrorResponse {
  error?: string;
  message?: string;
}
