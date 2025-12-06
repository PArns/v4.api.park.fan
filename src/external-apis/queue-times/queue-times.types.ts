/**
 * Queue-Times.com API Types
 *
 * Based on API documentation: https://queue-times.com/pages/api
 */

/**
 * Response from /parks.json
 */
export interface QueueTimesParksResponse {
  id: number;
  name: string;
  parks: QueueTimesPark[];
}

/**
 * Park data from Queue-Times
 */
export interface QueueTimesPark {
  id: number;
  name: string;
  country: string;
  continent: string;
  latitude: string;
  longitude: string;
  timezone: string;
}

/**
 * Response from /parks/{id}/queue_times.json
 */
export interface QueueTimesParkQueueData {
  lands: QueueTimesLand[];
  rides: QueueTimesRide[]; // Rides not in lands
}

/**
 * Land (themed area) within a park
 */
export interface QueueTimesLand {
  id: number;
  name: string;
  rides: QueueTimesRide[];
}

/**
 * Ride/Attraction with wait time
 */
export interface QueueTimesRide {
  id: number;
  name: string;
  is_open: boolean;
  wait_time: number; // Minutes, 0 if closed
  last_updated: string; // ISO 8601 timestamp (UTC)
}
