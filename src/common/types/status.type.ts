/**
 * Operational Status Types
 *
 * Defines the operational state of parks and attractions
 */

/**
 * Park Status
 *
 * - OPERATING: Park is open
 * - CLOSED: Park is closed (confirmed)
 * - UNKNOWN: Used in calendar when no schedule data from source yet (not yet published or placeholder)
 */
export type ParkStatus = "OPERATING" | "CLOSED" | "UNKNOWN";

/**
 * Attraction Status
 *
 * Attractions have more detailed statuses including maintenance and downtime
 * - OPERATING: Attraction is running normally
 * - CLOSED: Attraction is closed (scheduled closure)
 * - DOWN: Attraction is temporarily down (unexpected)
 * - REFURBISHMENT: Attraction is under maintenance/renovation
 */
export type AttractionStatus =
  | "OPERATING"
  | "CLOSED"
  | "DOWN"
  | "REFURBISHMENT";
