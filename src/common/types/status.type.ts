/**
 * Operational Status Types
 *
 * Defines the operational state of parks and attractions
 */

/**
 * Park Status
 *
 * Parks have simpler status: either operating or closed
 */
export type ParkStatus = "OPERATING" | "CLOSED";

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
