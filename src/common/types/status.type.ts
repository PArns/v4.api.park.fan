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
 * - UNKNOWN: No source tells us. Not a closure — an absence of information.
 *
 * UNKNOWN was already being served before it was listed here: rides in a park
 * whose wait times we cannot read at all (Hansa-Park) have carried it for
 * months while this union claimed four values. That drift is the same one that
 * kept `unknown` out of the published crowd-level contract, so: anything that
 * emits a status reads it from this type, and the Swagger enum comes from
 * ATTRACTION_STATUS_VALUES rather than a hand-written list.
 */
export const ATTRACTION_STATUS_VALUES = [
  "OPERATING",
  "CLOSED",
  "DOWN",
  "REFURBISHMENT",
  "UNKNOWN",
] as const;

export type AttractionStatus = (typeof ATTRACTION_STATUS_VALUES)[number];
