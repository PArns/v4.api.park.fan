/**
 * Rope-Drop Recommendation Types
 *
 * "Rope drop" = arriving right at park opening to ride a headliner before the
 * crowds build, when its wait is at the day's trough. This type carries the
 * precomputed recommendation surfaced on ride/park responses.
 *
 * Model (two layers — see docs / plan):
 * - **Shape** (`rideByMinutesAfterOpen`, `bestSlotMinutesAfterOpen`): the
 *   opening-relative ratio curve. Empirically season-stable, so it is pooled
 *   over the full available history. Expressed as minutes-after-open (timezone-
 *   free); the API also resolves them to concrete UTC instants.
 * - **Levels** (`busyPeak`, `openWait`, `savings`): absolute minutes, computed
 *   on a trailing window and recomputed daily so they track the current season.
 *   Split into weekend/weekday buckets (the busy-selection mechanism).
 *
 * All concrete timestamps are UTC ISO 8601 — no local clock strings.
 */

/** Per-day-type level bucket (absolute minutes, trailing-window). */
export interface RopeDropDayBucket {
  /** Median wait in the first 15-min slot after opening (the rope-drop wait). */
  openWait: number;
  /** Median daily peak wait on a full day (the line you skip). */
  busyPeak: number;
  /** busyPeak − openWait (minutes saved by rope-dropping). */
  savings: number;
}

/**
 * The persisted/cached rope-drop recommendation — everything except the UTC
 * instants, which are resolved at serve time from the park's schedule.
 */
export interface RopeDropStored {
  /** Whether this headliner is worth rope-dropping (busy/long enough + big gap). */
  worth: boolean;
  /** Tier of the recommendation when worthy, else null. */
  strength: "high" | "moderate" | null;
  /** Data-quality indicator from the number of operating days in the window. */
  confidence: "high" | "medium" | "low";

  // Headline levels (the busier of the weekend/weekday buckets).
  /** Daily peak wait you avoid by rope-dropping (minutes). */
  busyPeak: number;
  /** Typical wait at opening (minutes). */
  openWait: number;
  /** busyPeak − openWait (minutes). */
  savings: number;

  // Shape (opening-relative offsets, pooled over history).
  /** Minutes after opening until the wait crosses 50% of the peak (advantage window). */
  rideByMinutesAfterOpen: number;
  /** Minutes after opening of the day's absolute lowest wait (often the evening). */
  bestSlotMinutesAfterOpen: number;

  /** Level breakdown by day type. */
  byDaytype: {
    weekend: RopeDropDayBucket;
    weekday: RopeDropDayBucket;
  };
}

/** The rope-drop recommendation as returned on the API (UTC instants resolved). */
export interface RopeDropInfo extends RopeDropStored {
  // Shape resolved to concrete UTC instants for the next operating day.
  /** `openingTime + rideByMinutesAfterOpen` for the next operating day (UTC ISO), or null. */
  rideByUtc: string | null;
  /** `openingTime + bestSlotMinutesAfterOpen` for the next operating day (UTC ISO), or null. */
  bestSlotUtc: string | null;
}
