/**
 * Where a visitor stands when it rains, decided by a person.
 *
 * `indoor` — ride and queue are under a roof. `outdoor` — neither is.
 * `covered_queue` — the queue is covered, the ride itself runs outside, which
 * is the case that matters for a rain plan: you stay dry while waiting and
 * may still be sent away when the ride stops for weather.
 *
 * No feed we ingest carries this (docs/product/attraction-metadata-sources.md
 * in the frontend repo): ThemeParks.wiki, Queue-Times and Wartezeiten.app have
 * nothing, RCDB and park sites are off-limits by their terms. So every value
 * is an editor's, and the source goes into the `reason` of the curation audit
 * row that writes it.
 *
 * Null is not `outdoor`. It means nobody has looked, which is the state of the
 * whole catalogue on the day the column lands — a rain plan that reads null as
 * "outside" would send people away from rides that are dry, and one that
 * reads it as "inside" would send them into the rain. The rule is
 * docs/rules/absent-facts.md.
 *
 * Lower-case like `CROWD_LEVEL_VALUES`, because the issue that asked for the
 * field (PAR-424) named the values that way.
 */
export const INDOOR_OUTDOOR_VALUES = [
  "indoor",
  "outdoor",
  "covered_queue",
] as const;

export type IndoorOutdoor = (typeof INDOOR_OUTDOOR_VALUES)[number];
