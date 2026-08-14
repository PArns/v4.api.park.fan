/**
 * Parks whose wait times we cannot read.
 *
 * Not "the feed is down right now" — that is a staleness question the live
 * queries already answer. This is the permanent case: the park publishes wait
 * times in a place no server can reach, so no amount of ingestion will ever
 * produce a number for it.
 *
 * Why it has to be curated rather than derived: from the outside, a park with
 * no source and a park that is simply shut for the night look identical. Every
 * park in the catalog reports zero rides operating and an empty `queues` array
 * at 3 a.m. The difference is knowledge about where the park publishes, which
 * exists nowhere in the data — so it is written down here, with the evidence
 * that put it on the list.
 *
 * What the API does with it (see ParkIntegrationService):
 * - the optimistic "park is open, so assume the ride is too" fallback is
 *   skipped — the rides go to UNKNOWN instead of a fabricated OPERATING;
 * - crowd levels read `unknown` rather than the `very_low` that a park with no
 *   wait times otherwise collapses to;
 * - ML predictions are withheld, the same way they are for a closed park.
 *
 * The frontend reads `liveWaitTimes` off the park response and explains the
 * gap to the visitor. It must not re-derive this list.
 *
 * Keyed by citySlug + parkSlug because park slugs are not globally unique
 * ("disneyland-park" exists in Anaheim and in Paris), matching the convention
 * in MANUAL_ATTRACTION_METADATA.
 */

/**
 * Why a park's wait times are unreadable. The frontend translates these; the
 * strings are contract, so renaming one is a breaking change.
 *
 * - `in_park_app_only`: the park runs its own app and serves wait times only
 *   to devices inside the park — typically on its own WLAN. There is data, we
 *   just cannot be a client of it.
 * - `not_published`: the park publishes wait times nowhere at all, app or
 *   otherwise. Nothing to read in the first place.
 */
export type NoLiveWaitTimesReason = "in_park_app_only" | "not_published";

export interface ParkWithoutLiveWaitTimes {
  citySlug: string;
  parkSlug: string;
  reason: NoLiveWaitTimesReason;
  /** What established this. Keep it checkable — a future reader has to be able to re-verify. */
  note: string;
}

export const PARKS_WITHOUT_LIVE_WAIT_TIMES: ParkWithoutLiveWaitTimes[] = [
  {
    citySlug: "sierksdorf",
    parkSlug: "hansa-park",
    reason: "in_park_app_only",
    note:
      "Wait times exist only in the park's own app and only for devices on the " +
      "park WLAN; there is no public endpoint. The catalog carries all 82 " +
      "attractions and a working schedule feed, and /stats has stood at " +
      "totalSampleDays 0 since ingestion began (checked 2026-08-14), which is " +
      "what a park with metadata but no reachable wait times looks like.",
  },
];

/** Lookup key — lowercased so a differently-cased slug cannot miss. */
const key = (citySlug: string, parkSlug: string): string =>
  `${citySlug.toLowerCase()}/${parkSlug.toLowerCase()}`;

const BY_KEY = new Map<string, ParkWithoutLiveWaitTimes>(
  PARKS_WITHOUT_LIVE_WAIT_TIMES.map((entry) => [
    key(entry.citySlug, entry.parkSlug),
    entry,
  ]),
);

/**
 * The reason this park's wait times are unreadable, or `null` when we can read
 * them. A park with no citySlug (geocoding never resolved a city) cannot match
 * and reads as readable — the conservative direction, since the alternative is
 * telling visitors of a healthy park that we have no data.
 */
export function getNoLiveWaitTimesReason(
  citySlug: string | null | undefined,
  parkSlug: string | null | undefined,
): NoLiveWaitTimesReason | null {
  if (!citySlug || !parkSlug) return null;
  return BY_KEY.get(key(citySlug, parkSlug))?.reason ?? null;
}
