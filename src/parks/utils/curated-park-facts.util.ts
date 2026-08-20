import type { NoLiveWaitTimesReason } from "../data/live-wait-time-sources";
import { getNoLiveWaitTimesReason } from "../data/live-wait-time-sources";

/**
 * Merges the hand-curated park facts over the synced ones.
 *
 * The attraction-side counterpart is
 * `attractions/utils/curated-attraction-facts.util.ts`, and this follows the
 * same rule for the same reason: the metadata sync rewrites its columns on
 * every run, so a correction sits in a column of its own and the read side
 * merges them. One function, so the merge cannot be re-derived slightly
 * differently at the next call site.
 *
 * Notably absent: timezone. See the comment in the entity — it is read at
 * hundreds of call sites and a DTO-level override would leave the calendar
 * computing in the uncorrected zone.
 */

export interface CuratedParkSource {
  name?: string | null;
  curatedName?: string | null;
  parkType?: string | null;
  curatedParkType?: string | null;
  citySlug?: string | null;
  slug?: string | null;
  curatedNoWaitTimesReason?: string | null;
}

export interface ResolvedCuratedPark {
  name: string;
  parkType: string;
  /** Null when this park's wait times can be read. */
  noWaitTimesReason: NoLiveWaitTimesReason | null;
}

const VALID_NO_WAIT_TIMES_REASONS: readonly string[] = [
  "in_park_app_only",
  "not_published",
];

function cleaned(value: string | null | undefined): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Why this park's wait times are unreadable, curated column first, code list
 * second.
 *
 * Both are hand-written facts; the difference is only where they were written.
 * `PARKS_WITHOUT_LIVE_WAIT_TIMES` came first and stays as the seed and the
 * fallback, so nothing has to be migrated for the column to start winning. An
 * unrecognised string in the column is ignored rather than served: these values
 * are API contract, the frontend translates them, and an unknown one would
 * render as a missing translation on a live park page.
 */
export function resolveNoWaitTimesReason(
  park: CuratedParkSource,
): NoLiveWaitTimesReason | null {
  const curated = cleaned(park.curatedNoWaitTimesReason);
  if (curated && VALID_NO_WAIT_TIMES_REASONS.includes(curated)) {
    return curated as NoLiveWaitTimesReason;
  }
  return getNoLiveWaitTimesReason(park.citySlug, park.slug);
}

export function resolveCuratedPark(
  park: CuratedParkSource,
): ResolvedCuratedPark {
  return {
    name: cleaned(park.curatedName) ?? park.name ?? "",
    parkType: cleaned(park.curatedParkType) ?? park.parkType ?? "THEME_PARK",
    noWaitTimesReason: resolveNoWaitTimesReason(park),
  };
}
