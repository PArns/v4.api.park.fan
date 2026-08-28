import type { NoLiveWaitTimesReason } from "../data/live-wait-time-sources";
import { getNoLiveWaitTimesReason } from "../data/live-wait-time-sources";
// The currency belongs to the fast-pass resolver: it is the column that makes
// a per-ride price servable, and validating it in two places would let the
// park page publish a code the ride page rejects.
import { resolveCurrency } from "../../attractions/utils/fast-pass.util";

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
  curatedWebsite?: string | null;
  curatedTicketsUrl?: string | null;
  curatedWikipediaUrl?: string | null;
  curatedInstagramUrl?: string | null;
  curatedFacebookUrl?: string | null;
  curatedYoutubeUrl?: string | null;
  curatedStreetAddress?: string | null;
  curatedPostalCode?: string | null;
  curatedPhone?: string | null;
  curatedOpenedYear?: number | null;
  curatedAreaHectares?: number | null;
  curatedFastPassName?: string | null;
  curatedCurrency?: string | null;
  curatedFastPassTermId?: string | null;
  curatedFastPassPriceFrom?: number | null;
}

/**
 * The hand-written facts about a park, as the API serves them.
 *
 * One object rather than eleven top-level fields, because they belong together
 * on the page and because a client can then ask one question — "is there
 * anything to show" — instead of eleven.
 */
export interface ParkInfo {
  website: string | null;
  ticketsUrl: string | null;
  wikipediaUrl: string | null;
  instagramUrl: string | null;
  facebookUrl: string | null;
  youtubeUrl: string | null;
  streetAddress: string | null;
  postalCode: string | null;
  phone: string | null;
  openedYear: number | null;
  areaHectares: number | null;
  /**
   * What this park calls its paid queue-jump product, e.g. "QuickPass".
   *
   * Here as well as on every flagged ride, because the park page wants to name
   * the product once without walking the attraction list to find it.
   */
  fastPassName: string | null;
  /** ISO-4217 the park's curated prices are quoted in. */
  currency: string | null;
  /** Glossary term id explaining the product — the frontend resolves it. */
  fastPassTermId: string | null;
  /** What the cheapest version of the product costs, in `currency`. */
  fastPassPriceFrom: number | null;
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

/**
 * Every hand-written column on a park row, in one list.
 *
 * It exists because a merge deletes the losing row. The attraction side learned
 * this already and says so at `attraction-merge.service.ts`: a curation that
 * lived only on the loser is gone with no trace. The park side had no such
 * list, and by the time the info block landed it had more curated columns than
 * the attraction does.
 *
 * Keep it complete. `curated-field.spec-list.spec.ts` asserts it matches the
 * editor's descriptors, so a new column fails the suite until both know it.
 */
export const CURATED_PARK_COLUMNS = [
  "curatedName",
  "curatedParkType",
  "curatedNoWaitTimesReason",
  "curatedWebsite",
  "curatedTicketsUrl",
  "curatedWikipediaUrl",
  "curatedInstagramUrl",
  "curatedFacebookUrl",
  "curatedYoutubeUrl",
  "curatedStreetAddress",
  "curatedPostalCode",
  "curatedPhone",
  "curatedOpenedYear",
  "curatedAreaHectares",
  "curatedFastPassName",
  "curatedCurrency",
  "curatedFastPassTermId",
  "curatedFastPassPriceFrom",
  "curationNote",
] as const;

export type CuratedParkColumn = (typeof CURATED_PARK_COLUMNS)[number];

/**
 * The same list as physical column names, for the places that write SQL.
 *
 * Derived rather than typed out twice: every curated column on the entity is
 * named the snake_case of its property, and the spec asserts the derivation
 * against the real names so an irregular one fails the suite instead of
 * silently dropping out of a query.
 */
export const CURATED_PARK_DB_COLUMNS: readonly string[] =
  CURATED_PARK_COLUMNS.map((column) =>
    column.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`),
  );

export function resolveCuratedPark(
  park: CuratedParkSource,
): ResolvedCuratedPark {
  return {
    name: cleaned(park.curatedName) ?? park.name ?? "",
    parkType: cleaned(park.curatedParkType) ?? park.parkType ?? "THEME_PARK",
    noWaitTimesReason: resolveNoWaitTimesReason(park),
  };
}

function positive(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? value
    : null;
}

/**
 * The curated info block, or null when nobody has written anything.
 *
 * Null rather than an object of nulls: the global interceptor strips null keys
 * from public responses, so an untouched park would otherwise carry an empty
 * `info: {}` — a thing the frontend has to test for separately from "absent",
 * for no gain.
 */
export function resolveParkInfo(park: CuratedParkSource): ParkInfo | null {
  const info: ParkInfo = {
    website: cleaned(park.curatedWebsite),
    ticketsUrl: cleaned(park.curatedTicketsUrl),
    wikipediaUrl: cleaned(park.curatedWikipediaUrl),
    instagramUrl: cleaned(park.curatedInstagramUrl),
    facebookUrl: cleaned(park.curatedFacebookUrl),
    youtubeUrl: cleaned(park.curatedYoutubeUrl),
    streetAddress: cleaned(park.curatedStreetAddress),
    postalCode: cleaned(park.curatedPostalCode),
    phone: cleaned(park.curatedPhone),
    openedYear: positive(park.curatedOpenedYear),
    areaHectares: positive(park.curatedAreaHectares),
    fastPassName: cleaned(park.curatedFastPassName),
    currency: resolveCurrency(park),
    fastPassTermId: cleaned(park.curatedFastPassTermId),
    fastPassPriceFrom: positive(park.curatedFastPassPriceFrom),
  };

  return Object.values(info).some((value) => value !== null) ? info : null;
}
