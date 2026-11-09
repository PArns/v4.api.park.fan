import { getCurrentDateInTimezone } from "../../common/utils/date.util";

/**
 * The hand-written "this ride is in a works period from … to …" window.
 *
 * The feed cannot tell a breakdown from a rebuild. ThemeParks.wiki passes
 * `REFURBISHMENT` through with no start, no end and no announcement, and it does
 * not use it for every closure that is really planned work — a long rebuild seen
 * from outside looks exactly like a ride that keeps failing. Without this the
 * only guard is the reconstruction's seven-day bound, which lets a six-day
 * rebuild through as an outage and says nothing at all about the live line.
 *
 * So an editor may state the window under `/admin/attractions/<id>`, and inside
 * it nothing is reported: no live "Störung gemeldet seit", and no reconstructed
 * outage once that exists. It is a statement about the ride, not a correction of
 * a synced column, so it has no upstream half and no two-writers problem.
 *
 * ## Which clock
 *
 * The park's. An editor writing "16 January to 3 March" means the park's
 * calendar, and a UTC comparison would open and close the window at the wrong
 * moment for every park west of Greenwich. Both bounds are **inclusive**: a
 * window that ends on the 3rd covers the 3rd.
 *
 * ## Half-open windows
 *
 * Either bound may stand alone. `from` with no `to` is the usual case while work
 * is running and nobody has been told when it ends. `to` with no `from` is a
 * window that was already open when somebody wrote it down. Both empty is the
 * normal state and means nothing is claimed.
 */
export interface CuratedOutOfServiceSource {
  curatedOutOfServiceFrom?: string | null;
  curatedOutOfServiceTo?: string | null;
  curatedOutOfServiceToUncertain?: boolean | null;
  /**
   * The list of windows (PAR-1246). Read beside the three single-window columns
   * for one release — expand/contract — and never on its own: every reader goes
   * through {@link resolveWorksPeriods}.
   */
  curatedOutOfServicePeriods?: readonly WorksPeriod[] | null;
}

/**
 * The same window as it leaves the API, plus how firm its end is.
 *
 * Served as `worksPeriod`, next to and never inside `outage`: an outage is a
 * fault somebody is reporting right now, this is planned work somebody wrote
 * down in advance, and a client that folds them together tells a visitor the
 * ride broke when it is being rebuilt. The two live side by side on purpose.
 *
 * The keys drop the `curated` prefix the columns carry. That prefix is a
 * statement about where the value comes from, which is the storage layer's
 * question — no field this API serves has ever carried it, and a reader of the
 * ride page has no use for the distinction.
 */
export interface WorksPeriod {
  /** First park-local day, inclusive, or null for a window with no start. */
  from: string | null;
  /** Last park-local day, inclusive, or null while nobody knows when it ends. */
  to: string | null;
  /** Whether `to` is our estimate rather than a date the park published. */
  toUncertain: boolean;
  /**
   * Which automated writer owns this window, e.g.
   * `"efteling-maintenance-calendar"` (PAR-1245); absent on a hand-written
   * one. Stored and shown to the admin, never served: public payloads go
   * through {@link publicWorksPeriod}.
   */
  source?: string;
}

export type WorksPeriodSource = CuratedOutOfServiceSource;

/**
 * One stored window as the API serves it.
 *
 * Two normalisations, both so a client cannot render a half-statement:
 *
 * - `toUncertain` is false whenever `to` is null. An adverb needs a date to
 *   qualify, and the pair (`to: null`, `toUncertain: true`) is a curation
 *   leftover from clearing the end date and not clearing the flag.
 * - `null` on the flag reads as false here. Three states are what the EDITOR
 *   needs — "nobody has looked" is worth seeing in the form — but a page has
 *   two renderings, "until 3 March" and "probably until 3 March", and an
 *   unchecked date gets the plain one.
 *
 * Also the entity's read transformer for the jsonb list, so the keys always
 * come back in this order: jsonb stores them sorted by length (`to`, `from`,
 * `toUncertain`), and the admin's diff and undo compare `JSON.stringify` output.
 */
export function normalizeWorksPeriod(period: {
  from?: string | null;
  to?: string | null;
  toUncertain?: boolean | null;
  source?: string | null;
}): WorksPeriod {
  const to = period.to ?? null;
  return {
    from: period.from ?? null,
    to,
    toUncertain: to !== null && period.toUncertain === true,
    ...(period.source ? { source: period.source } : {}),
  };
}

/** A window as a payload serves it: the owner marker is the admin's business. */
export function publicWorksPeriod(window: WorksPeriod): WorksPeriod {
  return { from: window.from, to: window.to, toUncertain: window.toUncertain };
}

/**
 * `next` with each source-less window given the `source` of the `current`
 * window with the same `from` and `to`.
 *
 * The admin form sends windows without `source`, and a save that left an
 * automated window's dates alone must not turn it into a hand-written one —
 * the next sync would then refuse to touch it and report it as a clash. A
 * window whose dates the editor changed becomes theirs.
 */
export function keepWindowSources(
  next: readonly WorksPeriod[],
  current: readonly WorksPeriod[],
): WorksPeriod[] {
  return next.map((window) => {
    if (window.source) return window;
    const owner = current.find(
      (c) => c.source && c.from === window.from && c.to === window.to,
    );
    return owner
      ? normalizeWorksPeriod({ ...window, source: owner.source })
      : window;
  });
}

/**
 * Every curated window, sorted by `from` (an open start first), then by `to`
 * (an open end last), with exact duplicates removed.
 *
 * The single-window columns are read beside the list for this release: a write
 * of the list clears them, so until every ride has been saved once the old
 * window is still the only place some rides keep theirs. A list entry wins a
 * duplicate over the old window because it is the newer statement.
 *
 * Windows with neither bound are dropped — they claim nothing.
 */
export function resolveWorksPeriods(source: WorksPeriodSource): WorksPeriod[] {
  const legacy =
    source.curatedOutOfServiceFrom || source.curatedOutOfServiceTo
      ? [
          {
            from: source.curatedOutOfServiceFrom,
            to: source.curatedOutOfServiceTo,
            toUncertain: source.curatedOutOfServiceToUncertain,
          },
        ]
      : [];
  const seen = new Set<string>();
  return [...(source.curatedOutOfServicePeriods ?? []), ...legacy]
    .map(normalizeWorksPeriod)
    .filter((window) => {
      const key = `${window.from}|${window.to}`;
      if ((!window.from && !window.to) || seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort(compareWorksPeriods);
}

/**
 * `{ worksPeriods }` for a payload, or nothing: an empty list on every ride of
 * every park page would be payload for a fact that is almost always absent.
 */
export function worksPeriodsOf(source: WorksPeriodSource): {
  worksPeriods?: WorksPeriod[];
} {
  const worksPeriods = resolveWorksPeriods(source).map(publicWorksPeriod);
  return worksPeriods.length > 0 ? { worksPeriods } : {};
}

function compareWorksPeriods(a: WorksPeriod, b: WorksPeriod): number {
  if (a.from !== b.from) {
    if (a.from === null) return -1;
    if (b.from === null) return 1;
    return a.from < b.from ? -1 : 1;
  }
  if (a.to === b.to) return 0;
  if (a.to === null) return 1;
  if (b.to === null) return -1;
  return a.to < b.to ? -1 : 1;
}

/** Whether one window covers a park-local day. Both bounds inclusive. */
function covers(window: WorksPeriod, day: string): boolean {
  // Plain YYYY-MM-DD strings, so a lexicographic compare is a calendar
  // compare. No Date objects: constructing one would drag the server's offset
  // back into a park-local question.
  return (
    (!window.from || day >= window.from) && (!window.to || day <= window.to)
  );
}

/**
 * The one window `worksPeriod` serves, or null when nothing is curated.
 *
 * The window running on the park's today; else the next one to start; else the
 * one that ended most recently. That is the order a ride page asks in — "closed
 * for works now", "closed from 16 January", "was closed until March" — and a
 * ride with a single window gets that window whatever the day, exactly as
 * before the list existed. Every window is on `worksPeriods`.
 *
 * Null rather than a block of nulls: "no works period" is the state of nearly
 * every ride in the catalogue, and an object saying so on each of them is
 * payload on every park page for a fact that is almost always absent.
 *
 * @param timezone - The park's IANA timezone. Without one (a projection that
 *   did not join the park) the first window is served.
 * @param onDate - Park-local `YYYY-MM-DD`. Defaults to the park's today.
 */
export function resolveWorksPeriod(
  source: WorksPeriodSource,
  timezone?: string | null,
  onDate?: string,
): WorksPeriod | null {
  const windows = resolveWorksPeriods(source);
  if (windows.length === 0) return null;

  const day = onDate ?? (timezone ? safeToday(timezone) : null);
  // shortcut: no park timezone means no "today"; the first window is the
  // honest single answer for the one or two rides that carry several.
  if (!day) return publicWorksPeriod(windows[0]);

  return publicWorksPeriod(
    windows.find((window) => covers(window, day)) ??
      windows.find((window) => window.from !== null && window.from > day) ??
      windows.reduce((latest, window) =>
        (window.to ?? "") > (latest.to ?? "") ? window : latest,
      ),
  );
}

/**
 * Whether any curated works period covers the given park-local day.
 *
 * @param source - The attraction's curated works-period columns.
 * @param timezone - The park's IANA timezone.
 * @param onDate - Park-local `YYYY-MM-DD` to test. Defaults to the park's today.
 */
export function isCuratedOutOfService(
  source: CuratedOutOfServiceSource,
  timezone: string,
  onDate?: string,
): boolean {
  const windows = resolveWorksPeriods(source);
  if (windows.length === 0) return false;

  const day = onDate ?? safeToday(timezone);
  if (!day) return false;

  return windows.some((window) => covers(window, day));
}

/**
 * The window that covers a park-local day, as a payload serves it, or null.
 *
 * {@link isCuratedOutOfService} answers whether; this answers which, so a
 * caller can say until when. The assistant names it for a ride on the day it
 * is asked about, and refuses to plan the ride into that day.
 *
 * @param day - Park-local `YYYY-MM-DD`.
 */
export function worksPeriodOn(
  source: WorksPeriodSource,
  day: string,
): WorksPeriod | null {
  return worksPeriodCovering(
    resolveWorksPeriods(source).map(publicWorksPeriod),
    day,
  );
}

/**
 * {@link worksPeriodOn} for windows already served (`worksPeriods` on a park
 * or attraction payload), where the curated columns are gone.
 */
export function worksPeriodCovering(
  windows: readonly WorksPeriod[],
  day: string,
): WorksPeriod | null {
  return windows.find((window) => covers(window, day)) ?? null;
}

/** One ride's curated windows, as a park-wide read hands them out. */
export interface RideWorksPeriods {
  id: string;
  name: string;
  windows: WorksPeriod[];
}

/**
 * Every ride among `attractions` that has a curated window, with its windows
 * as a payload serves them, by name. The one place a park's rows become that
 * list, whichever query loaded them.
 */
export function rideWorksPeriods(
  attractions: readonly (WorksPeriodSource & { id: string; name: string })[],
): RideWorksPeriods[] {
  const out: RideWorksPeriods[] = [];
  for (const attraction of attractions) {
    const windows = resolveWorksPeriods(attraction).map(publicWorksPeriod);
    if (windows.length > 0) {
      out.push({ id: attraction.id, name: attraction.name, windows });
    }
  }
  return out.sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * `predictions` without the rows of a ride that a curated window shuts on the
 * row's day.
 *
 * The model forecasts every ride for every day and has never heard of a
 * window, so a ride in maintenance arrives with an ordinary wait. Left in, it
 * counts towards a day's average and crowd level and can head the day's
 * headliner list while the same day lists it as closed.
 *
 * @param dayOf - The row's park-local `YYYY-MM-DD`, read the way the caller
 *   already reads it, so the filter and the grouping agree on the day.
 */
export function withoutRidesInMaintenance<T extends { attractionId: string }>(
  predictions: T[],
  rides: readonly Pick<RideWorksPeriods, "id" | "windows">[],
  dayOf: (prediction: T) => string,
): T[] {
  if (rides.length === 0) return predictions;
  const windowsByRide = new Map(rides.map((ride) => [ride.id, ride.windows]));
  return predictions.filter((prediction) => {
    const windows = windowsByRide.get(prediction.attractionId);
    return !windows || !worksPeriodCovering(windows, dayOf(prediction));
  });
}

/**
 * The next window to start after `day` among windows already served, or null.
 * Only a window with a start can lie ahead.
 */
export function nextWorksPeriod(
  windows: readonly WorksPeriod[],
  day: string,
): WorksPeriod | null {
  return (
    windows.find((window) => window.from !== null && window.from > day) ?? null
  );
}

/**
 * SQL twin of {@link isCuratedOutOfService}, for the reconstruction and for any
 * catalogue-wide query that never loads a row.
 *
 * Hand-maintained beside its TypeScript half in the pattern
 * `attractionIsOutOfSeason()` set, and changed with it or not at all.
 *
 * Unlike that predicate this one takes the day as a parameter rather than
 * reading `NOW()`. A works period is a fact about a date range, so asking it
 * about a past day is meaningful and has to give the answer that day had.
 * `attractionIsOutOfSeason()` computes against the park's month of `NOW()`,
 * which is why it may not be used on history at all: applied to 90 days it
 * deletes the July of a ride that leaves season in September.
 *
 * @param alias - Table alias the `attractions` row carries in the caller's SQL.
 * @param dayExpr - SQL expression yielding the park-local `date` to test.
 */
export function attractionIsCuratedOutOfService(
  alias: string,
  dayExpr: string,
): string {
  // The list is jsonb, so its bounds are text cast to `date` — the cast sits
  // on the extracted value, never on a column an index could serve.
  return `(
           (
             (${alias}.curated_out_of_service_from IS NOT NULL
              OR ${alias}.curated_out_of_service_to IS NOT NULL)
             AND (${alias}.curated_out_of_service_from IS NULL
                  OR ${dayExpr} >= ${alias}.curated_out_of_service_from)
             AND (${alias}.curated_out_of_service_to IS NULL
                  OR ${dayExpr} <= ${alias}.curated_out_of_service_to)
           )
           OR EXISTS (
             SELECT 1
               FROM jsonb_array_elements(
                      COALESCE(${alias}.curated_out_of_service_periods, '[]'::jsonb)
                    ) AS works_window(w)
              WHERE (w->>'from' IS NOT NULL OR w->>'to' IS NOT NULL)
                AND (w->>'from' IS NULL OR ${dayExpr} >= (w->>'from')::date)
                AND (w->>'to' IS NULL OR ${dayExpr} <= (w->>'to')::date)
           )
         )`;
}

/**
 * A bad timezone must not cost the caller its whole response.
 *
 * This runs inside the park page's ride loop, so an unguarded throw would take
 * every ride's status with it for the sake of one malformed column.
 */
function safeToday(timezone: string): string | null {
  try {
    return getCurrentDateInTimezone(timezone);
  } catch {
    return null;
  }
}

/**
 * Whether a works period ends before it begins — a window that never applies.
 *
 * Lives here rather than inside the endpoint that rejects it, because two
 * writers now have to agree on it: `AdminCurationService` refuses the pair a
 * curator types, and `AttractionMergeService` refuses to carry one onto a row
 * that outlives a merge. The `??`-rules in `resolveCuratedFacts` were copied
 * into two DTO mappers once, drifted, and shipped a bug; this is the same shape
 * of rule and gets one home.
 *
 * Both bounds inclusive, both park-local ISO dates, so a string comparison is
 * the date comparison. A half-open window is never inverted: there is nothing
 * for the missing bound to precede.
 */
export function worksPeriodEndsBeforeItBegins(
  from: string | null | undefined,
  to: string | null | undefined,
): boolean {
  return Boolean(from) && Boolean(to) && (to as string) < (from as string);
}

/**
 * Most windows one ride may hold. A ride closes for planned work a few times a
 * year at most; twenty is somebody pasting a calendar, not a curation.
 */
export const MAX_WORKS_PERIODS = 20;

/**
 * Whether two windows share a day. Both bounds inclusive; a missing bound is
 * open in its direction.
 */
export function worksPeriodsOverlap(a: WorksPeriod, b: WorksPeriod): boolean {
  return (
    (!a.from || !b.to || a.from <= b.to) && (!b.from || !a.to || b.from <= a.to)
  );
}
