/**
 * Reading a park's opening calendar forwards, in operating minutes.
 *
 * Everything downstream of `attraction_outages` counts in **operating**
 * minutes: the recovery curve is built from `operating_minutes`, and
 * `trailingOutageWithElapsedSql()` reads the same clock backwards to say how
 * long a running outage has already lasted. Counting that way is what took
 * censoring from 68 % to 15.6 % (`docs/analytics/ride-downtime.md` §6a) — a
 * closed park is a pause and not an ending.
 *
 * The consequence is that a remaining figure off that curve is not a duration a
 * clock can add. An outage with 120 operating minutes left, in a park that
 * shuts in twenty, ends 100 minutes into tomorrow's opening — not two hours from
 * now. Turning the one into the other needs the calendar, which is what this
 * file walks.
 *
 * These are the forward twin of the backwards sum inside
 * `trailingOutageWithElapsedSql()`, and they read the same windows: whatever
 * `parkOpenWindowCtes()` flattened. Pure on purpose, so the four cases that
 * matter — inside today, across a closing, no calendar at all, an unresolved
 * upper quartile — can be pinned without a database.
 */

/**
 * One flattened `OPERATING` window, half-open `[opensAt, closesAt)`.
 *
 * The shape `parkOpenWindowCtes()`'s `win` CTE produces: overlapping schedule
 * rows already merged, the closing time already repaired.
 */
export interface OperatingWindow {
  opensAt: Date;
  closesAt: Date;
}

/**
 * The recovery window as instants, ready to be rendered as clock times.
 *
 * `to` is `null` when there is no upper bound to give — either the curve did not
 * resolve the upper quartile (past roughly two hours it stops), or the calendar
 * on hand does not reach that far. **On the wire that null is an absent key**:
 * `ExcludeNullInterceptor` deletes every null-valued key from every response
 * outside `/v1/admin/*` and `?debug=true`, which is also why `remaining.p75`
 * arrives as a missing key rather than as `null`. A client reads both the same
 * way: no upper bound.
 */
export interface RecoveryWindow {
  /** ISO 8601 UTC. The 25th percentile, placed on the calendar. */
  from: string;
  /** ISO 8601 UTC, or null when there is no upper bound to give. */
  to: string | null;
}

/**
 * The instant at which `minutes` further operating minutes will have passed.
 *
 * Walks the windows from `from`, spending the requested minutes inside them and
 * skipping every closed hour. `from` outside a window is not an error and not a
 * zero: the clock starts ticking at the next opening, which is the whole reason
 * this function exists.
 *
 * @param windows - The park's windows, **ascending and disjoint**, as
 *   `parkOpenWindowCtes()` produces them. Unsorted or overlapping input is not
 *   repaired here — the merge is the SQL's job, and doing it twice would hide a
 *   caller that skipped it.
 * @param from - The instant to start counting at.
 * @param minutes - Operating minutes to spend. `0` answers with the next
 *   instant the park is open, which is the honest reading of "no time left".
 * @returns The instant, or `null` when the windows run out first — a park shut
 *   for the season, or a horizon too short. Never a wall-clock fallback: adding
 *   the minutes to `from` would answer a question nobody asked.
 */
export function projectOperatingMinutes(
  windows: readonly OperatingWindow[],
  from: Date,
  minutes: number,
): Date | null {
  if (!Number.isFinite(minutes) || minutes < 0) return null;
  const fromMs = from.getTime();
  if (!Number.isFinite(fromMs)) return null;

  let remainingMs = minutes * 60_000;
  for (const window of windows) {
    const opens = window.opensAt.getTime();
    const closes = window.closesAt.getTime();
    if (!Number.isFinite(opens) || !Number.isFinite(closes)) continue;
    if (closes <= fromMs) continue;
    const start = Math.max(opens, fromMs);
    // A window with no minutes in it. `parkOpenWindowCtes` drops these in
    // `w_ord` (`WHERE closes_at > opens_at`), so nothing from that query
    // reaches here — this is for a caller that builds a list by hand, and it
    // keeps a zero-length row from consuming the loop's only `continue`-free
    // path with a negative `available`.
    if (closes <= start) continue;

    const available = closes - start;
    if (available >= remainingMs) return new Date(start + remainingMs);
    remainingMs -= available;
  }
  return null;
}

/**
 * The measured remaining quartiles, placed on the park's calendar.
 *
 * The pair stays a pair. A single instant would read as a promise, and the
 * distribution behind it is heavy-tailed enough that the median alone is wrong
 * in the one direction that costs a visitor their afternoon.
 *
 * @param windows - The park's upcoming windows, ascending and disjoint.
 * @param asOf - Now, or whatever instant the caller is answering for.
 * @param remaining - `p25` and `p75` off the curve, in operating minutes. `p75`
 *   is `null` past roughly two hours, where the curve stops resolving it.
 * @returns The window, or `undefined` when even the lower bound cannot be
 *   placed — no calendar at all, or none reaching far enough. An unplaceable
 *   lower bound withholds the whole object rather than shipping half of it.
 */
export function recoveryWindowFrom(
  windows: readonly OperatingWindow[],
  asOf: Date,
  remaining: { p25: number; p75: number | null },
): RecoveryWindow | undefined {
  if (windows.length === 0) return undefined;

  const from = projectOperatingMinutes(windows, asOf, remaining.p25);
  if (!from) return undefined;

  const to =
    remaining.p75 === null
      ? null
      : projectOperatingMinutes(windows, asOf, remaining.p75);

  return { from: from.toISOString(), to: to ? to.toISOString() : null };
}
