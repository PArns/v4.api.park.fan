/**
 * "No source mentions this ride any more" is not "this ride is closed".
 *
 * Reverse-reconciliation (wait-times.processor) writes a CLOSED queue_data row
 * for any attraction no upstream source has reported in 24h, so that the
 * seasonal detector has something to read. That write is deliberate, but the
 * status it produces is an assertion we cannot support: it says the operator
 * closed the ride, when all that happened is that our data stopped arriving.
 *
 * The damage is measurable. On 2026-06-07 ThemeParks.wiki dropped 44
 * Europa-Park attractions and 18 Rulantica ones from its live feed. Ten weeks
 * later the park page still showed a Ball Pool, a London Bus and a Dwarf City
 * as closed, in August, at one of the busiest parks in Europe.
 *
 * The rest of what used to stand here — "the same signature appears at
 * Universal Studios Singapore, both Wet'n'Wild records, Busch Gardens Tampa
 * and Ocean Park: roughly 140 attractions across ten parks" — did not survive
 * being measured (PAR-38, 2026-09-11). Wet'n'Wild and Ocean Park were never
 * silent: the feed reports them CLOSED every few minutes, so whatever has them
 * shut — the southern winter for Wet'n'Wild, something else at year-round
 * Ocean Park — it is not our data going missing.
 * Universal Studios Singapore's 17 were recategorised to `SHOW`
 * upstream and still report, into `show_live_data`. Busch Gardens Tampa came
 * back by itself after 65 days, and all nine of its rides had a Queue-Times
 * mapping — so "no Queue-Times mapping to fall back on" is not the tell it was
 * taken for either: 50 of the 170 rides the cluster query returns carry one.
 *
 * What this guard was reaching for on 2026-09-11 is **97 genuinely silent
 * rides across seven parks**. The operating rides among them are Europa-Park's,
 * Rulantica's and three operating rides at Mid-America Parks; the rest are
 * arcades, museums and out-of-season mazes. Add, for now, the 17 Singapore
 * rows — equally source-absent here, because their feed moved to
 * `show_live_data`. They stop counting once PAR-159 retires them.
 *
 * Note what the test below does NOT do: a carried heartbeat keeps the previous
 * row's `dataSource`, so a ride reached only by heartbeats is not source-absent
 * by this definition even though nothing fresh arrived. It does not bite today
 * — none of those 97 has a heartbeat row at all, measured — but the rides it
 * would bite for are exactly the ones this guard exists for. PAR-162.
 *
 * See §5.2a of `docs/architecture/attraction-status-and-seasonality.md` for the
 * three groups and the two checks that tell them apart.
 *
 * This says the honest thing instead, and it is the rule the codebase already
 * applies one level up — a park whose wait times we cannot read puts its rides
 * on UNKNOWN rather than guessing.
 */

/** `dataSource` reverse-reconciliation stamps on the rows it writes. */
export const RECONCILIATION_SOURCE = "system-reconciliation";

/**
 * Whether every current reading for an attraction was written by
 * reverse-reconciliation rather than by a real feed.
 *
 * Requires *all* rows to be reconciliation-written, not merely the newest: a
 * ride that still reports a real STANDBY queue is being observed, whatever else
 * sits beside it. An empty list is NOT source-absence — that is "no data inside
 * the freshness window", which the callers already handle on their own terms.
 */
export function isSourceAbsent(
  rows: Array<{ dataSource?: string | null }>,
): boolean {
  return (
    rows.length > 0 &&
    rows.every((row) => row.dataSource === RECONCILIATION_SOURCE)
  );
}

/**
 * Whether that absence should be shown to a visitor as UNKNOWN.
 *
 * Only while the park itself is operating. In a closed park every ride is
 * closed for a reason we *can* state, and UNKNOWN there would replace a true
 * answer with a shrug.
 */
export function readsUnknownFromAbsentSource(
  rows: Array<{ dataSource?: string | null }>,
  parkStatus: string | null | undefined,
): boolean {
  return parkStatus === "OPERATING" && isSourceAbsent(rows);
}
