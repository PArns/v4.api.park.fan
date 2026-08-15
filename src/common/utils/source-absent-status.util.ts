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
 * Europa-Park attractions and 18 Rulantica ones from its live feed — every one
 * of them an attraction with no Queue-Times mapping to fall back on. Ten weeks
 * later the park page still showed a Ball Pool, a London Bus and a Dwarf City
 * as closed, in August, at one of the busiest parks in Europe. The same
 * signature (one date, a whole cluster of rides) appears at Universal Studios
 * Singapore, both Wet'n'Wild records, Busch Gardens Tampa and Ocean Park:
 * roughly 140 attractions across ten parks.
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
