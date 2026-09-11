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
 * taken for either: 84 of the 204 rides the cluster query returns carry one.
 *
 * What this guard was reaching for on 2026-09-11 is **125 genuinely silent
 * rides across eleven parks**. The operating rides among them are
 * Europa-Park's, Rulantica's and three at Mid-America Parks; the rest are
 * arcades, museums and — the dominant theme — the attractions of seasonal
 * events that left the feed when the event ended. Add, for now, the 17
 * Singapore
 * rows — equally source-absent here, because their feed moved to
 * `show_live_data`. They stop counting once PAR-159 retires them.
 *
 * One thing the test below does not look at: a carried heartbeat keeps the
 * previous row's `dataSource`, so a heartbeat row is indistinguishable from a
 * reading here — only `is_heartbeat` tells them apart, which is why
 * `observedReadingsSql()` checks it and this does not.
 *
 * The two writers are mutually exclusive at any instant: `writeHourlyHeartbeats`
 * only keeps a ride whose `attraction:last-seen` is inside 24h, and
 * reverse-reconciliation only writes when that same key is missing or older. So
 * a ride is never heartbeated and reconciled in the same cycle, and none of
 * those 125 has a heartbeat row at all (measured 2026-09-11).
 *
 * That leaves the **first 24 hours after a feed drops a ride**, when it gets
 * heartbeats and no reconciliation at all: every row in that window carries the
 * real feed's `dataSource`, so this test says "not absent" and the ride keeps
 * asserting whatever it last said. The window closes by itself, and after it
 * the rows are reconciliation and this test is right again. PAR-162 is whether
 * a day of a stale status is worth changing the definition for.
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
