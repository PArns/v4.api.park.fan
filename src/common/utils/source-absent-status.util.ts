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
 * The test below reads `dataSource` only and ignores `is_heartbeat`, unlike
 * `observedReadingsSql()`. That is deliberate; the reasoning is on
 * `isSourceAbsent` (PAR-162).
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
 *
 * ## Why a carried heartbeat is not treated as absence (PAR-162)
 *
 * `writeHourlyHeartbeats` copies the previous row's `dataSource`, so a carried
 * row looks like a feed reading here. `observedReadingsSql()` separates the two
 * with `is_heartbeat`; this function does not, and should not:
 *
 * 1. **A caller never holds a heartbeat and a reconciliation row together.**
 *    All three callers (the ride, park and favourites payloads) read
 *    `QueueDataService.findCurrentStatusBy*`, which is `DISTINCT ON
 *    (attractionId, queueType)`: one row per queue type, the newest, inside a
 *    window of the park's opening hours or 6 h. Both writers only write
 *    `STANDBY` (the heartbeat explicitly, reconciliation through the
 *    status-only row of `saveLiveDataBatch`), so they compete for the same
 *    slot. Whatever the window, the set holds one or the other.
 * 2. **A heartbeat means a short silence, not absence.** Since PAR-720 the
 *    heartbeat skips any ride a feed delivered in the last 10 minutes, so a
 *    ride the feed reports steadily gets its hourly row from the feed and no
 *    heartbeat at all; this reason used to be that the two were
 *    indistinguishable. A heartbeat now says "no feed for at least 10 minutes",
 *    which covers a single missed poll as much as a dropped ride. Reading it as
 *    absence would put a ride on UNKNOWN for a transient gap; that is a product
 *    decision this function does not take on its own.
 *
 * What stays uncovered is the first 24 h after a feed drops a ride: its newest
 * row is a carried heartbeat, and only the Redis key can tell how long the
 * silence has lasted. Closing that window needs `last-seen` on the read path, not a
 * change here. Measured 2026-10-02 08:17 UTC: 7 of 6,403 rides had a
 * `last-seen` between 1 h and 24 h old; 5 of them showed a carried heartbeat as
 * their newest row, one of those carrying OPERATING.
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

/**
 * How many days a source may stop measuring an attraction before its last
 * CLOSED reading stops being a statement about the ride (PO, 2026-10-26,
 * PAR-1023 option C).
 *
 * The threshold is on the SILENCE, not on the age of the current reading, and
 * that distinction is the whole decision. A gate on the current row's feed age
 * reads the time of day: an evening Halloween event is twelve hours stale at
 * three in the afternoon and a water park in October is forty-five days stale,
 * so at six hours it would have taken 892 of the 1,107 CLOSED readings in
 * open parks — 80.6 % — and replaced them with a shrug. At seven days it still
 * took 243. Measured 2026-10-23 22:00 UTC; the numbers are on PAR-1023.
 *
 * "No observation in fourteen days" took 228, and the hard markers below bring
 * it to 64. The number is a wide trough rather than a ridge: seven days gives
 * 239 and the eleven rows between the two are the whole difference.
 */
export const SOURCE_SILENT_DAYS = 14;

/**
 * Whether the source has stopped measuring this attraction.
 *
 * `null` is NOT silence — it is "never scanned", and the column's docstring
 * says why that has to stay distinguishable. A scan that found nothing stores
 * its window's lower bound instead.
 */
export function isSourceSilent(
  sourceLastFreshAt: Date | string | null | undefined,
  now: Date = new Date(),
): boolean {
  if (!sourceLastFreshAt) return false;
  const last = new Date(sourceLastFreshAt).getTime();
  if (!Number.isFinite(last)) return false;
  return now.getTime() - last > SOURCE_SILENT_DAYS * 24 * 3600 * 1000;
}

/**
 * The facts that decide whether a silence may be shown as UNKNOWN. Resolved
 * values, not raw columns: a curated season has to be able to govern this the
 * same way it governs the free-flow gate.
 */
export interface SilentSourceFacts {
  /** `attractions.source_last_fresh_at`. */
  sourceLastFreshAt: Date | string | null | undefined;
  /** `attractions.retired_at`. */
  retiredAt: Date | string | null | undefined;
  /** Resolved `seasonOutSince` — `resolveCuratedFacts(attraction)`. */
  seasonOutSince: string | null | undefined;
}

/**
 * Whether a silent source should be shown to a visitor as UNKNOWN rather than
 * as the CLOSED its last frozen reading claims.
 *
 * The sibling of {@link readsUnknownFromAbsentSource}, and it answers the half
 * that one cannot: there the source went away, here it is still delivering
 * rows and has simply stopped looking. The rows carry a real `dataSource`, so
 * `isSourceAbsent` is false on every one of them.
 *
 * Three conditions, and each of them removes a class the measurement found:
 *
 * 1. **The park is OPERATING.** Same reason as the absent-source rule: in a
 *    closed park every ride is closed for a reason we can state.
 * 2. **No `retired_at`.** A demolished ride is not unknown.
 * 3. **No `seasonOutSince`.** This is the one that does the work. Of the 97
 *    frozen readings sorted by cause on 2026-10-24, 41 were seasonally or
 *    permanently out of service and CLOSED was right for them; a water park in
 *    October reports a forty-five-day-old stamp because that is when it shut.
 *    Dropping this condition is how option B got to 80 % of all CLOSED.
 *
 * Not a fourth condition: `curated_out_of_service_from`/`_to`. A hand-entered
 * out-of-service window is as hard a marker as the two above, but on
 * 2026-10-27 it overlapped the candidate set in **0** rows, so it is left out
 * rather than guessed at. If it ever overlaps, it belongs here.
 *
 * The 50 remaining readings of that sort are evening events — Halloween mazes
 * measured at three in the afternoon — and they are NOT silent: they take
 * fresh readings every night, so condition 0 above never fires for them. That
 * is the difference between a threshold on the silence and one on the row.
 */
export function readsUnknownFromSilentSource(
  rows: Array<{ status?: string | null }>,
  facts: SilentSourceFacts,
  parkStatus: string | null | undefined,
  now: Date = new Date(),
): boolean {
  if (parkStatus !== "OPERATING") return false;
  if (facts.retiredAt) return false;
  if (facts.seasonOutSince) return false;
  // A frozen OPERATING is a different published statement with a different
  // owner (PAR-759), and this rule must not reach it: it would turn a stale
  // wait time into a shrug on the way past. Any current row that still claims
  // the ride is running takes the ride out of scope here, which is also how
  // the season predicate beside it treats a live OPERATING.
  if (rows.some((row) => row.status === "OPERATING")) return false;
  return isSourceSilent(facts.sourceLastFreshAt, now);
}

/**
 * The same rule as a SQL boolean, for the counters that are computed in the
 * database rather than in the ride loop.
 *
 * Here rather than in a `.sql.ts` of its own so that the threshold has one
 * home: a second copy of the 14 is a drift waiting to happen, and the two
 * halves of this rule have to agree to the day or the page contradicts itself.
 *
 * Only the silence. The hard markers and the live-OPERATING override are
 * already spelled out in the WHERE clauses this drops into, beside
 * `retired_at IS NULL` and `attractionIsOutOfSeason`.
 *
 * @param alias - Table alias the `attractions` row carries in the caller's SQL.
 */
export function attractionSourceIsSilent(alias: string): string {
  return `(${alias}.source_last_fresh_at IS NOT NULL
           AND ${alias}.source_last_fresh_at < NOW() - INTERVAL '${SOURCE_SILENT_DAYS} days')`;
}
