/**
 * Why a day plan has no rides in it.
 *
 * `rides: []` used to mean four different things at once — "nobody measures
 * this park", "the feed stopped in June", "we have readings but not enough of
 * them", "something is broken" — and a caller could not tell them apart.
 * Measured against production on 2026-09-14: of 73 parks with a park-wide
 * OPERATING entry for 2026-10-14, **19 (26 %) came back empty**, and those 19
 * carry six different causes. See `docs/frontend/plan-day-endpoint.md` §11.
 *
 * Two of them mean the plan is not coming back: a park that publishes no wait
 * times anywhere readable has none to publish. The rest are gaps that close on
 * their own as history accumulates, and one — `feed_stale` — is a fault that
 * nobody would otherwise notice, because a park whose feed died still answers
 * 200 with a drawn axis.
 */
export type PlanDayUnavailableReason =
  /** The operator states the park is shut on this date. */
  | "park_closed"
  /** Neither a published nor an observed opening window for this date. */
  | "hours_unknown"
  /** The catalog holds no ride for this park at all. */
  | "no_rides_on_file"
  /**
   * Curated: this park publishes wait times nowhere a server can read them
   * (`PARKS_WITHOUT_LIVE_WAIT_TIMES`). Permanent, and the one reason a caller
   * should present as an answer rather than as a gap.
   */
  | "no_wait_time_source"
  /** Rides on file, and not one qualifying wait-time reading has ever arrived. */
  | "never_measured"
  /**
   * Readings arrived once and stopped. The park still schedules operating days,
   * so this is the case that looks healthiest from outside and is the one worth
   * an alert.
   */
  | "feed_stale"
  /** Every ride's season or works window excludes the day being planned. */
  | "rides_cannot_open"
  /**
   * Readings are arriving, but no ride has reached the measured-days floor the
   * hour shape needs. The common case, and it closes by itself.
   */
  | "insufficient_history"
  /**
   * Rides cleared that floor, and still no single hour is measured across
   * enough of them to be a column of the park's day shape — so there is no
   * curve to scale. Knott's Berry Farm: four rides report around the clock and
   * the rest only 10:00–21:00, so no hour carries the table.
   */
  | "no_hourly_shape"
  /** Hour shapes exist, and the model has produced no day level for this date. */
  | "no_forecast"
  /**
   * A past date whose 15-minute rollup holds nothing for this park. The
   * forecast reasons cannot apply — a past day is answered from what the queues
   * did, so no shape was scaled and no day level was asked for.
   */
  | "no_observations"
  /**
   * A service this endpoint depends on did not answer, so why the plan is empty
   * is not known.
   *
   * This is the reason the other nine exist to keep out of themselves. The
   * hourly profile and the daily forecast are both fetched behind a `catch`
   * that degrades to "nothing" — sound for serving, and a lie for diagnosis:
   * without this, a profile service having a bad minute reported
   * `insufficient_history` and a nightly counter filed it as a data gap that
   * would close on its own.
   */
  | "data_unavailable";

/**
 * How long a park's wait-time feed may be silent before the endpoint calls it
 * stale rather than quiet.
 *
 * Thirty days, from the measured spread: on 2026-09-14 every park that was
 * still being read had a qualifying reading within 2 days, and the two parks
 * whose feeds had died sat at 77 (Wet'n'Wild) and 96 days (La Ronde). Nothing
 * measured falls between 2 and 77, so the threshold is not a fine judgement —
 * it only has to be longer than a weekend-only park's gap and shorter than a
 * dead feed's.
 */
export const FEED_STALE_DAYS = 30;

/** Everything the classification below is allowed to look at. */
export interface PlanDayAvailabilityInput {
  /** The day's park status, as the calendar reports it. */
  status: string;
  /** Whether an opening window was found — published or observed. */
  hoursKnown: boolean;
  /** Non-retired rides in the catalog for this park. */
  rideCount: number;
  /** Rides left after the season and works-window filters. */
  plannableRideCount: number;
  /** Curated: the park publishes no readable wait times. */
  noWaitTimeSource: boolean;
  /**
   * Days since the last qualifying wait-time reading, counted from today;
   * `null` when not one has ever arrived, and `"unknown"` when the statement
   * that would answer it failed.
   *
   * Three states rather than two, because a failed query degraded to a number
   * is worse than no answer: reporting `0` would call a dead feed healthy and
   * push the verdict down into the data gaps, and reporting `null` would call a
   * live park never measured. Measured only when the plan is empty — see
   * `PlanDayService.explainEmptyPlan`.
   */
  staleDays: number | null | "unknown";
  /** Rides that cleared the hourly profile's own measured-days floor. */
  profiledRideCount: number;
  /** Rides with at least one measured hour to scale a curve from. */
  shapedRideCount: number;
  /** Whether the model produced any day level for this date. */
  hasDayLevels: boolean;
  /**
   * A past date, answered from the rollup. The two forecast questions do not
   * arise, so the ladder must not reach them.
   */
  observed: boolean;
  /**
   * Something this endpoint asks failed open — the calendar, the hourly
   * profile, either forecast, or the rollup. None of them can empty the
   * plannable set, so this is tested after the season question.
   */
  dependencyUnavailable: boolean;
  /**
   * The LIVE-STATUS lookup failed, specifically.
   *
   * Kept apart from the rest because it is the only one that can empty the
   * plannable set: it is what lifts a ride back out of the season filter, so
   * with it down every blocked ride stays blocked and "every ride's season
   * excludes this day" becomes true for a reason that has nothing to do with
   * seasons.
   */
  plannableUnavailable: boolean;
}

/**
 * The single reason to report, most specific first.
 *
 * The order is a statement about evidence, not a preference. `rides_cannot_open`
 * sits **after** the feed checks on purpose: `season_out_since` is written by a
 * detector that reads the feed, so on a park whose feed stopped in June "out of
 * season" is our own bookkeeping rather than the operator's word, and reporting
 * it there would be exactly the substitution `claude.md` §4 bans. With a live
 * feed the note means what it says and is reported.
 */
export function classifyPlanDayUnavailable(
  input: PlanDayAvailabilityInput,
): PlanDayUnavailableReason {
  // Tier 1 — the park's and the day's own facts. They outrank everything below
  // because they are answers rather than gaps, and because two of them are what
  // keeps a park we will never read out of a number that is meant to fall.
  if (input.status === "CLOSED") return "park_closed";
  if (input.rideCount === 0) return "no_rides_on_file";
  if (input.noWaitTimeSource) return "no_wait_time_source";

  // Tier 2 — the feed. A dead feed is the cause of most of what follows, and it
  // is the only fault among these reasons, so it is named before its effects.
  if (input.staleDays === "unknown") return "data_unavailable";
  if (input.staleDays === null) return "never_measured";
  if (input.staleDays >= FEED_STALE_DAYS) return "feed_stale";

  // Tier 3 — could we ask at all? Each of these sits directly above the reason
  // it would otherwise be mistaken for.
  //
  // `season_out_since` is written by a detector that reads the feed, so with
  // the feed alive the note means what it says — which is why tier 2 is above
  // this and not below it (`claude.md` §4). The live-status lookup is what
  // lifts a ride back out of that filter, so its failure has to be named before
  // the emptiness it causes, and `rides_cannot_open` is structural: an outage
  // reported there leaves the watched number for good.
  if (input.plannableUnavailable) return "data_unavailable";
  if (input.plannableRideCount === 0) return "rides_cannot_open";
  if (input.dependencyUnavailable) return "data_unavailable";
  // Both halves that could produce an opening window — the calendar's published
  // hours and the profile's observed ones — are in the check above, so reaching
  // here means both answered and neither had one.
  if (!input.hoursKnown) return "hours_unknown";

  // Tier 4 — what we have, and do not have, about the rides.
  //
  // A past day never consulted a shape or a day level, so neither may be
  // blamed for it.
  if (input.observed) return "no_observations";
  if (input.profiledRideCount === 0) return "insufficient_history";
  if (input.shapedRideCount === 0) return "no_hourly_shape";
  if (!input.hasDayLevels) return "no_forecast";
  // Shapes and day levels both present and still nothing came out: the curves
  // were built and every one of them fell outside the day's own hours. Naming
  // the shape is the honest half — the levels are there, the hours are not.
  return "no_hourly_shape";
}

/**
 * Whether this reason describes a park we will never plan for, as opposed to a
 * gap that closes as data accumulates.
 *
 * The distinction is what keeps a monitoring count honest: a park that
 * publishes no wait times is not a data gap and must not be counted as one, or
 * the number can never reach zero and stops being watched.
 */
export function isStructuralPlanDayReason(
  reason: PlanDayUnavailableReason,
): boolean {
  return (
    reason === "no_wait_time_source" ||
    reason === "no_rides_on_file" ||
    reason === "park_closed" ||
    reason === "rides_cannot_open"
  );
}

/**
 * Whether this reason says the question could not be asked, rather than
 * answering it.
 *
 * Counted apart from both halves above: a run that could not reach the profile
 * service has measured nothing, and filing it as either a healthy park or a
 * data gap would move a number that nobody checked.
 */
export function isUnknownPlanDayReason(
  reason: PlanDayUnavailableReason,
): boolean {
  return reason === "data_unavailable";
}
