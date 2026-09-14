import {
  classifyPlanDayUnavailable,
  FEED_STALE_DAYS,
  isStructuralPlanDayReason,
  isUnknownPlanDayReason,
  type PlanDayAvailabilityInput,
} from "./plan-day-availability.util";

/**
 * Every case here is written as a PAIR: the state that produces the reason, and
 * the one input flipped back so it produces something else.
 *
 * Without the second half a case proves nothing. `classifyPlanDayUnavailable`
 * is a ladder of guards, so a test that only asserts the expected string passes
 * just as well when an EARLIER guard swallowed the case and the branch under
 * test was never reached — which is exactly the failure 📚 G-44 describes. The
 * counter-check is what shows the ladder got that far.
 */
describe("classifyPlanDayUnavailable", () => {
  /** A park that can be planned for: every gate open, nothing missing. */
  const healthy: PlanDayAvailabilityInput = {
    status: "OPERATING",
    hoursKnown: true,
    rideCount: 40,
    plannableRideCount: 40,
    noWaitTimeSource: false,
    staleDays: 0,
    profiledRideCount: 18,
    shapedRideCount: 18,
    hasDayLevels: true,
    observed: false,
    dependencyUnavailable: false,
  };

  const of = (patch: Partial<PlanDayAvailabilityInput>) =>
    classifyPlanDayUnavailable({ ...healthy, ...patch });

  it("reports a stated closure before anything else", () => {
    expect(of({ status: "CLOSED", rideCount: 0 })).toBe("park_closed");
    // Same input, park open: the ladder walks past the first guard and lands on
    // the ride count, so the closure branch really was the one that answered.
    expect(of({ status: "OPERATING", rideCount: 0 })).toBe("no_rides_on_file");
  });

  it("reports an unknown window before looking at the rides", () => {
    expect(of({ hoursKnown: false, rideCount: 0 })).toBe("hours_unknown");
    expect(of({ hoursKnown: true, rideCount: 0 })).toBe("no_rides_on_file");
  });

  it("names an empty catalog rather than the missing history it causes", () => {
    expect(of({ rideCount: 0, profiledRideCount: 0 })).toBe("no_rides_on_file");
    expect(of({ rideCount: 40, profiledRideCount: 0 })).toBe(
      "insufficient_history",
    );
  });

  it("prefers the curated source flag over 'never measured'", () => {
    // Hansa-Park: 82 rides, never a qualifying reading, and the reason is known
    // — its wait times live in an app on the park WLAN. Measured 2026-09-14.
    expect(of({ noWaitTimeSource: true, staleDays: null })).toBe(
      "no_wait_time_source",
    );
    // Paradise Country: the same silence with nothing written down about it.
    expect(of({ noWaitTimeSource: false, staleDays: null })).toBe(
      "never_measured",
    );
  });

  it("separates a feed that stopped from one that was never there", () => {
    // La Ronde on 2026-09-14: last qualifying reading 2026-06-10.
    expect(of({ staleDays: 96 })).toBe("feed_stale");
    expect(of({ staleDays: null })).toBe("never_measured");
  });

  it("holds the stale threshold at the measured gap, not below it", () => {
    expect(of({ staleDays: FEED_STALE_DAYS })).toBe("feed_stale");
    // One day under, and the ladder falls through to the data gaps — so the
    // threshold is what decided it, not some later guard.
    expect(of({ staleDays: FEED_STALE_DAYS - 1, profiledRideCount: 0 })).toBe(
      "insufficient_history",
    );
  });

  it("does not call a park out of season on the word of a dead feed", () => {
    // `season_out_since` is written by a detector that reads the feed. With the
    // feed silent for months the note is our own bookkeeping, so the silence is
    // reported instead — `claude.md` §4.
    expect(of({ staleDays: 96, plannableRideCount: 0 })).toBe("feed_stale");
    // Live feed, same empty plannable set: now the note means what it says.
    expect(of({ staleDays: 1, plannableRideCount: 0 })).toBe(
      "rides_cannot_open",
    );
  });

  it("separates 'no ride cleared the floor' from 'no hour did'", () => {
    // The twelve parks of 2026-09-14 whose readings arrive but stay thin.
    expect(of({ profiledRideCount: 0, shapedRideCount: 0 })).toBe(
      "insufficient_history",
    );
    // Knott's Berry Farm: 17 rides over the floor, and still no hour is carried
    // by enough of them to be a column, so every p50 row comes back empty.
    expect(of({ profiledRideCount: 17, shapedRideCount: 0 })).toBe(
      "no_hourly_shape",
    );
  });

  it("names the missing forecast only once there is a shape to scale", () => {
    expect(of({ hasDayLevels: false })).toBe("no_forecast");
    expect(of({ hasDayLevels: false, shapedRideCount: 0 })).toBe(
      "no_hourly_shape",
    );
  });

  it("does not read a failed recency query as an answer about the feed", () => {
    expect(of({ staleDays: "unknown" })).toBe("data_unavailable");
    // Both answers it could have degraded to are wrong in opposite directions,
    // and both would be reported as facts about the park.
    expect(of({ staleDays: null })).toBe("never_measured");
    expect(of({ staleDays: 0, profiledRideCount: 0 })).toBe(
      "insufficient_history",
    );
    // And a fact about the park still outranks it.
    expect(of({ staleDays: "unknown", noWaitTimeSource: true })).toBe(
      "no_wait_time_source",
    );
  });

  it("says the question could not be asked before answering it", () => {
    // `loadProfile` and `dayLevels` both degrade to "nothing" on failure, which
    // is right for serving and a lie for diagnosis. Without this guard a
    // profile service having a bad minute reported `insufficient_history`.
    expect(of({ dependencyUnavailable: true, profiledRideCount: 0 })).toBe(
      "data_unavailable",
    );
    // Same empty profile with the dependency healthy: the data gap is named.
    expect(of({ dependencyUnavailable: false, profiledRideCount: 0 })).toBe(
      "insufficient_history",
    );
    // And it does not shout over a fact about the park itself.
    expect(of({ dependencyUnavailable: true, rideCount: 0 })).toBe(
      "no_rides_on_file",
    );
  });

  it("blames no forecast for a past day, which never asked for one", () => {
    // A past date is answered from the 15-minute rollup: nothing is composed
    // and no day level is read, so neither may be reported as missing.
    expect(
      of({ observed: true, profiledRideCount: 0, hasDayLevels: false }),
    ).toBe("no_observations");
    // The same counts on a future date are a forecast gap, which shows the
    // `observed` flag is what decided it.
    expect(
      of({ observed: false, profiledRideCount: 0, hasDayLevels: false }),
    ).toBe("insufficient_history");
  });

  it("still names a dead feed on a past day rather than the empty rollup", () => {
    // The feed checks sit above `observed` on purpose: a feed that died in June
    // is WHY the rollup is empty in July, and it is the more useful sentence.
    expect(of({ observed: true, staleDays: 96 })).toBe("feed_stale");
    expect(of({ observed: true, staleDays: 1 })).toBe("no_observations");
  });

  it("falls back to the shape when shapes and levels are both present", () => {
    // Curves were built and every one of them fell outside the day's hours.
    // Reporting the forecast as missing would be the one wrong answer here.
    expect(of({})).toBe("no_hourly_shape");
  });
});

describe("isStructuralPlanDayReason", () => {
  it("counts a park we cannot read as an answer, not as a data gap", () => {
    expect(isStructuralPlanDayReason("no_wait_time_source")).toBe(true);
    expect(isStructuralPlanDayReason("no_rides_on_file")).toBe(true);
    expect(isStructuralPlanDayReason("park_closed")).toBe(true);
    expect(isStructuralPlanDayReason("rides_cannot_open")).toBe(true);
  });

  it("counts a failed dependency as neither, so it cannot be mistaken for either", () => {
    expect(isUnknownPlanDayReason("data_unavailable")).toBe(true);
    expect(isStructuralPlanDayReason("data_unavailable")).toBe(false);
    expect(isUnknownPlanDayReason("insufficient_history")).toBe(false);
  });

  it("counts every closable gap, so the number can reach zero", () => {
    expect(isStructuralPlanDayReason("feed_stale")).toBe(false);
    expect(isStructuralPlanDayReason("never_measured")).toBe(false);
    expect(isStructuralPlanDayReason("insufficient_history")).toBe(false);
    expect(isStructuralPlanDayReason("no_hourly_shape")).toBe(false);
    expect(isStructuralPlanDayReason("no_forecast")).toBe(false);
    expect(isStructuralPlanDayReason("hours_unknown")).toBe(false);
    expect(isStructuralPlanDayReason("no_observations")).toBe(false);
  });
});
