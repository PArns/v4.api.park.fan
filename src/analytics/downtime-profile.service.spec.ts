import { DOWNTIME_GATES, decideProfile } from "./downtime-profile.service";
import type { ProfileInputs } from "./downtime-profile.service";
import { isDurationUsable } from "../queues/processors/downtime-reconstruction.processor";

/**
 * The gates are the difference between a figure and a claim about a named
 * company, so they get a test each rather than one happy path.
 *
 * The order matters as much as the thresholds: "this park cannot report an
 * outage" must never be answered with "this ride had none", because the second
 * reads as a compliment the data cannot pay.
 */

/** A ride comfortably over every threshold. Each test spoils exactly one thing. */
const HEALTHY: ProfileInputs = {
  parkId: "p1",
  operatingMinutes: 200 * 60,
  downMinutes: 400,
  observedDays: 80,
  outages: 40,
  usableDurations: 30,
  censored: 4,
  firstHalf: 20,
  secondHalf: 20,
  medianMinutes: 25,
  longestMinutes: 200,
  lastMergedAt: null,
  lifetimeObservedDays: 400,
};

describe("decideProfile", () => {
  it("publishes when everything clears", () => {
    const d = decideProfile(HEALTHY, "reports");
    expect(d.figures).toBe(true);
    expect(d.reason).toBeNull();
    // 400 down over (400 down + 12000 operating).
    expect(Number(d.downShare)).toBeCloseTo(400 / 12400, 4);
  });

  it("refuses a park that cannot report an outage at all, before anything else", () => {
    // Checked first on purpose. A park with no wiki mapping and a ride with no
    // outages are opposite statements and only the first is about us.
    expect(decideProfile(HEALTHY, "not_capable").reason).toBe(
      "not_down_capable",
    );
    expect(
      decideProfile({ ...HEALTHY, outages: 0 }, "not_capable").reason,
    ).toBe("not_down_capable");
  });

  it("refuses an unknown park rather than defaulting to publishable", () => {
    expect(decideProfile(HEALTHY, undefined).figures).toBe(false);
  });

  it("refuses a park in the artefact regime however many events it has", () => {
    // Its intervals rest on single readings: what a duration measures there is
    // how fast the queue drained after the resolver rewrote the DOWN away.
    expect(decideProfile(HEALTHY, "artefact").reason).toBe("artefact_regime");
  });

  it("refuses a park with no published hours", () => {
    // Its operating window is inferred from ride activity, so normalising ride
    // downtime by it runs in a circle.
    expect(decideProfile(HEALTHY, "no_schedule").reason).toBe("no_schedule");
  });

  it("refuses a ride whose history is two interleaved series", () => {
    const merged = { ...HEALTHY, lastMergedAt: new Date("2026-08-01") };
    expect(decideProfile(merged, "reports").reason).toBe("recently_merged");
  });

  it("refuses a ride too new to have a steady state", () => {
    const fresh = {
      ...HEALTHY,
      lifetimeObservedDays: DOWNTIME_GATES.newRideOperatingDays - 1,
    };
    expect(decideProfile(fresh, "reports").reason).toBe("new_ride");
  });

  it("refuses below the event floor", () => {
    const thin = { ...HEALTHY, outages: DOWNTIME_GATES.minOutages - 1 };
    expect(decideProfile(thin, "reports").reason).toBe("thin_events");
  });

  it("refuses when the events exist but too few have a usable length", () => {
    // 30 outages of which 5 ended observably: a median over five is a sample,
    // not a ride.
    const thin = { ...HEALTHY, usableDurations: 5 };
    expect(decideProfile(thin, "reports").reason).toBe("thin_events");
  });

  it("refuses on thin exposure even with plenty of events", () => {
    // A ride that broke forty times over twenty operating hours is telling you
    // about twenty hours.
    const thin = { ...HEALTHY, operatingMinutes: 20 * 60 };
    expect(decideProfile(thin, "reports").reason).toBe("thin_exposure");
    const fewDays = { ...HEALTHY, observedDays: 10 };
    expect(decideProfile(fewDays, "reports").reason).toBe("thin_exposure");
  });

  it("refuses when a quarter of the intervals are censored", () => {
    const censored = { ...HEALTHY, censored: 20 };
    const d = decideProfile(censored, "reports");
    expect(d.figures).toBe(false);
    expect(d.censoredShare).toBeCloseTo(0.5, 3);
  });

  it("refuses a ride whose two halves disagree", () => {
    // Eleven outages then two is not a rate. One number over the window would
    // hide exactly the change a reader is looking for.
    const shifted = { ...HEALTHY, firstHalf: 33, secondHalf: 7 };
    expect(decideProfile(shifted, "reports").reason).toBe("inhomogeneous");
  });

  it("does not run the homogeneity check on halves too thin to speak", () => {
    // 20 vs 3 is a ratio of 6.7 and would fail the test — but three events in a
    // half say nothing, so the check stays silent rather than refusing for a
    // reason it cannot support.
    const lopsided = { ...HEALTHY, firstHalf: 37, secondHalf: 3 };
    expect(decideProfile(lopsided, "reports").figures).toBe(true);
  });

  it("reports the censored share even when it withholds", () => {
    // The share is a diagnostic and belongs in the row whatever the verdict, so
    // a later look can tell "thin" from "unobservable".
    const d = decideProfile({ ...HEALTHY, outages: 4, censored: 2 }, "reports");
    expect(d.figures).toBe(false);
    expect(d.censoredShare).toBeCloseTo(0.5, 3);
  });

  it("never emits a share when it withholds the figures", () => {
    for (const regime of ["not_capable", "artefact", "no_schedule"] as const) {
      expect(decideProfile(HEALTHY, regime).downShare).toBeNull();
    }
  });
});

describe("isDurationUsable", () => {
  const base = {
    endReason: "recovered",
    likelyWorksPeriod: false,
    startCensored: false,
    operatingMinutes: 60,
    observedOperatingMinutes: 60,
  };

  it("accepts an observed recovery", () => {
    expect(isDurationUsable(base)).toBe(true);
  });

  it("accepts a reclassification as an observed end", () => {
    // DOWN turning into REFURBISHMENT is the operator saying what it is, not us
    // losing sight of the ride.
    expect(isDurationUsable({ ...base, endReason: "reclassified" })).toBe(true);
  });

  it("rejects every censored end", () => {
    for (const endReason of [
      "closed",
      "ongoing",
      "window_edge",
      "gap",
      "unconfirmed",
      "source_absent",
    ]) {
      expect(isDurationUsable({ ...base, endReason })).toBe(false);
    }
  });

  it("rejects an interval whose start was never seen", () => {
    expect(isDurationUsable({ ...base, startCensored: true })).toBe(false);
  });

  it("rejects a works period", () => {
    expect(isDurationUsable({ ...base, likelyWorksPeriod: true })).toBe(false);
  });

  it("rejects an interval our own heartbeat mostly wrote", () => {
    // 20 of 60 minutes backed by real readings: the rest is the writer
    // repeating itself, and a median built out of that measures the writer.
    expect(isDurationUsable({ ...base, observedOperatingMinutes: 20 })).toBe(
      false,
    );
    expect(isDurationUsable({ ...base, observedOperatingMinutes: 30 })).toBe(
      true,
    );
  });

  it("gives censoring its own reason, not thin_events", () => {
    // "Too few outages" and "many outages whose end we did not see" send a
    // reader looking in different places, and only the second describes a ride
    // that breaks often. Censoring here is strongly seasonal — a spell ending
    // because the park shut for the winter ran 89.8 % in March against 20.5 %
    // in September — so this reason comes and goes with the season by design.
    const censored = {
      ...HEALTHY,
      outages: 40,
      censored: 20, // half, well past maxCensoredShare
    };
    const decision = decideProfile(censored, "reports");
    expect(decision.figures).toBe(false);
    expect(decision.reason).toBe("heavily_censored");
  });
});
