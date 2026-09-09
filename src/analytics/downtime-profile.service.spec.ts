import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import {
  DOWNTIME_GATES,
  DowntimeProfileService,
  decideProfile,
} from "./downtime-profile.service";
import type { ProfileInputs } from "./downtime-profile.service";
import { ParkDowntimeCoverage } from "./entities/park-downtime-coverage.entity";
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

  it("withholds for a park whose feed has never said DOWN", () => {
    // `wiki_entity_id IS NOT NULL` makes a DOWN possible; it does not make the
    // feed carry one. 102 of 182 scheduled parks had produced no DOWN row in
    // 180 days — Phantasialand, Energylandia, Alton Towers, Parc Asterix — with
    // MORE observed operating time between them than the reporting parks have.
    // Read as `reports`, every one of those rides would have said "no outage
    // reported in 90 days", which is our blindness printed as an operator's
    // clean record.
    const decision = decideProfile(HEALTHY, "never_reports");
    expect(decision.figures).toBe(false);
    expect(decision.reason).toBe("park_never_reports");
  });

  it("keeps that separate from a park with no capable source at all", () => {
    // Same outcome for the ride, different sentence to the reader, and only one
    // of them is a statement about configuration.
    expect(decideProfile(HEALTHY, "not_capable").reason).toBe(
      "not_down_capable",
    );
    expect(decideProfile(HEALTHY, "never_reports").reason).toBe(
      "park_never_reports",
    );
  });

  it("only withholds for a merge inside the window", () => {
    // `last_merged_at` is written once and never cleared, so an unbounded check
    // silenced a ride's figures forever. Stamping park merges — where colliding
    // rides actually happen, 29 in the USH consolidation alone — would have
    // made that permanent for a large named set.
    const windowFrom = new Date("2026-06-09T00:00:00Z");
    const longAgo = {
      ...HEALTHY,
      lastMergedAt: new Date("2026-01-15T00:00:00Z"),
    };
    expect(decideProfile(longAgo, "reports", windowFrom).figures).toBe(true);

    const recent = {
      ...HEALTHY,
      lastMergedAt: new Date("2026-07-01T00:00:00Z"),
    };
    expect(decideProfile(recent, "reports", windowFrom)).toMatchObject({
      figures: false,
      reason: "recently_merged",
    });
  });

  it("keeps withholding when the caller cannot say where the window starts", () => {
    // A caller with no windowFrom cannot know the seam has aged out, so the
    // safe answer is the old one.
    const merged = {
      ...HEALTHY,
      lastMergedAt: new Date("2020-01-01T00:00:00Z"),
    };
    expect(decideProfile(merged, "reports").reason).toBe("recently_merged");
  });
});

/**
 * The rebuild itself, over mocked repositories.
 *
 * `decideProfile` above is pure and gets the gate-by-gate treatment. What it
 * cannot see is the three things that decide which rows exist at all: whether a
 * ride that dropped out loses its row, whether a ride whose park publishes no
 * hours ever gets one, and whether the date beside the longest outage survives
 * the trip from the query into the write.
 */
describe("DowntimeProfileService — the rebuild's population", () => {
  const REPORTING_PARK = "11111111-1111-4111-8111-111111111111";
  const SCHEDULE_LESS_PARK = "22222222-2222-4222-8222-222222222222";
  const NOT_CAPABLE_PARK = "33333333-3333-4333-8333-333333333333";

  const LIVE_RIDE = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  const SCHEDULE_LESS_RIDE = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

  const LONGEST_STARTED_AT = "2026-07-12T09:30:00.000Z";

  /** A park row shaped so the regime falls out of the two flags under test. */
  const coverageRow = (
    parkId: string,
    { downCapable = true, hasSchedule = true } = {},
  ) => ({
    parkId,
    downCapable,
    hasSchedule,
    ridesTracked: 10,
    ridesWithOutages: 4,
    outages: 40,
    singleReadingSpells: 2,
    onTheHourShare: 0,
    // Under `minEdgesForResolution`, so the artefact test stays silent and
    // cannot decide the regime instead of the flag being tested.
    resolutionEdges: 0,
    observedOperatingHours: 2000,
    // Not blind: this park has said DOWN, so `never_reports` cannot fire.
    hasEverReportedDown: true,
    medianSpellMinutes: 30,
  });

  /** A ride over every gate, so a withheld verdict is the test's own doing. */
  const publishableRow = (attractionId: string, parkId: string) => ({
    attractionId,
    parkId,
    operatingMinutes: 200 * 60,
    downMinutes: 400,
    observedDays: 80,
    outages: 40,
    usableDurations: 30,
    censored: 4,
    firstHalf: 20,
    secondHalf: 20,
    worksMinutes: 0,
    medianMinutes: 25,
    longestMinutes: 200,
    longestStartedAt: LONGEST_STARTED_AT,
    lastMergedAt: null,
    firstObservedDay: "2025-01-01",
    lifetimeObservedDays: 400,
  });

  let service: DowntimeProfileService;
  let query: jest.Mock;
  let profileUpsert: jest.Mock;
  let managerQuery: jest.Mock;
  /** Every write in order, so "delete before upsert" is checkable. */
  let writes: string[];

  const build = async (coverageRows: unknown[], profileRows: unknown[]) => {
    writes = [];
    query = jest
      .fn()
      .mockResolvedValueOnce(coverageRows)
      .mockResolvedValueOnce(profileRows);
    profileUpsert = jest.fn().mockImplementation(() => {
      writes.push("upsert");
      return Promise.resolve(undefined);
    });
    managerQuery = jest.fn().mockImplementation(() => {
      writes.push("delete");
      return Promise.resolve(undefined);
    });

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DowntimeProfileService,
        {
          provide: DataSource,
          useValue: {
            query,
            transaction: (cb: (m: unknown) => Promise<void>) =>
              cb({
                query: managerQuery,
                getRepository: () => ({ upsert: profileUpsert }),
              }),
          },
        },
        {
          provide: getRepositoryToken(ParkDowntimeCoverage),
          useValue: { upsert: jest.fn() },
        },
      ],
    }).compile();
    service = module.get(DowntimeProfileService);
  };

  /** The rows handed to the profile upsert, flattened across its batches. */
  const saved = () =>
    profileUpsert.mock.calls.flatMap(
      (call) => call[0] as Record<string, unknown>[],
    );

  describe("stale rows", () => {
    it("deletes the profiles of rides that dropped out, before writing", async () => {
      await build(
        [coverageRow(REPORTING_PARK)],
        [publishableRow(LIVE_RIDE, REPORTING_PARK)],
      );
      await service.rebuild(null);

      expect(managerQuery).toHaveBeenCalledTimes(1);
      const [sql, params] = managerQuery.mock.calls[0] as [string, unknown[]];
      expect(sql).toContain("DELETE FROM attraction_downtime_profiles");
      // Scoped both ways: the parks this rebuild produced rows for, minus the
      // rides it kept. A retired ride in REPORTING_PARK is what falls between.
      expect(params[0]).toEqual([REPORTING_PARK]);
      expect(params[1]).toEqual([LIVE_RIDE]);

      // Order is the point. An upsert that lands first would be deleted again.
      expect(writes).toEqual(["delete", "upsert"]);
    });

    it("only deletes rows whose ride is retired or gone, never a live one", async () => {
      await build(
        [coverageRow(REPORTING_PARK)],
        [publishableRow(LIVE_RIDE, REPORTING_PARK)],
      );
      await service.rebuild(null);

      // Dropping out of the population is not the same as ceasing to exist,
      // and the read path cannot tell them apart: a missing row is
      // not_down_capable, a statement about the park's SOURCE. Rides do leave
      // the population while remaining real — the reconstruction wipes
      // attraction_exposure_days for op_day >= scanStart and rewrites only
      // what its own population produces — so without this predicate a wide
      // reconstruction run would print the wrong refusal on live ride pages.
      // The honest outcome there is the row they already have, ageing into
      // stale_data.
      const sql = managerQuery.mock.calls[0][0] as string;
      expect(sql).toContain("NOT EXISTS");
      expect(sql).toContain("a.retired_at IS NULL");
    });

    it("scopes the delete to the parks the rebuild actually produced rows for", async () => {
      // Two parks known to coverage, one of them with no ride in the result.
      // Deleting across both would erase the second park's profiles on the
      // strength of a query that said nothing about it.
      await build(
        [coverageRow(REPORTING_PARK), coverageRow(NOT_CAPABLE_PARK)],
        [publishableRow(LIVE_RIDE, REPORTING_PARK)],
      );
      await service.rebuild(null);

      const params = managerQuery.mock.calls[0][1] as unknown[];
      expect(params[0]).toEqual([REPORTING_PARK]);
      expect(params[0]).not.toContain(NOT_CAPABLE_PARK);
    });

    it("deletes nothing when the rebuild produced no rows at all", async () => {
      // The dangerous case: an exposure table that failed to build returns an
      // empty population, and a whole-table delete would answer
      // not_down_capable for every ride in the catalogue the next morning.
      await build([coverageRow(REPORTING_PARK)], []);
      await service.rebuild(null);

      expect(managerQuery).not.toHaveBeenCalled();
      expect(profileUpsert).not.toHaveBeenCalled();
    });
  });

  describe("a park that publishes no hours", () => {
    it("asks the population query for that park's rides, and only that park's", async () => {
      await build(
        [
          coverageRow(REPORTING_PARK),
          coverageRow(SCHEDULE_LESS_PARK, { hasSchedule: false }),
          coverageRow(NOT_CAPABLE_PARK, { downCapable: false }),
        ],
        [],
      );
      await service.rebuild(null);

      const [sql, params] = query.mock.calls[1] as [string, unknown[]];
      // not_capable stays out: a missing row already says exactly that, so a
      // written one would only repeat it at the cost of a row per ride.
      expect(params[4]).toEqual([SCHEDULE_LESS_PARK]);
      expect(sql).toContain("sched AS (");
      expect(sql).toContain("UNION ALL");
    });

    it("resolves the added ride to no_schedule and not to a thinness reason", async () => {
      // The population fix itself is the test above — this one is about what
      // the added rows then say. They arrive with zero exposure and zero
      // observed days, which is also what `thin_exposure` is measured on, so
      // the regime check has to win. It does, because it runs first; this
      // pins that ordering against the one population that depends on it.
      //
      // Without a row at all the read path reaches toDowntimeBlock(null) and
      // answers not_down_capable — a statement about the park's SOURCE, which
      // here is capable. The six no_schedule translations were unreachable.
      await build(
        [coverageRow(SCHEDULE_LESS_PARK, { hasSchedule: false })],
        [
          {
            ...publishableRow(SCHEDULE_LESS_RIDE, SCHEDULE_LESS_PARK),
            operatingMinutes: 0,
            downMinutes: 0,
            observedDays: 0,
          },
        ],
      );
      await service.rebuild(null);

      expect(saved()).toHaveLength(1);
      expect(saved()[0]).toMatchObject({
        attractionId: SCHEDULE_LESS_RIDE,
        publishable: false,
        withheldReason: "no_schedule",
      });
    });
  });

  describe("the longest outage's date", () => {
    it("writes it beside the minutes instead of a hardcoded null", async () => {
      await build(
        [coverageRow(REPORTING_PARK)],
        [publishableRow(LIVE_RIDE, REPORTING_PARK)],
      );
      await service.rebuild(null);

      expect(saved()[0]).toMatchObject({
        longestMinutes: 200,
        longestStartedAt: new Date(LONGEST_STARTED_AT),
      });
    });

    it("computes it in the same pass as the minutes, tie-broken to the newest", async () => {
      await build([coverageRow(REPORTING_PARK)], []);
      await service.rebuild(null);

      const sql = query.mock.calls[1][0] as string;
      expect(sql).toContain("AS longest_started_at");
      expect(sql).toContain("o.started_at DESC");
    });

    it("withholds the date wherever it withholds the minutes", async () => {
      // The doc publishes the two as a pair. A date with no duration beside it
      // is a sentence with a hole in it.
      await build(
        [coverageRow(REPORTING_PARK)],
        [
          {
            ...publishableRow(LIVE_RIDE, REPORTING_PARK),
            outages: DOWNTIME_GATES.minOutages - 1,
          },
        ],
      );
      await service.rebuild(null);

      expect(saved()[0]).toMatchObject({
        publishable: false,
        withheldReason: "thin_events",
        longestMinutes: null,
        longestStartedAt: null,
      });
    });

    it("leaves it null when no usable outage produced one", async () => {
      await build(
        [coverageRow(REPORTING_PARK)],
        [
          {
            ...publishableRow(LIVE_RIDE, REPORTING_PARK),
            longestMinutes: null,
            longestStartedAt: null,
          },
        ],
      );
      await service.rebuild(null);

      expect(saved()[0]).toMatchObject({
        publishable: true,
        longestMinutes: null,
        longestStartedAt: null,
      });
    });
  });
});
