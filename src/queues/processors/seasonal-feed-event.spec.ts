import {
  FEED_EVENT_CLUSTER_THRESHOLDS,
  findSharedMinuteClusters,
} from "./queue-percentile.processor";

/**
 * Forty seasons do not start in the same minute.
 *
 * On 2026-06-07 at 13:18 UTC thirty-eight Europa-Park rides stopped reporting
 * OPERATING within one minute, and eighteen at Rulantica twenty-five minutes
 * earlier; Universal Studios Singapore did it on 2026-04-25, Six Flags Fiesta
 * Texas on 2026-04-12, Knott's Berry Farm two days after that. The parks
 * stayed open and the rides kept being reported — as CLOSED, for months. To a
 * behavioural detector that is the exact signature of a season, so it called
 * half of Europa-Park seasonal: 95 rides across five parks.
 *
 * It is one upstream write. A real seasonal closure is ragged, because each
 * ride stops when it stops.
 *
 * Two of those five clusters are no longer suppressed. Fiesta Texas at 18.75 %
 * and Knott's Berry Farm at 17 % fall below the 20 % line this guard has used
 * since PAR-1070; the same minute at either park would now be read as a season.
 * They stay named here because they are what the guard was built from, not
 * because the guard still catches them.
 */
describe("findSharedMinuteClusters", () => {
  // The value production runs on, not a copy of it: a spec that carried its
  // own 0.15 stayed green through the whole move to 0.20 and proved nothing
  // about the detector.
  const thresholds = FEED_EVENT_CLUSTER_THRESHOLDS;
  const at = (iso: string) => ({ at: new Date(iso) });

  const cluster = (parkId: string, count: number) =>
    Array.from({ length: count }, (_, i) => ({
      attractionId: `${parkId}-ride-${i}`,
      parkId,
    }));

  const lastOperatingFor = (
    rides: Array<{ attractionId: string }>,
    iso: string,
  ) => new Map(rides.map((r) => [r.attractionId, at(iso)]));

  it("finds the cluster that took half a park down in one minute", () => {
    const rides = cluster("europa-park", 38);
    const clusters = findSharedMinuteClusters(
      rides,
      lastOperatingFor(rides, "2026-06-07T13:18:40Z"),
      new Map([["europa-park", 90]]),
      thresholds,
    );

    expect(clusters).toHaveLength(1);
    expect(clusters[0]).toMatchObject({
      parkId: "europa-park",
      minute: "2026-06-07T13:18",
      tracked: 90,
    });
    expect(clusters[0].attractionIds).toHaveLength(38);
  });

  it("leaves a ragged closure alone — each ride stopped on its own", () => {
    const rides = cluster("phantasialand", 8);
    const lastOperating = new Map(
      rides.map((ride, index) => [
        ride.attractionId,
        // Same day, minutes apart: the shape of rides closing one by one.
        at(`2026-01-25T18:${String(10 + index).padStart(2, "0")}:00Z`),
      ]),
    );

    expect(
      findSharedMinuteClusters(
        rides,
        lastOperating,
        new Map([["phantasialand", 40]]),
        thresholds,
      ),
    ).toEqual([]);
  });

  it("does not call four rides a feed event", () => {
    const rides = cluster("small-park", 4);
    expect(
      findSharedMinuteClusters(
        rides,
        lastOperatingFor(rides, "2026-04-01T20:00:00Z"),
        new Map([["small-park", 8]]),
        thresholds,
      ),
    ).toEqual([]);
  });

  it("does not call five rides in a two-hundred-ride park a feed event", () => {
    const rides = cluster("huge-park", 5);
    expect(
      findSharedMinuteClusters(
        rides,
        lastOperatingFor(rides, "2026-04-01T20:00:00Z"),
        new Map([["huge-park", 200]]),
        thresholds,
      ),
    ).toEqual([]);
  });

  it("reads a whole water area closing on one evening as a season", () => {
    // Carowinds: 14 of 73 tracked rides, all at 2026-09-07 20:15, and all of
    // them Carolina Harbor — one water area over the Labor Day weekend, while
    // the park ran another 23 open days. At 19.2 % the old 15 % line read that
    // shape as a feed event and left all 14 without season_out_since
    // (PAR-1070).
    const rides = cluster("carowinds", 14);
    expect(
      findSharedMinuteClusters(
        rides,
        lastOperatingFor(rides, "2026-09-07T20:15:10Z"),
        new Map([["carowinds", 73]]),
        thresholds,
      ),
    ).toEqual([]);
  });

  it("still suppresses a cluster that is exactly a fifth of its park", () => {
    // The line itself: 20 % is a feed event, 19 % is not, and the comparison
    // is `< minParkShare` rather than `<=`.
    const rides = cluster("boundary-park", 20);
    expect(
      findSharedMinuteClusters(
        rides,
        lastOperatingFor(rides, "2026-09-07T20:15:00Z"),
        new Map([["boundary-park", 100]]),
        thresholds,
      ),
    ).toHaveLength(1);

    const justUnder = cluster("boundary-park", 19);
    expect(
      findSharedMinuteClusters(
        justUnder,
        lastOperatingFor(justUnder, "2026-09-07T20:15:00Z"),
        new Map([["boundary-park", 100]]),
        thresholds,
      ),
    ).toEqual([]);
  });

  it("ignores rides that were never OPERATING — a different question", () => {
    const rides = cluster("never-park", 9);
    expect(
      findSharedMinuteClusters(rides, new Map(), new Map(), thresholds),
    ).toEqual([]);
  });
});
