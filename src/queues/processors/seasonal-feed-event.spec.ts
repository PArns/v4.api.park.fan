import { findSharedMinuteClusters } from "./queue-percentile.processor";

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
 */
describe("findSharedMinuteClusters", () => {
  const thresholds = { minSize: 5, minParkShare: 0.15 };
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

  it("keeps the 19 % case, because that one really happened", () => {
    // Six Flags Fiesta Texas: 15 of 80, all at 2026-04-12 22:53.
    const rides = cluster("fiesta-texas", 15);
    expect(
      findSharedMinuteClusters(
        rides,
        lastOperatingFor(rides, "2026-04-12T22:53:00Z"),
        new Map([["fiesta-texas", 80]]),
        thresholds,
      ),
    ).toHaveLength(1);
  });

  it("ignores rides that were never OPERATING — a different question", () => {
    const rides = cluster("never-park", 9);
    expect(
      findSharedMinuteClusters(rides, new Map(), new Map(), thresholds),
    ).toEqual([]);
  });
});
