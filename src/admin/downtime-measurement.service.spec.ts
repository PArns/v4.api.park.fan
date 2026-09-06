import { DowntimeMeasurementService } from "./downtime-measurement.service";

/**
 * The grouping in `outageEvents` is the whole measurement, and it is the kind of
 * window function that looks right and is off by one. These pin the two
 * decisions that decide whether an outage comes back with an end at all, by
 * running the same arithmetic the SQL does over an ordered row list.
 *
 * The SQL itself is exercised against a real Postgres in the phase 0 run; what
 * is worth freezing here is the intent, because a reworded frame is silent.
 */
describe("outage grouping (the arithmetic the SQL performs)", () => {
  /** The TypeScript mirror of the `seq` CTE's group id. */
  function groupIds(breaks: boolean[]): number[] {
    const out: number[] = [];
    let before = 0;
    for (const isBreak of breaks) {
      out.push(before);
      if (isBreak) before++;
    }
    return out;
  }

  it("keeps the ending reading in the run it ends", () => {
    // D D C D  ->  the CLOSED row must land in group 0 beside the two DOWNs,
    // not open group 1. Counting the current row instead of only the ones
    // before it is what makes every outage come back open-ended.
    expect(groupIds([false, false, true, false])).toEqual([0, 0, 0, 1]);
  });

  it("leaves a still-running outage without an end", () => {
    // D D  ->  one group, no breaking row in it, so ended_at is NULL and the
    // interval is reported as ongoing rather than given the window's edge.
    expect(groupIds([false, false])).toEqual([0, 0]);
  });

  it("separates two outages by the recovery between them", () => {
    // D C D C  ->  two runs, each with its own end.
    expect(groupIds([false, true, false, true])).toEqual([0, 0, 1, 1]);
  });

  it("does not invent a run out of consecutive recoveries", () => {
    // C C  ->  two groups, neither with a non-breaking row. The HAVING clause
    // drops both; without it the measurement would report zero-length outages
    // wherever a park sat closed across two readings.
    expect(groupIds([true, true])).toEqual([0, 1]);
  });
});

describe("DowntimeMeasurementService", () => {
  function serviceWith(rows: unknown[]) {
    const query = jest
      .fn()
      // capabilityCensus runs first
      .mockResolvedValueOnce([
        { capable: "197", total: "213", scheduled: "192" },
      ])
      .mockResolvedValueOnce(rows);
    return {
      service: new DowntimeMeasurementService({
        manager: { query },
      } as never),
      query,
    };
  }

  const ride = (startedAt: string, endedAt: string | null, rowsInRun = 3) => ({
    attractionId: "a1",
    attractionName: "Taron",
    parkSlug: "phantasialand",
    parkName: "Phantasialand",
    startedAt: new Date(startedAt),
    endedAt: endedAt ? new Date(endedAt) : null,
    rowsInRun,
  });

  it("stays in the reports regime while most runs rest on several readings", () => {
    // Two of three is 0.667, under the 0.7 boundary. The threshold is a real
    // decision and not a rounding: a park just under it still gets durations.
    const { service } = serviceWith([
      ride("2026-08-01T10:00:00Z", "2026-08-01T10:05:00Z", 1),
      ride("2026-08-02T10:00:00Z", "2026-08-02T10:05:00Z", 1),
      ride("2026-08-03T10:00:00Z", "2026-08-03T11:00:00Z", 8),
    ]);
    return service.measure({}).then((result) => {
      expect(result.parks[0].singleReadingShare).toBeCloseTo(0.667, 2);
      expect(result.parks[0].regime).toBe("reports");
    });
  });

  it("counts events and leaves the ongoing one out of the durations", () => {
    const { service } = serviceWith([
      ride("2026-08-01T10:00:00Z", "2026-08-01T10:30:00Z"),
      ride("2026-08-02T11:00:00Z", "2026-08-02T12:00:00Z"),
      ride("2026-08-03T09:00:00Z", null),
    ]);

    return service.measure({ days: 90 }).then((result) => {
      expect(result.totals.outages).toBe(3);
      expect(result.totals.ongoing).toBe(1);
      // Median over the two with an observed end: 30 and 60.
      expect(result.rides[0].medianMinutes).toBe(45);
      expect(result.rides[0].longestMinutes).toBe(60);
    });
  });

  it("counts single-reading runs separately from the rest", () => {
    // A run of one row proves nothing about how long it lasted: the reading
    // that would have ended it never came. Folding it into the median would
    // make an erased DOWN look like a two-minute outage.
    const { service } = serviceWith([
      ride("2026-08-01T10:00:00Z", "2026-08-01T10:05:00Z", 1),
      ride("2026-08-02T10:00:00Z", "2026-08-02T10:05:00Z", 1),
      ride("2026-08-03T10:00:00Z", "2026-08-03T10:05:00Z", 1),
      ride("2026-08-04T10:00:00Z", "2026-08-04T11:00:00Z", 8),
    ]);

    return service.measure({}).then((result) => {
      expect(result.totals.singleReadingRuns).toBe(3);
      // Three of four runs are one reading long. Past 70 % the park is in the
      // artefact regime: what is being measured there is how fast a queue
      // drains after the ConflictResolver rewrites the DOWN away, not how long
      // the ride stood still.
      expect(result.parks[0].singleReadingShare).toBeCloseTo(0.75, 2);
      expect(result.parks[0].regime).toBe("artefact");
    });
  });

  it("reads capability from configuration, not from the outcome", () => {
    // A park with no outages in the window and a park that can never report one
    // are two different statements, and only the census can tell them apart.
    const { service } = serviceWith([]);
    return service.measure({}).then((result) => {
      expect(result.capability).toEqual({
        downCapableParks: 197,
        totalParks: 213,
        parksWithSchedule: 192,
      });
      expect(result.totals.outages).toBe(0);
    });
  });

  it("clamps the window rather than accepting any number of days", () => {
    const { service, query } = serviceWith([]);
    return service.measure({ days: 5000 }).then((result) => {
      expect(result.windowDays).toBe(365);
      const since = query.mock.calls[1][1][2] as Date;
      const spanDays = (Date.now() - since.getTime()) / 86_400_000;
      expect(Math.round(spanDays)).toBe(365);
    });
  });
});
