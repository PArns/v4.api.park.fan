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

  /** The TypeScript mirror of the `ops_seen` running count. */
  function opsSeen(rows: Array<{ breaks: boolean; st: string }>): number[] {
    let n = 0;
    return rows.map((r) => {
      if (r.breaks && r.st === "OPERATING") n++;
      return n;
    });
  }

  it("sees the ride running again even when the sighting lands in an empty group", () => {
    // DOWN DOWN CLOSED OPERATING DOWN DOWN.
    //
    // The CLOSED closes the first run, so the OPERATING falls into a group of
    // its own that contains no run — invisible to `end_state`. The stitch then
    // read prev_end_state = CLOSED, called the seam one interrupted outage and
    // merged two separate ones. Measured on Cedar Point over 7 days: 24 of 330
    // run pairs inside the 20-hour window, and the longest "outage" came back
    // as 4068 minutes against 1185 once split.
    const rows = [
      { breaks: false, st: "DOWN" },
      { breaks: false, st: "DOWN" },
      { breaks: true, st: "CLOSED" },
      { breaks: true, st: "OPERATING" },
      { breaks: false, st: "DOWN" },
      { breaks: false, st: "DOWN" },
    ];
    const ops = opsSeen(rows);
    // The first run ends at index 1, the second starts at index 4.
    const opsAtEndOfFirst = ops[1];
    const opsAtStartOfSecond = ops[4];
    expect(opsAtStartOfSecond).toBeGreaterThan(opsAtEndOfFirst);
  });

  it("does not split when the ride was never seen running between runs", () => {
    // DOWN CLOSED DOWN — one outage the park closure interrupted, not two.
    const rows = [
      { breaks: false, st: "DOWN" },
      { breaks: true, st: "CLOSED" },
      { breaks: false, st: "DOWN" },
    ];
    const ops = opsSeen(rows);
    expect(ops[2]).toBe(ops[0]);
  });

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
    parkId: "park-1",
    parkSlug: "phantasialand",
    parkName: "Phantasialand",
    parkCity: "Brühl",
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

  it("counts single-reading runs but no longer calls them an artefact", () => {
    // This test used to assert the opposite, and the assertion was wrong.
    //
    // Measured over production (90 days, 78 parks, 150 132 events), the share
    // of single-reading runs correlates with "share of outages shorter than an
    // hour" at r = 0.996. `queue_data` is a change log, so an outage that ends
    // before the hourly heartbeat fires writes exactly one row — 99.9 % of
    // single-row runs are under 65 minutes, and 99.8 % of them have an OBSERVED
    // end, the reading that says the ride is running again. One reading is what
    // a short outage looks like, not an erased one.
    //
    // The old threshold marked 47 of 78 parks artefact, including EPCOT, which
    // has a single second-source row in thirty days and so cannot be eroded by
    // the ConflictResolver at all.
    const { service } = serviceWith([
      ride("2026-08-01T10:00:00Z", "2026-08-01T10:05:00Z", 1),
      ride("2026-08-02T10:12:00Z", "2026-08-02T10:37:00Z", 1),
      ride("2026-08-03T10:24:00Z", "2026-08-03T10:49:00Z", 1),
      ride("2026-08-04T10:36:00Z", "2026-08-04T11:36:00Z", 8),
    ]);

    return service.measure({}).then((result) => {
      expect(result.totals.singleReadingRuns).toBe(3);
      expect(result.parks[0].singleReadingShare).toBeCloseTo(0.75, 2);
      // Timestamps spread across the hour: a feed we can read a duration from.
      expect(result.parks[0].regime).toBe("reports");
    });
  });

  it("calls a park an artefact when its feed publishes on the hour", () => {
    // What the regime always meant: „Störungsmeldungen liegen nur stundengenau
    // vor. Eine Dauer lässt sich daraus nicht ablesen." Every edge on the same
    // minute of the hour is that feed, and nothing else is.
    const onTheHour = Array.from({ length: 12 }, (_, i) =>
      ride(
        `2026-08-${String(i + 1).padStart(2, "0")}T10:00:00Z`,
        `2026-08-${String(i + 1).padStart(2, "0")}T12:00:00Z`,
        3,
      ),
    );
    const { service } = serviceWith(onTheHour);

    return service.measure({}).then((result) => {
      expect(result.parks[0].onTheHourShare).toBe(1);
      expect(result.parks[0].regime).toBe("artefact");
    });
  });

  it("measures resolution on the most common minute, not on minute zero", () => {
    // A park at a :30 offset publishing hourly piles up on :30. Hard-coding
    // minute zero would read it as fine-grained and let a duration through.
    const halfPast = Array.from({ length: 12 }, (_, i) =>
      ride(
        `2026-08-${String(i + 1).padStart(2, "0")}T10:30:00Z`,
        `2026-08-${String(i + 1).padStart(2, "0")}T12:30:00Z`,
        3,
      ),
    );
    const { service } = serviceWith(halfPast);

    return service.measure({}).then((result) => {
      expect(result.parks[0].regime).toBe("artefact");
    });
  });

  it("does not judge resolution on too few edges", () => {
    // Two runs on the hour is not an hourly feed, it is two runs. Calling a
    // park unreadable on four readings is the same error in the other
    // direction.
    const { service } = serviceWith([
      ride("2026-08-01T10:00:00Z", "2026-08-01T12:00:00Z", 3),
      ride("2026-08-02T10:00:00Z", "2026-08-02T12:00:00Z", 3),
    ]);

    return service.measure({}).then((result) => {
      expect(result.parks[0].regime).toBe("reports");
    });
  });

  it("groups per park id, because a slug is not unique", () => {
    // `disneyland-park` is Anaheim AND Paris. Grouping the per-park report on
    // the slug merged the two into one row with both parks' rides in it.
    const anaheim = { ...ride("2026-08-01T10:07:00Z", "2026-08-01T10:37:00Z") };
    const paris = {
      ...ride("2026-08-01T11:13:00Z", "2026-08-01T11:43:00Z"),
      attractionId: "a2",
      parkId: "park-2",
      parkCity: "Paris",
    };
    const { service } = serviceWith([anaheim, paris]);

    return service.measure({}).then((result) => {
      expect(result.parks).toHaveLength(2);
      expect(result.parks.map((p) => p.parkCity).sort()).toEqual([
        "Brühl",
        "Paris",
      ]);
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
