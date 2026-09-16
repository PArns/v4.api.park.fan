import { DataQualityMonitorService } from "./data-quality-monitor.service";

/**
 * Each detector exists because of a specific failure that ran for weeks:
 * `detect-seasonal` threw on every run for 73 days, and ThemeParks.wiki dropped
 * 44 Europa-Park attractions for 10 weeks. Neither was visible in any check —
 * `freshness()` reads global maxima, which stayed healthy throughout.
 * La Ronde came third: 84 days with no reading at all and a schedule published
 * through 2027-08-31, invisible to both of the others.
 */
describe("DataQualityMonitorService", () => {
  const build = (query: jest.Mock, client: unknown = {}) =>
    new DataQualityMonitorService(
      { query } as never,
      {
        client,
      } as never,
    );

  describe("findSilencedClusters", () => {
    it("passes the window, silence floor and cluster size through", async () => {
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findSilencedClusters(30, 5, 8);

      const [, params] = query.mock.calls[0];
      expect(params).toEqual([30, 5, 8]);
    });

    it("gates on an absolute count of live attractions, never a ratio", async () => {
      // A ratio is self-defeating: Europa-Park lost 44 of ~96, leaving 45%
      // live, so a 70% health gate would have hidden the largest cluster in
      // the data. What separates a dropped feed from a park closed for the
      // season is that the closed park has NOBODY operating.
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findSilencedClusters();

      const [sql] = query.mock.calls[0] as [string];
      expect(sql).toMatch(/HAVING count\(\*\) FILTER[\s\S]*?>= 3/);
      expect(sql).not.toMatch(/0\.7/);
    });

    it("requires rows to still be arriving, so a silence is not a deletion", async () => {
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findSilencedClusters();

      const [sql] = query.mock.calls[0] as [string];
      expect(sql).toMatch(/last_row > now\(\) - INTERVAL '2 days'/);
      // Free-flow attractions never report OPERATING by nature — including
      // them would make every curated playground look like an incident.
      expect(sql).toMatch(/NOT a\.open_with_park/);
    });

    it("maps a cluster row into something a warning can name", async () => {
      const query = jest.fn().mockResolvedValue([
        {
          park_id: "p1",
          park_name: "Europa-Park",
          last_op: "2026-06-07",
          n: "44",
          names: ["Ball Pool", "Crazy Taxi"],
        },
      ]);

      expect(await build(query).findSilencedClusters()).toEqual([
        {
          parkId: "p1",
          parkName: "Europa-Park",
          attractionCount: 44,
          lastOperating: "2026-06-07",
          sampleNames: ["Ball Pool", "Crazy Taxi"],
        },
      ]);
    });
  });

  describe("findScheduledButSilentParks", () => {
    // The behaviour lives in a real database; `test/e2e/scheduled-but-silent-parks`
    // seeds the pairs and reads the rows back. What is worth pinning here is the
    // shape a regular expression CAN see: the two gates that make the query say
    // something rather than everything.
    it("passes the silence window through", async () => {
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findScheduledButSilentParks(45);

      expect(query.mock.calls[0][1]).toEqual([45]);
    });

    it("keys on a FUTURE operating day, not on any operating day", async () => {
      // Without the comparison a park whose schedule ran out in June is in the
      // same state as one publishing days into 2027, and only the second is a
      // contradiction worth a warning.
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findScheduledButSilentParks();

      const [sql] = query.mock.calls[0] as [string];
      expect(sql).toMatch(
        /se\.date > \(now\(\) AT TIME ZONE p\.timezone\)::date/,
      );
    });

    it("counts only observed readings, so our own bookkeeping cannot clear a park", async () => {
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findScheduledButSilentParks();

      const [sql] = query.mock.calls[0] as [string];
      expect(sql).toContain("system-reconciliation");
      expect(sql).toContain("system-heartbeat");
      // The anti-join IS the detector: a park absent from `last_seen` is the
      // reported one.
      expect(sql).toMatch(/LEFT JOIN last_seen[\s\S]*?WHERE ls\.ts IS NULL/);
    });

    it("maps a row into something a warning can name", async () => {
      const query = jest.fn().mockResolvedValue([
        {
          park_id: "p9",
          park_name: "La Ronde",
          n: "38",
          last_reading: "2026-06-24",
          future_days: "349",
          last_day: "2027-08-31",
        },
      ]);

      expect(await build(query).findScheduledButSilentParks()).toEqual([
        {
          parkId: "p9",
          parkName: "La Ronde",
          attractionCount: 38,
          lastReading: "2026-06-24",
          futureOperatingDays: 349,
          lastScheduledDay: "2027-08-31",
        },
      ]);
    });

    it("carries a park that has never reported as a null reading, not as a zero", async () => {
      // Four of the five parks found on 2026-09-16 have no reading at all.
      // `Number(null)` is 0, and a 0 here would print as a date of sorts.
      const query = jest.fn().mockResolvedValue([
        {
          park_id: "p10",
          park_name: "Paradise Country",
          n: "12",
          last_reading: null,
          future_days: "118",
          last_day: "2027-01-13",
        },
      ]);

      const [row] = await build(query).findScheduledButSilentParks();
      expect(row.lastReading).toBeNull();
    });
  });

  describe("findFailingJobs", () => {
    const clientWith = (
      failedIds: string[],
      hash: Record<string, string[]>,
    ) => ({
      zrange: jest
        .fn()
        .mockImplementation((key: string) =>
          key.includes("analytics")
            ? Promise.resolve(failedIds)
            : Promise.resolve([]),
        ),
      hmget: jest
        .fn()
        .mockImplementation((key: string) =>
          Promise.resolve(hash[key] ?? [null, null, null]),
        ),
    });

    it("groups a queue's failures by job name and keeps the newest reason", async () => {
      // The real fixture: detect-seasonal's corpse in production still reads
      // 'syntax error at or near "attr_activity"'.
      const client = clientWith(["78", "79"], {
        "parkfan:analytics:78": ["detect-seasonal", "older failure", "1000"],
        "parkfan:analytics:79": [
          "detect-seasonal",
          'syntax error at or near "attr_activity"\n    at Parser...',
          "2000",
        ],
      });

      const [failing] = await build(jest.fn(), client).findFailingJobs();

      expect(failing.queue).toBe("analytics");
      expect(failing.jobName).toBe("detect-seasonal");
      expect(failing.failures).toBe(2);
      // The stack is noise; the first line is the fact.
      expect(failing.lastReason).toBe(
        'syntax error at or near "attr_activity"',
      );
      expect(failing.lastFailedAt).toBe(new Date(2000).toISOString());
    });

    it("reports nothing when no queue holds a failure", async () => {
      const client = clientWith([], {});
      expect(await build(jest.fn(), client).findFailingJobs()).toEqual([]);
    });
  });
});
