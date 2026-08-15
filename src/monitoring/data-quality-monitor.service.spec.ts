import { DataQualityMonitorService } from "./data-quality-monitor.service";

/**
 * Both detectors exist because of a specific failure that ran for weeks:
 * `detect-seasonal` threw on every run for 73 days, and ThemeParks.wiki dropped
 * 44 Europa-Park attractions for 10 weeks. Neither was visible in any check —
 * `freshness()` reads global maxima, which stayed healthy throughout.
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
