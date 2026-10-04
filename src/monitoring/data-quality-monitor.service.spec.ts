import {
  DataQualityMonitorService,
  REISSUE_CANDIDATE_METERS,
} from "./data-quality-monitor.service";
import { ABSENT_UPSTREAM_REASON } from "../attractions/services/attraction-retirement.service";

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
          rides: [
            { id: "a1", name: "Ball Pool" },
            { id: "a2", name: "Crazy Taxi" },
          ],
        },
      ]);

      expect(await build(query).findSilencedClusters()).toEqual([
        {
          parkId: "p1",
          parkName: "Europa-Park",
          attractionCount: 44,
          lastOperating: "2026-06-07",
          sampleNames: ["Ball Pool", "Crazy Taxi"],
          attractions: [
            { attractionId: "a1", name: "Ball Pool" },
            { attractionId: "a2", name: "Crazy Taxi" },
          ],
        },
      ]);
    });

    it("leaves out rides whose season is known, so answering the card clears it (PAR-695)", async () => {
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findSilencedClusters();

      const [sql] = query.mock.calls[0] as [string];
      expect(sql).toMatch(
        /NOT COALESCE\(a\.curated_is_seasonal, a\.is_seasonal\)/,
      );
      expect(sql).toMatch(/a\.retired_at IS NULL/);
    });
  });

  /**
   * PAR-684, option B: the absence step cannot know a season the detector has
   * not seen yet, so a maze in its first year is retired like a demolished
   * ride. This list is what a human answers.
   */
  describe("findAbsenceRetiredUnreviewed", () => {
    it("asks only for rows the absence step retired, by the exact reason", async () => {
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findAbsenceRetiredUnreviewed();

      const [sql, params] = query.mock.calls[0] as [string, unknown[]];
      expect(sql).toMatch(/a\.retired_reason = \$1/);
      expect(params).toEqual([ABSENT_UPSTREAM_REASON]);
    });

    it("leaves out every row someone already answered, and every row the detector calls seasonal", async () => {
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findAbsenceRetiredUnreviewed();

      const [sql] = query.mock.calls[0] as [string];
      expect(sql).toMatch(/a\.curated_is_seasonal IS NULL/);
      expect(sql).toMatch(/NOT a\.is_seasonal/);
    });

    it("bounds the last-reading lookup, so it does not plan against every chunk", async () => {
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findAbsenceRetiredUnreviewed();

      const [sql] = query.mock.calls[0] as [string];
      expect(sql).toMatch(/qd\.timestamp > now\(\) - INTERVAL '400 days'/);
    });

    it("maps a row into something an editor can open", async () => {
      const query = jest.fn().mockResolvedValue([
        {
          id: "a1",
          name: "Asylum",
          slug: "asylum",
          park_id: "p1",
          park_name: "Parque de Atracciones de Madrid",
          retired_at: new Date("2026-10-03T03:29:45Z"),
          last_reading: "2026-04-23",
        },
      ]);

      expect(await build(query).findAbsenceRetiredUnreviewed()).toEqual([
        {
          attractionId: "a1",
          name: "Asylum",
          slug: "asylum",
          parkId: "p1",
          parkName: "Parque de Atracciones de Madrid",
          retiredAt: "2026-10-03T03:29:45.000Z",
          lastReading: "2026-04-23",
        },
      ]);
    });
  });

  /**
   * PAR-686: the wiki re-issues a ride under a new id AND a new name, which the
   * sync's exact-name claim cannot see. The list shows every nearby pair to a
   * human, with a name hint; nothing merges by itself.
   */
  describe("findReissueCandidates", () => {
    const row = (over: Record<string, unknown> = {}) => ({
      park_id: "p1",
      park_name: "Six Flags Great America",
      o_id: "old",
      o_name: "HAUNTED HOUSE: SAW: Legacy of Terror",
      o_slug: "haunted-house-saw-legacy-of-terror",
      o_ext: "wiki-old",
      o_created: new Date("2025-12-24T00:00:00Z"),
      o_last: "2025-11-01",
      n_id: "new",
      n_name: "SAW Legacy of Terror",
      n_slug: "saw-legacy-of-terror",
      n_ext: "wiki-new",
      n_created: new Date("2026-09-17T00:00:00Z"),
      n_last: "2026-10-03",
      meters: "24.98",
      ...over,
    });

    it("asks for absence-retired rows only, inside the radius, minus pairs marked not-a-duplicate", async () => {
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findReissueCandidates();

      const [sql, params] = query.mock.calls[0] as [string, unknown[]];
      expect(sql).toMatch(/o\.retired_reason = \$1/);
      expect(sql).toMatch(/pr\.meters <= \$2/);
      expect(sql).toMatch(/m\.kind = 'not_a_duplicate'/);
      // Marks are stored in canonical order; the lookup has to use it too.
      expect(sql).toMatch(
        /LEAST\(o\.id, n\.id\)[\s\S]*GREATEST\(o\.id, n\.id\)/,
      );
      expect(params).toEqual([
        ABSENT_UPSTREAM_REASON,
        REISSUE_CANDIDATE_METERS,
      ]);
    });

    it("maps both sides and flags a name match", async () => {
      const query = jest.fn().mockResolvedValue([row()]);
      const [candidate] = await build(query).findReissueCandidates();

      expect(candidate).toEqual({
        parkId: "p1",
        parkName: "Six Flags Great America",
        previous: {
          attractionId: "old",
          name: "HAUNTED HOUSE: SAW: Legacy of Terror",
          slug: "haunted-house-saw-legacy-of-terror",
          externalId: "wiki-old",
          createdAt: "2025-12-24T00:00:00.000Z",
          lastReading: "2025-11-01",
        },
        current: {
          attractionId: "new",
          name: "SAW Legacy of Terror",
          slug: "saw-legacy-of-terror",
          externalId: "wiki-new",
          createdAt: "2026-09-17T00:00:00.000Z",
          lastReading: "2026-10-03",
        },
        meters: 25,
        namesMatch: true,
      });
    });

    it("keeps a pair whose names say nothing, and sorts name matches first", async () => {
      const query = jest.fn().mockResolvedValue([
        row({
          park_name: "Movie Park Germany",
          o_name: "Hell House",
          n_name: "Helhuis",
          meters: "9.3",
        }),
        row(),
      ]);
      const candidates = await build(query).findReissueCandidates();

      expect(candidates.map((c) => [c.previous.name, c.namesMatch])).toEqual([
        ["HAUNTED HOUSE: SAW: Legacy of Terror", true],
        ["Hell House", false],
      ]);
    });
  });

  describe("findScheduledButSilentParks", () => {
    // The behaviour lives in a real database; `test/e2e/scheduled-but-silent-parks`
    // seeds the pairs and reads the rows back. What is worth pinning here is the
    // shape a regular expression CAN see: the two gates that make the query say
    // something rather than everything.
    it("passes the silence window and the lookahead through", async () => {
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findScheduledButSilentParks(45, 3);

      expect(query.mock.calls[0][1]).toEqual([45, 3]);
    });

    it("reads the park's own calendar, not a ride's", async () => {
      // `schedule_entries` holds both in one table; PAR-246 is what the same
      // blind key cost on the cleanup path.
      const query = jest.fn().mockResolvedValue([]);
      await build(query).findScheduledButSilentParks();

      const [sql] = query.mock.calls[0] as [string];
      expect(sql).toMatch(/se\."attractionId" IS NULL/);
      // Two rows for one day are possible between dedup passes.
      expect(sql).toMatch(/count\(DISTINCT se\.date\)/);
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
          days_ahead: "7",
          last_day: "2027-08-31",
        },
      ]);

      expect(await build(query).findScheduledButSilentParks()).toEqual([
        {
          parkId: "p9",
          parkName: "La Ronde",
          attractionCount: 38,
          lastReading: "2026-06-24",
          operatingDaysAhead: 7,
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
          days_ahead: "7",
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
