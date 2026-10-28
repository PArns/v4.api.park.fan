import { SiteAnalyticsRedisMock } from "../../test/mocks/site-analytics-redis.mock";
import {
  SiteAnalyticsStorageService,
  eventField,
  pageviewField,
  readSiteAnalyticsBacklog,
  rowsOfFields,
  type EventDims,
  type PageviewDims,
} from "./site-analytics-storage.service";
import { ROLLUP_TABLES } from "./site-analytics.rollup";
import {
  CLAIM_FIELD,
  DROP_REASONS,
  MAX_FLUSH_ATTEMPTS,
  SITE_ANALYTICS_KEYS as KEYS,
} from "./site-analytics.constants";

const NOW = new Date("2026-10-28T10:06:10Z");
const PAGE: PageviewDims = [
  "de",
  "/de",
  "home",
  "mobile",
  "safari",
  "ios",
  "de",
  "en",
  "",
  "",
  "",
  "",
];
const VISIT: PageviewDims = [
  ...PAGE.slice(0, 8),
  "/de",
  "google",
  "search",
  "",
] as PageviewDims;
const EVENT: EventDims = [
  "search_opened",
  "de",
  "/de",
  "home",
  "mobile",
  "safari",
  "ios",
  "de",
  "surface=hero",
  "",
];

/**
 * The Postgres side. What is pinned: a window is claimed only a minute after
 * it ended, renamed and given a claim before it is read in steps, written
 * under its hour together with the claim's marker in one transaction and
 * deleted after; a claim already marked is not written twice; a claim a
 * crashed run left is picked up; counts that reach a window after its claim
 * are flushed by the next run, not orphaned; a window that fails does not
 * stop the others and is set aside after a number of tries; one instance
 * flushes at a time; the snapshot never writes a lost day as zero; a chunk
 * drop that cannot get its lock waits for the next night.
 */
describe("SiteAnalyticsStorageService", () => {
  let redis: SiteAnalyticsRedisMock;
  let statements: Array<[string, unknown[] | undefined]>;
  let marked: boolean;
  let failOn: string | null;
  let service: SiteAnalyticsStorageService;

  const manager = {
    query: jest.fn(
      async (sql: string, params?: unknown[]): Promise<unknown[]> => {
        statements.push([sql, params]);
        if (failOn && params?.[0] === failOn) throw new Error("bad window");
        if (sql.includes("INSERT INTO site_analytics_flushes"))
          return marked ? [] : [{ bucket: params?.[0] }];
        if (sql.includes("drop_chunks")) return [{ drop_chunks: "c1" }];
        return [];
      },
    ),
  };
  const dataSource = {
    transaction: jest.fn(async (work: (m: typeof manager) => unknown) =>
      work(manager),
    ),
    query: jest.fn(async (sql: string, params?: unknown[]) =>
      manager.query(sql, params),
    ),
  };

  const buffer = (window: string, fields: Record<string, number>) => {
    redis.hashes.set(
      KEYS.buffer(window),
      new Map(Object.entries(fields).map(([k, v]) => [k, String(v)])),
    );
    redis.sets.set(
      KEYS.windows,
      new Set([...(redis.sets.get(KEYS.windows) ?? []), window]),
    );
  };

  beforeEach(() => {
    redis = new SiteAnalyticsRedisMock();
    statements = [];
    marked = false;
    failOn = null;
    manager.query.mockClear();
    dataSource.transaction.mockClear();
    service = new SiteAnalyticsStorageService(
      dataSource as never,
      redis as never,
    );
  });

  describe("flush", () => {
    it("writes the windows that ended a minute ago under their hour, with a marker, and forgets them", async () => {
      buffer("2026-10-28T09:55:00.000Z", {
        [pageviewField("v", PAGE)]: 3,
        [pageviewField("s", VISIT)]: 2,
        [eventField(EVENT)]: 4,
      });
      buffer("2026-10-28T10:00:00.000Z", { [pageviewField("v", PAGE)]: 1 });
      // The window that is still open.
      buffer("2026-10-28T10:05:00.000Z", { [pageviewField("v", PAGE)]: 1 });

      await expect(service.flush(NOW)).resolves.toEqual({
        windows: 2,
        pageviewRows: 3,
        eventRows: 1,
        failed: 0,
        visitsClosed: 0,
      });
      expect(await redis.smembers(KEYS.windows)).toEqual([
        "2026-10-28T10:05:00.000Z",
      ]);
      for (const window of [
        "2026-10-28T09:55:00.000Z",
        "2026-10-28T10:00:00.000Z",
      ]) {
        expect(redis.hashes.has(KEYS.buffer(window))).toBe(false);
        expect(redis.hashes.has(KEYS.flushing(window))).toBe(false);
      }
      expect(redis.hashes.has(KEYS.buffer("2026-10-28T10:05:00.000Z"))).toBe(
        true,
      );

      const [marker, pageviews, events] = statements;
      expect(marker[0]).toContain("ON CONFLICT (bucket, claim) DO NOTHING");
      expect(marker[1]?.[0]).toBe("2026-10-28T09:55:00.000Z");
      expect(marker[1]?.[1]).toMatch(/^[0-9a-f-]{36}$/);
      expect(marker[1]?.slice(2)).toEqual([2, 1]);
      expect(pageviews[0]).toMatch(/^INSERT INTO site_pageviews/);
      // The rows carry the hour, not the five-minute window.
      expect(pageviews[1]?.[0]).toBe("2026-10-28T09:00:00.000Z");
      // views, seconds, visits, engaged, visit_seconds, exits, visit_span_seconds
      expect(pageviews[1]?.slice(-7)).toEqual([
        [3, 0],
        [0, 0],
        [0, 2],
        [0, 0],
        [0, 0],
        [0, 0],
        [0, 0],
      ]);
      expect(events[0]).toMatch(/^INSERT INTO site_events/);
      expect(events[0]).not.toContain("referrer");
      expect(events[1]?.[0]).toBe("2026-10-28T09:00:00.000Z");
      expect(events[1]?.[10]).toEqual([JSON.stringify({ surface: "hero" })]);
      expect(events[1]?.slice(-1)).toEqual([[4]]);
      expect(statements[4][1]?.[0]).toBe("2026-10-28T10:00:00.000Z");
      expect(dataSource.transaction).toHaveBeenCalledTimes(2);
    });

    it("reads a window in HSCAN steps, every field once", async () => {
      const fields = Object.fromEntries(
        Array.from({ length: 7 }, (_, i) => [
          pageviewField("v", [
            "de",
            `/de/p${i}`,
            ...PAGE.slice(2),
          ] as PageviewDims),
          i + 1,
        ]),
      );
      buffer("2026-10-28T09:55:00.000Z", fields);
      await service.flush(NOW);
      expect(statements[1][1]?.[13]).toEqual([1, 2, 3, 4, 5, 6, 7]);
    });

    it("writes a claim with a marker no second time", async () => {
      marked = true;
      buffer("2026-10-28T09:55:00.000Z", { [pageviewField("v", PAGE)]: 3 });
      await expect(service.flush(NOW)).resolves.toMatchObject({
        windows: 1,
        pageviewRows: 0,
      });
      expect(statements).toHaveLength(1);
      expect(await redis.smembers(KEYS.windows)).toEqual([]);
    });

    it("picks up the claim a crashed run left, under the same claim", async () => {
      redis.hashes.set(
        KEYS.flushing("2026-10-28T09:50:00.000Z"),
        new Map([
          [pageviewField("v", PAGE), "5"],
          [CLAIM_FIELD, "0b3c6a9e-1111-4222-8333-444455556666"],
        ]),
      );
      redis.sets.set(KEYS.windows, new Set(["2026-10-28T09:50:00.000Z"]));
      await expect(service.flush(NOW)).resolves.toMatchObject({
        windows: 1,
        pageviewRows: 1,
      });
      expect(statements[0][1]?.[1]).toBe(
        "0b3c6a9e-1111-4222-8333-444455556666",
      );
      expect(statements[1][1]?.slice(-7)).toEqual([
        [5],
        [0],
        [0],
        [0],
        [0],
        [0],
        [0],
      ]);
    });

    it("flushes counts that reached a window after its claim on the next run, under a new claim", async () => {
      const window = "2026-10-28T09:55:00.000Z";
      buffer(window, { [pageviewField("v", PAGE)]: 3 });
      // A slow request adds to the window between the claim and its release.
      redis.onCommand = (name) => {
        if (name === "hscan") {
          redis.onCommand = null;
          void redis.hincrby(KEYS.buffer(window), pageviewField("v", PAGE), 2);
          void redis.sadd(KEYS.windows, window);
        }
      };
      await service.flush(NOW);
      expect(await redis.smembers(KEYS.windows)).toEqual([window]);
      await service.flush(NOW);
      const markers = statements.filter(([sql]) =>
        sql.includes("site_analytics_flushes"),
      );
      expect(markers).toHaveLength(2);
      expect(markers[0][1]?.[1]).not.toBe(markers[1][1]?.[1]);
      const views = statements
        .filter(([sql]) => sql.includes("INSERT INTO site_pageviews"))
        .map(([, params]) => params?.[13]);
      expect(views).toEqual([[3], [2]]);
      expect(await redis.smembers(KEYS.windows)).toEqual([]);
    });

    it("goes on past a window that fails, and sets it aside after the last try", async () => {
      buffer("2026-10-28T09:50:00.000Z", { [pageviewField("v", PAGE)]: 1 });
      buffer("2026-10-28T09:55:00.000Z", { [pageviewField("v", PAGE)]: 2 });
      failOn = "2026-10-28T09:50:00.000Z";
      await expect(service.flush(NOW)).resolves.toMatchObject({
        windows: 1,
        failed: 1,
      });
      // The failed window waits, claimed, for the next run.
      expect(await redis.smembers(KEYS.windows)).toEqual([
        "2026-10-28T09:50:00.000Z",
      ]);
      expect(redis.hashes.has(KEYS.flushing("2026-10-28T09:50:00.000Z"))).toBe(
        true,
      );
      for (let i = 1; i < MAX_FLUSH_ATTEMPTS; i++) await service.flush(NOW);
      expect(await redis.smembers(KEYS.windows)).toEqual([]);
      expect(await redis.smembers(KEYS.parkedWindows)).toEqual([
        "2026-10-28T09:50:00.000Z",
      ]);
      expect(redis.hashes.has(KEYS.parked("2026-10-28T09:50:00.000Z"))).toBe(
        true,
      );
      expect(redis.ttls.get(KEYS.parked("2026-10-28T09:50:00.000Z"))).toBe(
        7 * 24 * 3600,
      );
    });

    it("leaves the flush to the instance that holds the lock", async () => {
      await redis.set(KEYS.flushLock, "someone-else");
      buffer("2026-10-28T09:55:00.000Z", { [pageviewField("v", PAGE)]: 3 });
      await expect(service.flush(NOW)).resolves.toMatchObject({
        skipped: "locked",
      });
      expect(statements).toEqual([]);
    });

    it("releases its own lock and only its own", async () => {
      await service.flush(NOW);
      expect(await redis.get(KEYS.flushLock)).toBeNull();
    });
  });

  describe("closeIdleVisits", () => {
    const seconds = (iso: string) => Math.floor(Date.parse(iso) / 1000);
    const EXIT: PageviewDims = [
      "de",
      "/de/news",
      "news_index",
      ...PAGE.slice(3, 8),
      "/de",
      "google",
      "search",
      "",
    ] as PageviewDims;
    const openVisit = (member: string, first: string, last: string) => {
      redis.hashes.set(
        KEYS.visit(member),
        new Map([
          ["c", "{}"],
          ["n", "2"],
          ["s", String(seconds(first))],
          ["l", String(seconds(last))],
          ["x", JSON.stringify(EXIT)],
        ]),
      );
      const open = redis.zsets.get(KEYS.openVisits) ?? new Map();
      open.set(member, seconds(last));
      redis.zsets.set(KEYS.openVisits, open);
    };

    it("ends a visit idle for 30 minutes: one exit and its span in the window of its last hit, then its key goes", async () => {
      openVisit("idle", "2026-10-28T09:20:00Z", "2026-10-28T09:31:10Z");
      openVisit("active", "2026-10-28T09:50:00Z", "2026-10-28T09:58:00Z");
      await expect(service.closeIdleVisits(NOW)).resolves.toBe(1);
      expect(
        Object.fromEntries(
          redis.hashes.get(KEYS.buffer("2026-10-28T09:30:00.000Z")) ?? [],
        ),
      ).toEqual({
        [pageviewField("x", EXIT)]: "1",
        [pageviewField("y", EXIT)]: String(11 * 60 + 10),
      });
      expect(await redis.smembers(KEYS.windows)).toEqual([
        "2026-10-28T09:30:00.000Z",
      ]);
      expect(redis.hashes.has(KEYS.visit("idle"))).toBe(false);
      expect([...(redis.zsets.get(KEYS.openVisits)?.keys() ?? [])]).toEqual([
        "active",
      ]);
      expect(redis.hashes.has(KEYS.visit("active"))).toBe(true);
    });

    it("leaves a visit that had a hit between the read and the close, and writes the exit with the window", async () => {
      openVisit("busy", "2026-10-28T09:20:00Z", "2026-10-28T09:31:10Z");
      const pipeline = redis.pipeline.bind(redis);
      let runs = 0;
      redis.pipeline = () => {
        const chain = pipeline();
        const exec = chain.exec;
        chain.exec = async () => {
          const results = await exec();
          // The reader comes back right after the visit was read.
          if (runs++ === 0)
            redis.hashes
              .get(KEYS.visit("busy"))
              ?.set("l", String(seconds("2026-10-28T10:06:00Z")));
          return results;
        };
        return chain;
      };
      await expect(service.closeIdleVisits(NOW)).resolves.toBe(0);
      expect(redis.hashes.has(KEYS.visit("busy"))).toBe(true);
      expect(redis.hashes.has(KEYS.buffer("2026-10-28T09:30:00.000Z"))).toBe(
        false,
      );
    });

    it("counts a visit whose key is gone as a lost exit, and forgets it", async () => {
      redis.zsets.set(
        KEYS.openVisits,
        new Map([["gone", seconds("2026-10-28T09:00:00Z")]]),
      );
      await expect(service.closeIdleVisits(NOW)).resolves.toBe(0);
      expect(redis.zsets.has(KEYS.openVisits)).toBe(false);
      expect(
        redis.hashes.get(KEYS.drops("2026-10-28"))?.get(DROP_REASONS.exit),
      ).toBe("1");
    });

    it("runs first in every flush, so the exits it adds are written in the same run", async () => {
      openVisit("idle", "2026-10-28T09:20:00Z", "2026-10-28T09:31:10Z");
      await expect(service.flush(NOW)).resolves.toMatchObject({
        windows: 1,
        pageviewRows: 1,
        visitsClosed: 1,
      });
      const [, pageviews] = statements;
      expect(pageviews[1]?.[0]).toBe("2026-10-28T09:00:00.000Z");
      // views, seconds, visits, engaged, visit_seconds, exits, visit_span_seconds
      expect(pageviews[1]?.slice(-7)).toEqual([
        [0],
        [0],
        [0],
        [0],
        [0],
        [1],
        [670],
      ]);
    });
  });

  describe("writeDirect", () => {
    it("writes held counts under their hour, without a marker", async () => {
      await service.writeDirect(
        "2026-10-28T10:05:00.000Z",
        new Map([[pageviewField("v", PAGE), 4]]),
      );
      expect(statements).toHaveLength(1);
      expect(statements[0][0]).toMatch(/^INSERT INTO site_pageviews/);
      expect(statements[0][1]?.[0]).toBe("2026-10-28T10:00:00.000Z");
    });
  });

  describe("rowsOfFields", () => {
    it("sums a combination's measures into one row and skips what it cannot read", () => {
      const rows = rowsOfFields({
        [pageviewField("v", VISIT)]: "2",
        [pageviewField("s", VISIT)]: "1",
        [pageviewField("e", VISIT)]: "1",
        ["q" + JSON.stringify(VISIT)]: "9",
        ["v{not json"]: "1",
        ["v" + JSON.stringify(["too", "short"])]: "1",
        [pageviewField("v", PAGE)]: "0",
        [CLAIM_FIELD]: "0b3c6a9e-1111-4222-8333-444455556666",
        [eventField(EVENT)]: "3",
        ["n" + JSON.stringify([...EVENT, "an old field"])]: "1",
      });
      expect(rows.pageviews).toEqual([
        {
          dims: VISIT,
          counts: {
            views: 2,
            seconds: 0,
            visits: 1,
            engaged: 1,
            visitSeconds: 0,
            exits: 0,
            visitSpanSeconds: 0,
          },
        },
      ]);
      expect(rows.events).toEqual([{ dims: EVENT, count: 3 }]);
    });
  });

  describe("snapshotVisitors", () => {
    it("writes yesterday's visitors that still have a set, and each of their hours, once", async () => {
      redis.sets.set("site-analytics:uv:2026-10-27:all", new Set(["a", "b"]));
      redis.sets.set("site-analytics:uv:2026-10-27:de", new Set(["a"]));
      redis.sets.set("site-analytics:uv:2026-10-26:all", new Set(["c"]));
      redis.sets.set("site-analytics:uvh:2026102710:all", new Set(["a", "b"]));
      redis.sets.set("site-analytics:uvh:2026102710:de", new Set(["a"]));
      redis.sets.set("site-analytics:uvh:2026102711:all", new Set(["b"]));
      // An hour of the day before is not yesterday's.
      redis.sets.set("site-analytics:uvh:2026102610:all", new Set(["c"]));
      manager.query.mockImplementation(async (sql, params) => {
        statements.push([sql, params]);
        return sql.includes("INSERT INTO site_visitor")
          ? (params?.[0] as unknown[]).map(() => ({}))
          : [];
      });
      await expect(service.snapshotVisitors(NOW)).resolves.toEqual({
        found: 2,
        written: 2,
        hoursWritten: 168,
      });
      const [sql, params] = statements[0];
      expect(sql).toContain("ON CONFLICT (day, scope) DO NOTHING");
      expect(params).toEqual([
        ["2026-10-27", "2026-10-27"],
        ["all", "de"],
        [2, 1],
      ]);
      // Every hour of a counted day, with zeros, for every language.
      const [hoursSql, hours] = statements[1] as [string, unknown[][]];
      expect(hoursSql).toContain("ON CONFLICT (hour, scope) DO NOTHING");
      expect(hours[0]).toHaveLength(7 * 24);
      expect(hours[0].slice(0, 2)).toEqual([
        "2026-10-27T00:00:00.000Z",
        "2026-10-27T01:00:00.000Z",
      ]);
      expect(hours[1].filter((scope) => scope === "all")).toHaveLength(24);
      expect(hours[2].slice(0, 24)).toEqual(
        Array.from({ length: 24 }, (_, h) => (h === 10 ? 2 : h === 11 ? 1 : 0)),
      );
      expect(
        hours[2].slice(24).reduce((a, b) => (a as number) + (b as number), 0),
      ).toBe(1);
    });

    it("never writes a day whose set is gone as zero visitors, nor its hours", async () => {
      redis.sets.set("site-analytics:uvh:2026102710:all", new Set(["a"]));
      await expect(service.snapshotVisitors(NOW)).resolves.toEqual({
        found: 0,
        written: 0,
        hoursWritten: 0,
      });
      expect(statements).toEqual([]);
    });
  });

  describe("backlog", () => {
    it("reports the windows waiting, the oldest one's age, the parked ones and the day's drops", async () => {
      buffer("2026-10-28T09:40:00.000Z", { [pageviewField("v", PAGE)]: 1 });
      buffer("2026-10-28T10:05:00.000Z", { [pageviewField("v", PAGE)]: 1 });
      redis.sets.set(KEYS.parkedWindows, new Set(["2026-10-27T03:00:00.000Z"]));
      redis.hashes.set(
        KEYS.drops("2026-10-28"),
        new Map([[DROP_REASONS.window, "12"]]),
      );
      redis.zsets.set(
        KEYS.openVisits,
        new Map([
          ["a", 1],
          ["b", 2],
        ]),
      );
      const query = jest
        .fn()
        .mockResolvedValue([{ last: new Date("2026-10-28T10:01:02Z") }]);
      await expect(
        readSiteAnalyticsBacklog(redis as never, { query } as never, NOW),
      ).resolves.toEqual({
        pendingWindows: 2,
        readyWindows: 1,
        oldestPendingMinutes: 26,
        parkedWindows: 1,
        openVisits: 2,
        lastFlushAt: "2026-10-28T10:01:02.000Z",
        dropsToday: { [DROP_REASONS.window]: 12 },
        dropsYesterday: {},
        healthy: false,
      });
    });

    it("is healthy when only the current and the last window wait", async () => {
      buffer("2026-10-28T10:00:00.000Z", { [pageviewField("v", PAGE)]: 1 });
      buffer("2026-10-28T10:05:00.000Z", { [pageviewField("v", PAGE)]: 1 });
      const query = jest.fn().mockResolvedValue([{ last: null }]);
      await expect(
        readSiteAnalyticsBacklog(redis as never, { query } as never, NOW),
      ).resolves.toMatchObject({ readyWindows: 1, healthy: true });
    });
  });

  describe("rollupDays", () => {
    const TOTALS = {
      views: "10",
      seconds: "0",
      visits: "0",
      engaged: "0",
      visit_seconds: "0",
      exits: "0",
      visit_span_seconds: "0",
      count: "10",
    };
    const ALL_PAGEVIEW_CUBOIDS = ROLLUP_TABLES.pageviews.cuboids.length;
    const ALL_EVENT_CUBOIDS = ROLLUP_TABLES.events.cuboids.length;
    let oldest: string | null;
    let markers: Array<{ source: string; cuboid: number; day: string }>;
    let rolledBefore: Set<string>;
    let drift: number | null;

    const markersOf = (day: string, except: Array<[string, number]> = []) =>
      (["pageviews", "events"] as const).flatMap((source) =>
        ROLLUP_TABLES[source].cuboids
          .filter((c) => !except.some(([s, id]) => s === source && id === c.id))
          .map((c) => ({ source, cuboid: c.id, day })),
      );
    const rolledInserts = () =>
      statements.filter(([sql]) =>
        sql.includes("INSERT INTO site_rollup_days"),
      );

    beforeEach(() => {
      oldest = "2026-10-26";
      markers = [];
      rolledBefore = new Set();
      drift = null;
      dataSource.query.mockImplementation(async (sql: string) => {
        if (sql.includes("LEAST(")) return oldest ? [{ day: oldest }] : [{}];
        if (sql.includes("FROM site_rollup_days")) return markers;
        return [];
      });
      manager.query.mockImplementation(
        async (sql: string, params?: unknown[]) => {
          statements.push([sql, params]);
          if (sql.includes("INSERT INTO site_rollup_days")) {
            const key = `${params?.[1]}:${params?.[2]}`;
            return rolledBefore.has(key) ? [] : [{ rows: 2 }];
          }
          if (sql.includes("GROUP BY cuboid")) {
            return (params?.[1] as number[]).map((cuboid, i) => ({
              cuboid: String(cuboid),
              ...TOTALS,
              ...(drift !== null && i === 0
                ? { views: String(10 + drift) }
                : {}),
            }));
          }
          if (sql.includes("COALESCE(SUM(")) return [TOTALS];
          return [];
        },
      );
    });

    it("rolls the settled days oldest first, each cuboid without a marker, a day in one transaction", async () => {
      markers = markersOf("2026-10-26");
      await expect(
        service.rollupDays(new Date("2026-10-28T03:00:00Z")),
      ).resolves.toEqual({
        days: 1,
        cuboids: ALL_PAGEVIEW_CUBOIDS + ALL_EVENT_CUBOIDS,
        rows: 2 * (ALL_PAGEVIEW_CUBOIDS + ALL_EVENT_CUBOIDS),
      });
      expect(dataSource.transaction).toHaveBeenCalledTimes(1);
      expect(statements[0][0]).toContain("pg_advisory_xact_lock");
      const inserts = rolledInserts();
      expect(inserts).toHaveLength(ALL_PAGEVIEW_CUBOIDS + ALL_EVENT_CUBOIDS);
      expect(inserts[0][1]).toEqual([
        "2026-10-27T00:00:00.000Z",
        "2026-10-27",
        1,
      ]);
      expect(inserts[ALL_PAGEVIEW_CUBOIDS][0]).toContain("source = 'events'");
      // Each source's cuboids are checked against the day's hourly rows once.
      expect(
        statements.filter(([sql]) => sql.includes("GROUP BY cuboid")),
      ).toHaveLength(2);
      // The lock is let go.
      expect(await redis.get(KEYS.rollupLock)).toBeNull();
    });

    it("rolls more than one day, and only the cuboids a day lacks", async () => {
      oldest = "2026-10-25";
      markers = [
        ...markersOf("2026-10-25"),
        ...markersOf("2026-10-26", [["pageviews", 49]]),
      ];
      await expect(
        service.rollupDays(new Date("2026-10-28T03:00:00Z")),
      ).resolves.toMatchObject({
        days: 2,
        cuboids: 1 + ALL_PAGEVIEW_CUBOIDS + ALL_EVENT_CUBOIDS,
      });
      const days = rolledInserts().map(([, params]) => params?.[1]);
      expect(days[0]).toBe("2026-10-26");
      expect(days.filter((day) => day === "2026-10-26")).toHaveLength(1);
      expect(days[days.length - 1]).toBe("2026-10-27");
    });

    it("counts only the cuboids it wrote, not one another instance wrote first", async () => {
      rolledBefore.add("2026-10-27:49");
      oldest = "2026-10-27";
      const rolled = await service.rollupDays(new Date("2026-10-28T03:00:00Z"));
      expect(rolled.cuboids).toBe(ALL_PAGEVIEW_CUBOIDS + ALL_EVENT_CUBOIDS - 1);
    });

    it("leaves a day alone until two hours past midnight", async () => {
      oldest = "2026-10-27";
      await expect(
        service.rollupDays(new Date("2026-10-28T01:59:00Z")),
      ).resolves.toEqual({ days: 0, cuboids: 0, rows: 0 });
      expect(dataSource.transaction).not.toHaveBeenCalled();
      await expect(
        service.rollupDays(new Date("2026-10-28T02:00:00Z")),
      ).resolves.toMatchObject({ days: 1 });
    });

    it("waits for a flush window of the day that is still in Redis", async () => {
      oldest = "2026-10-26";
      buffer("2026-10-27T23:55:00.000Z", { [pageviewField("v", PAGE)]: 1 });
      await expect(
        service.rollupDays(new Date("2026-10-28T03:00:00Z")),
      ).resolves.toMatchObject({ days: 1 });
      expect(rolledInserts()[0][1]?.[1]).toBe("2026-10-26");
      expect(
        rolledInserts().some(([, params]) => params?.[1] === "2026-10-27"),
      ).toBe(false);
    });

    it("does nothing without hourly rows, and never looks further back than the retention", async () => {
      oldest = null;
      await expect(
        service.rollupDays(new Date("2026-10-28T03:00:00Z")),
      ).resolves.toEqual({ days: 0, cuboids: 0, rows: 0 });
      oldest = "2020-01-01";
      markers = [];
      await service.rollupDays(new Date("2026-10-28T03:00:00Z"));
      const first = rolledInserts()[0][1]?.[1] as string;
      expect(first).toBe(
        new Date(Date.parse("2026-10-28T00:00:00Z") - 400 * 86_400_000)
          .toISOString()
          .slice(0, 10),
      );
    });

    it("rolls a day back and says why when a cuboid does not add up to the hourly rows", async () => {
      drift = 1;
      await expect(
        service.rollupDays(new Date("2026-10-28T03:00:00Z")),
      ).rejects.toThrow(
        /pageviews cuboid 1 of 2026-10-26 adds up to 11 views, the hourly rows to 10/,
      );
      expect(await redis.get(KEYS.rollupLock)).toBeNull();
    });

    it("runs on one instance at a time", async () => {
      await redis.set(KEYS.rollupLock, "someone else", "PX", 60_000, "NX");
      await expect(
        service.rollupDays(new Date("2026-10-28T03:00:00Z")),
      ).resolves.toEqual({ days: 0, cuboids: 0, rows: 0, skipped: "locked" });
      expect(dataSource.transaction).not.toHaveBeenCalled();
      expect(await redis.get(KEYS.rollupLock)).toBe("someone else");
    });
  });

  describe("dropOldChunks", () => {
    it("drops whole chunks under a lock timeout, and leaves a busy one for tomorrow", async () => {
      manager.query.mockImplementation(async (sql: string, params) => {
        statements.push([sql, params]);
        if (sql.includes("drop_chunks") && params?.[0] === "site_events")
          throw new Error("canceling statement due to lock timeout");
        return sql.includes("drop_chunks") ? [{ drop_chunks: "c" }] : [];
      });
      await expect(service.dropOldChunks()).resolves.toEqual({
        site_pageviews: 1,
        site_events: 0,
        site_analytics_flushes: 1,
        site_pageviews_daily: 1,
        site_events_daily: 1,
      });
      expect(statements[0][0]).toContain(
        "set_config('lock_timeout', '5s', true)",
      );
      expect(statements[1][1]).toEqual(["site_pageviews", "400 days"]);
    });
  });
});
