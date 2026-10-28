import { SiteAnalyticsRedisMock } from "../../test/mocks/site-analytics-redis.mock";
import {
  SiteAnalyticsStorageService,
  eventField,
  pageviewField,
  rowsOfFields,
  type PageviewDims,
} from "./site-analytics-storage.service";
import { SITE_ANALYTICS_KEYS as KEYS } from "./site-analytics.constants";

const NOW = new Date("2026-10-28T10:06:10Z");
const DIMS: PageviewDims = [
  "de",
  "/de",
  "home",
  "mobile",
  "safari",
  "ios",
  "de",
  "/de",
  "google",
  "search",
  "",
];

/**
 * The Postgres side. What is pinned: a window is claimed only a minute after
 * it ended, renamed before it is read, written together with its marker in
 * one transaction and deleted after; a window already marked is not written
 * twice; a claim a crashed run left is picked up; one instance flushes at a
 * time; the snapshot never writes a lost day as zero; a chunk drop that
 * cannot get its lock waits for the next night.
 */
describe("SiteAnalyticsStorageService", () => {
  let redis: SiteAnalyticsRedisMock;
  let statements: Array<[string, unknown[] | undefined]>;
  let marked: boolean;
  let service: SiteAnalyticsStorageService;

  const manager = {
    query: jest.fn(
      async (sql: string, params?: unknown[]): Promise<unknown[]> => {
        statements.push([sql, params]);
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
    manager.query.mockClear();
    dataSource.transaction.mockClear();
    service = new SiteAnalyticsStorageService(
      dataSource as never,
      redis as never,
    );
  });

  describe("flush", () => {
    it("writes the windows that ended a minute ago, with their marker, and forgets them", async () => {
      buffer("2026-10-28T09:55:00.000Z", {
        [pageviewField("v", DIMS)]: 3,
        [pageviewField("s", DIMS)]: 2,
        [eventField(["search_opened", ...DIMS, "surface=hero", ""] as never)]:
          4,
      });
      buffer("2026-10-28T10:00:00.000Z", { [pageviewField("v", DIMS)]: 1 });
      // The window that is still open.
      buffer("2026-10-28T10:05:00.000Z", { [pageviewField("v", DIMS)]: 1 });

      await expect(service.flush(NOW)).resolves.toEqual({
        windows: 2,
        pageviewRows: 2,
        eventRows: 1,
      });
      expect(await redis.smembers(KEYS.windows)).toEqual([
        "2026-10-28T10:05:00.000Z",
      ]);
      expect(redis.hashes.has(KEYS.buffer("2026-10-28T09:55:00.000Z"))).toBe(
        false,
      );
      expect(redis.hashes.has(KEYS.flushing("2026-10-28T09:55:00.000Z"))).toBe(
        false,
      );
      expect(redis.hashes.has(KEYS.buffer("2026-10-28T10:05:00.000Z"))).toBe(
        true,
      );

      const [marker, pageviews, events] = statements;
      expect(marker[0]).toContain("ON CONFLICT (bucket) DO NOTHING");
      expect(marker[1]).toEqual(["2026-10-28T09:55:00.000Z", 1, 1]);
      expect(pageviews[0]).toMatch(/^INSERT INTO site_pageviews/);
      expect(pageviews[1]?.slice(-3)).toEqual([[3], [2], [0]]);
      expect(events[0]).toMatch(/^INSERT INTO site_events/);
      expect(events[1]?.[14]).toEqual([JSON.stringify({ surface: "hero" })]);
      expect(events[1]?.slice(-1)).toEqual([[4]]);
      expect(dataSource.transaction).toHaveBeenCalledTimes(2);
    });

    it("writes a window with a marker no second time", async () => {
      marked = true;
      buffer("2026-10-28T09:55:00.000Z", { [pageviewField("v", DIMS)]: 3 });
      await expect(service.flush(NOW)).resolves.toMatchObject({
        windows: 1,
        pageviewRows: 0,
      });
      expect(statements).toHaveLength(1);
      expect(await redis.smembers(KEYS.windows)).toEqual([]);
    });

    it("picks up the claim a crashed run left", async () => {
      redis.hashes.set(
        KEYS.flushing("2026-10-28T09:50:00.000Z"),
        new Map([[pageviewField("v", DIMS), "5"]]),
      );
      redis.sets.set(KEYS.windows, new Set(["2026-10-28T09:50:00.000Z"]));
      await expect(service.flush(NOW)).resolves.toMatchObject({
        windows: 1,
        pageviewRows: 1,
      });
      expect(statements[1][1]?.slice(-3)).toEqual([[5], [0], [0]]);
    });

    it("leaves the flush to the instance that holds the lock", async () => {
      await redis.set(KEYS.flushLock, "someone-else");
      buffer("2026-10-28T09:55:00.000Z", { [pageviewField("v", DIMS)]: 3 });
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

  describe("rowsOfFields", () => {
    it("sums a combination's measures into one row and skips what it cannot read", () => {
      const rows = rowsOfFields({
        [pageviewField("v", DIMS)]: "2",
        [pageviewField("s", DIMS)]: "1",
        [pageviewField("e", DIMS)]: "1",
        ["x" + JSON.stringify(DIMS)]: "9",
        ["v{not json"]: "1",
        ["v" + JSON.stringify(["too", "short"])]: "1",
        [pageviewField("v", [...DIMS.slice(0, 10), "camp"] as PageviewDims)]:
          "0",
      });
      expect(rows.pageviews).toEqual([
        { dims: DIMS, views: 2, visits: 1, engaged: 1 },
      ]);
      expect(rows.events).toEqual([]);
    });
  });

  describe("snapshotVisitors", () => {
    it("writes the last two finished days that still have a set, once", async () => {
      redis.sets.set("site-analytics:uv:2026-10-27:all", new Set(["a", "b"]));
      redis.sets.set("site-analytics:uv:2026-10-27:de", new Set(["a"]));
      redis.sets.set("site-analytics:uv:2026-10-26:all", new Set(["c"]));
      manager.query.mockImplementationOnce(async (sql, params) => {
        statements.push([sql, params]);
        return [{}, {}];
      });
      await expect(service.snapshotVisitors(NOW)).resolves.toEqual({
        found: 3,
        written: 2,
      });
      const [sql, params] = statements[0];
      expect(sql).toContain("ON CONFLICT (day, scope) DO NOTHING");
      expect(params).toEqual([
        ["2026-10-27", "2026-10-27", "2026-10-26"],
        ["all", "de", "all"],
        [2, 1, 1],
      ]);
    });

    it("never writes a day whose set is gone as zero visitors", async () => {
      await expect(service.snapshotVisitors(NOW)).resolves.toEqual({
        found: 0,
        written: 0,
      });
      expect(statements).toEqual([]);
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
      });
      expect(statements[0][0]).toContain(
        "set_config('lock_timeout', '5s', true)",
      );
      expect(statements[1][1]).toEqual(["site_pageviews", "400 days"]);
    });
  });
});
