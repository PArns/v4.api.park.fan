import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { GeoipService } from "../geoip/geoip.service";
import { SiteTrafficDay } from "./entities/site-traffic-day.entity";
import {
  SITE_ANALYTICS_STORAGE_OPTIONS,
  SiteAnalyticsCollectService,
  buildCollectRows,
} from "./site-analytics-collect.service";
import {
  MAX_HITS_PER_ADDRESS_PER_DAY,
  VISIT_IDLE_SECONDS,
} from "./site-analytics.constants";
import { validateHit, type ValidHit } from "./site-analytics.hits";

const BROWSER =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1";
const DESKTOP =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const IP = "203.0.113.7";
const NOW = new Date("2026-10-28T21:15:30Z");
const PARK_ID = "3f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
const PATH = "/de/parks/europe/germany/bruehl/phantasialand";

const pageview = (overrides: Record<string, unknown> = {}) => ({
  t: "pageview",
  locale: "de",
  path: PATH,
  pageType: "park",
  viewport: "mobile",
  ...overrides,
});
const tabChanged = {
  t: "event",
  locale: "de",
  path: PATH,
  pageType: "park",
  viewport: "mobile",
  name: "tab_changed",
  props: { tab: "map" },
  entity: PARK_ID,
};

/**
 * An in-memory Redis with the commands the collector pipelines: counters,
 * hashes, HyperLogLogs as sets, and the TTL each key was given.
 */
class FakeRedis {
  counters = new Map<string, number>();
  hashes = new Map<string, Set<string>>();
  sets = new Map<string, Set<string>>();
  ttls = new Map<string, number>();
  commands: Array<[string, ...unknown[]]> = [];
  down = false;

  pipeline() {
    const queued: Array<() => unknown> = [];
    const log = (...command: [string, ...unknown[]]) =>
      this.commands.push(command);
    const chain = {
      incrby: (key: string, by: number) => {
        log("incrby", key, by);
        queued.push(() => {
          this.counters.set(key, (this.counters.get(key) ?? 0) + by);
          return this.counters.get(key);
        });
        return chain;
      },
      expire: (key: string, ttl: number, mode?: string) => {
        log("expire", key, ttl, mode);
        queued.push(() => {
          const exists =
            this.counters.has(key) ||
            this.hashes.has(key) ||
            this.sets.has(key);
          if (!exists || (mode === "NX" && this.ttls.has(key))) return 0;
          this.ttls.set(key, ttl);
          return 1;
        });
        return chain;
      },
      hsetnx: (key: string, field: string) => {
        log("hsetnx", key, field);
        queued.push(() => {
          const hash = this.hashes.get(key) ?? new Set<string>();
          this.hashes.set(key, hash);
          if (hash.has(field)) return 0;
          hash.add(field);
          return 1;
        });
        return chain;
      },
      pfadd: (key: string, element: string) => {
        log("pfadd", key, element);
        queued.push(() => {
          const set = this.sets.get(key) ?? new Set<string>();
          this.sets.set(key, set);
          const had = set.has(element);
          set.add(element);
          return had ? 0 : 1;
        });
        return chain;
      },
      exists: (key: string) => {
        queued.push(() => (this.sets.has(key) ? 1 : 0));
        return chain;
      },
      pfcount: (key: string) => {
        queued.push(() => this.sets.get(key)?.size ?? 0);
        return chain;
      },
      exec: async () => {
        if (this.down) throw new Error("Redis down");
        return queued.map((run) => [null, run()]);
      },
    };
    return chain;
  }

  /** Ends every visit, as 30 idle minutes would. */
  expireVisits() {
    for (const key of [...this.hashes.keys()]) {
      if (key.startsWith("site-analytics:visit:")) this.hashes.delete(key);
    }
  }
}

/**
 * The write side. What is pinned: a bot, a refused hit or a hit past the
 * address's budget never reaches the table; a visit opens on a page view
 * without a live visit key and counts entry and referrer once; every
 * dimension counts its visit once; no address or User-Agent reaches a key, a
 * field or a row; Redis down still counts page views; one statement per
 * request; the nightly snapshot never writes a lost key as zero.
 */
describe("SiteAnalyticsCollectService", () => {
  let service: SiteAnalyticsCollectService;
  let query: jest.Mock;
  let redis: FakeRedis;
  let lookupCountry: jest.Mock;

  const build = async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [
        SiteAnalyticsCollectService,
        { provide: getRepositoryToken(SiteTrafficDay), useValue: { query } },
        { provide: REDIS_CLIENT, useValue: redis },
        { provide: GeoipService, useValue: { lookupCountry } },
      ],
    }).compile();
    return moduleRef.get(SiteAnalyticsCollectService);
  };

  /** The traffic rows of the n-th write as `dim:value → [views, visits]`. */
  const trafficOf = (call = 0) => {
    const params = query.mock.calls[call][1] as unknown[][];
    const [dims, , values, views, visits] = params.slice(1, 6);
    return Object.fromEntries(
      dims.map((dim, i) => [`${dim}:${values[i]}`, [views[i], visits[i]]]),
    );
  };

  const collect = (
    hits: unknown[],
    overrides: { ip?: string | null; userAgent?: string | null } = {},
    now = NOW,
  ) => service.collect({ hits, ip: IP, userAgent: BROWSER, ...overrides }, now);

  beforeEach(async () => {
    process.env.ANALYTICS_HASH_SECRET = "test-secret";
    query = jest.fn().mockResolvedValue([]);
    redis = new FakeRedis();
    lookupCountry = jest.fn().mockReturnValue("nl");
    service = await build();
  });

  afterEach(() => {
    delete process.env.ANALYTICS_HASH_SECRET;
    delete process.env.PAGE_VIEWS_HASH_SECRET;
  });

  it("writes a page view under every dimension, as a new visit with entry and referrer, in one statement", async () => {
    await expect(collect([pageview({ referrer: "google.com" })])).resolves.toBe(
      1,
    );
    expect(query).toHaveBeenCalledTimes(1);
    const [sql, params] = query.mock.calls[0];
    expect(sql).toContain("INSERT INTO site_traffic_days");
    expect(sql).toContain("INSERT INTO site_event_days");
    expect(sql).toContain("INSERT INTO site_event_entity_days");
    expect(params[0]).toBe("2026-10-28");
    expect(trafficOf()).toEqual({
      [`path:${PATH}`]: [1, 1],
      "page_type:park": [1, 1],
      "viewport:mobile": [1, 1],
      "browser:safari": [1, 1],
      "os:ios": [1, 1],
      "country:nl": [1, 1],
      "locale:de": [1, 1],
      [`entry:${PATH}`]: [1, 1],
      "referrer:google.com": [1, 1],
    });
  });

  it("counts the second page of a visit as a view, a new page as a visit of that page only", async () => {
    await collect([pageview()]);
    await collect([pageview({ referrer: "google.com" })]);
    expect(trafficOf(1)).toEqual({
      [`path:${PATH}`]: [1, 0],
      "page_type:park": [1, 0],
      "viewport:mobile": [1, 0],
      "browser:safari": [1, 0],
      "os:ios": [1, 0],
      "country:nl": [1, 0],
      "locale:de": [1, 0],
    });
    await collect([pageview({ path: "/de/news", pageType: "news_index" })]);
    expect(trafficOf(2)).toMatchObject({
      "path:/de/news": [1, 1],
      "page_type:news_index": [1, 1],
      "browser:safari": [1, 0],
    });
    expect(trafficOf(2)[`entry:/de/news`]).toBeUndefined();
  });

  it("opens a new visit after the idle time, and keeps it open on every hit", async () => {
    await collect([pageview()]);
    const visitKey = [...redis.hashes.keys()][0];
    expect(redis.ttls.get(visitKey)).toBe(VISIT_IDLE_SECONDS);
    redis.expireVisits();
    await collect([tabChanged]);
    expect(redis.hashes.size).toBe(0);
    await collect([pageview()]);
    expect(trafficOf(2)[`entry:${PATH}`]).toEqual([1, 1]);
  });

  it("gives another browser on the same address its own visit", async () => {
    await collect([pageview()]);
    await collect([pageview()], { userAgent: DESKTOP });
    expect(trafficOf(1)).toMatchObject({
      [`entry:${PATH}`]: [1, 1],
      "browser:chrome": [1, 1],
      "os:windows": [1, 1],
    });
  });

  it("sums several hits of one request into one row each", async () => {
    await collect([pageview(), pageview(), tabChanged, tabChanged]);
    expect(trafficOf()[`path:${PATH}`]).toEqual([2, 1]);
    expect(trafficOf()[`entry:${PATH}`]).toEqual([1, 1]);
    const params = query.mock.calls[0][1] as unknown[][];
    expect(params[6]).toEqual(["tab_changed"]);
    expect(params[10]).toEqual(["map"]);
    expect(params[11]).toEqual([""]);
    expect(params[13]).toEqual([2]);
    expect(params[14]).toEqual(["tab_changed"]);
    expect(params[16]).toEqual([PARK_ID]);
    expect(params[17]).toEqual(["park"]);
    expect(params[18]).toEqual([2]);
  });

  it("drops a refused hit alone and writes nothing when none is left", async () => {
    await expect(
      collect([pageview({ pageType: "landing" }), tabChanged]),
    ).resolves.toBe(1);
    await expect(collect([pageview({ locale: "pt" }), 7])).resolves.toBe(0);
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("counts nothing from a bot or a request without a User-Agent", async () => {
    for (const userAgent of [
      "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
      "curl/8.5.0",
      null,
    ]) {
      await expect(collect([pageview()], { userAgent })).resolves.toBe(0);
    }
    expect(query).not.toHaveBeenCalled();
    expect(redis.commands).toEqual([]);
  });

  it("stops an address at its daily budget, splitting the request that crosses it", async () => {
    redis.counters.set(
      [...(await capKeyOf())][0],
      MAX_HITS_PER_ADDRESS_PER_DAY - 2,
    );
    await expect(
      collect([pageview(), pageview(), pageview(), pageview()]),
    ).resolves.toBe(2);
    await expect(collect([pageview()])).resolves.toBe(0);
    expect(query).toHaveBeenCalledTimes(1);
    // A whole IPv6 /64 is one address; the next /64 is another.
    await expect(
      collect([pageview()], { ip: "2001:db8:1:2::1" }),
    ).resolves.toBe(1);
  });

  /** Learns the cap key by sending one hit, then forgets everything else. */
  async function capKeyOf(): Promise<Set<string>> {
    await collect([pageview()]);
    const keys = new Set(
      [...redis.counters.keys()].filter((k) =>
        k.startsWith("site-analytics:cap:"),
      ),
    );
    query.mockClear();
    return keys;
  }

  it("lets the cap expire a minute after its UTC day, and the visitors three days later", async () => {
    await collect([pageview()]);
    const ttlOf = (prefix: string) =>
      [...redis.ttls.entries()].find(([key]) => key.startsWith(prefix))?.[1];
    // 21:15:30 → 2 h 44 min 30 s to midnight.
    const toMidnight = 2 * 3600 + 44 * 60 + 30;
    expect(ttlOf("site-analytics:cap:")).toBe(toMidnight + 60);
    expect(ttlOf("site-analytics:uv:2026-10-28:all")).toBe(
      toMidnight + 3 * 86400,
    );
    expect(ttlOf("site-analytics:rt:uv:202610282115:all")).toBe(600);
    expect(ttlOf("site-analytics:rt:pv:202610282115:all")).toBe(2100);
  });

  it("adds the visitor to the day's set overall and per language, and to the minute", async () => {
    await collect([pageview(), pageview({ locale: "en", path: "/en" })]);
    expect(redis.sets.get("site-analytics:uv:2026-10-28:all")?.size).toBe(1);
    expect(redis.sets.get("site-analytics:uv:2026-10-28:de")?.size).toBe(1);
    expect(redis.sets.get("site-analytics:uv:2026-10-28:en")?.size).toBe(1);
    expect(redis.counters.get("site-analytics:rt:pv:202610282115:all")).toBe(2);
    expect(redis.counters.get("site-analytics:rt:pv:202610282115:de")).toBe(1);
    await collect([tabChanged], { userAgent: DESKTOP });
    // An event alone is activity, not a visitor of the day.
    expect(redis.sets.get("site-analytics:uv:2026-10-28:all")?.size).toBe(1);
    expect(redis.sets.get("site-analytics:rt:uv:202610282115:all")?.size).toBe(
      2,
    );
  });

  it("writes the address and the User-Agent nowhere: not in a key, a field, an element or a row", async () => {
    await collect([pageview({ referrer: "google.com" }), tabChanged]);
    const everything = JSON.stringify([
      redis.commands,
      query.mock.calls,
      [...redis.hashes.entries()].map(([k, v]) => [k, [...v]]),
    ]);
    expect(everything).not.toContain(IP);
    expect(everything).not.toContain("iPhone");
    for (const [command, key, member] of redis.commands) {
      expect(key).toMatch(
        /^site-analytics:(cap|visit):[0-9a-f]{32}$|^site-analytics:(uv|rt:uv|rt:pv):/,
      );
      // A visit's fields and a visitor are HMACs too, not a page or a reader.
      if (command === "hsetnx" && member !== "s")
        expect(member).toMatch(/^[0-9a-f]{32}$/);
      if (command === "pfadd") expect(member).toMatch(/^[0-9a-f]{32}$/);
    }
  });

  it("keys a visit on the secret: two instances holding it share it, another secret does not", async () => {
    await collect([pageview()]);
    const visitKey = [...redis.hashes.keys()][0];
    await (
      await build()
    ).collect({ hits: [pageview()], ip: IP, userAgent: BROWSER }, NOW);
    expect([...redis.hashes.keys()]).toEqual([visitKey]);

    delete process.env.ANALYTICS_HASH_SECRET;
    process.env.PAGE_VIEWS_HASH_SECRET = "the-fallback";
    await (
      await build()
    ).collect({ hits: [pageview()], ip: IP, userAgent: BROWSER }, NOW);
    expect(redis.hashes.size).toBe(2);
  });

  it("still counts page views when Redis is down, without a visit", async () => {
    redis.down = true;
    await expect(collect([pageview()])).resolves.toBe(1);
    expect(trafficOf()).toMatchObject({
      [`path:${PATH}`]: [1, 0],
      "locale:de": [1, 0],
    });
    expect(trafficOf()[`entry:${PATH}`]).toBeUndefined();
  });

  it("counts a hit without an address, uncapped and without a visit", async () => {
    await expect(collect([pageview()], { ip: null })).resolves.toBe(1);
    expect(redis.commands).toEqual([]);
    expect(lookupCountry).not.toHaveBeenCalled();
    expect(trafficOf()["country:zz"]).toEqual([1, 0]);
  });

  it("files an address GeoIP cannot place under zz", async () => {
    lookupCountry.mockReturnValue(null);
    await collect([pageview()]);
    expect(trafficOf()["country:zz"]).toEqual([1, 1]);
  });

  it("swallows a failed write, so the beacon still gets its 204", async () => {
    query.mockRejectedValueOnce(new Error("deadlock detected"));
    await expect(collect([pageview()])).resolves.toBe(0);
  });

  describe("snapshotVisitors", () => {
    it("writes the last two finished days that still have a set, all languages and each", async () => {
      redis.sets.set("site-analytics:uv:2026-10-27:all", new Set(["a", "b"]));
      redis.sets.set("site-analytics:uv:2026-10-27:de", new Set(["a"]));
      redis.sets.set("site-analytics:uv:2026-10-26:all", new Set(["c"]));
      await expect(service.snapshotVisitors(NOW)).resolves.toBe(3);
      const [sql, params] = query.mock.calls[0];
      expect(sql).toContain("ON CONFLICT (day, scope)");
      expect(params).toEqual([
        ["2026-10-27", "2026-10-27", "2026-10-26"],
        ["all", "de", "all"],
        [2, 1, 1],
      ]);
    });

    it("never writes a day whose set is gone as zero visitors", async () => {
      await expect(service.snapshotVisitors(NOW)).resolves.toBe(0);
      expect(query).not.toHaveBeenCalled();
    });
  });

  describe("prune", () => {
    it("deletes what is older than 400 days from every table", async () => {
      query.mockResolvedValue([[], 3]);
      const deleted = await service.prune(NOW);
      expect(deleted).toEqual({
        site_traffic_days: 3,
        site_event_days: 3,
        site_event_entity_days: 3,
        site_visitor_days: 3,
      });
      for (const [sql, params] of query.mock.calls) {
        expect(sql).toMatch(/^DELETE FROM site_\w+ WHERE day < \$1::date$/);
        expect(params).toEqual(["2025-09-23"]);
      }
    });
  });

  describe("onModuleInit", () => {
    it("writes the storage options onto the three tables a hit writes", async () => {
      query.mockResolvedValue([{ reloptions: null }]);
      await service.onModuleInit();
      const alters = query.mock.calls
        .map(([sql]) => sql as string)
        .filter((sql) => sql.startsWith("ALTER TABLE"));
      expect(alters).toEqual(
        ["site_traffic_days", "site_event_days", "site_event_entity_days"].map(
          (table) =>
            `ALTER TABLE ${table} SET (${SITE_ANALYTICS_STORAGE_OPTIONS.join(", ")})`,
        ),
      );
    });
  });
});

describe("buildCollectRows", () => {
  const reader = { browser: "firefox", os: "linux", country: "de" };
  const hits = [
    validateHit(pageview({ referrer: "bing.com" })),
    validateHit(tabChanged),
  ] as ValidHit[];

  it("counts a visit for a dimension only where the visit saw its value first", () => {
    const rows = buildCollectRows(
      hits,
      [{ start: false, firstSeen: new Set(["path"]) }],
      reader,
    );
    const traffic = [...rows.traffic.values()].map(
      (row) => `${row.dim}:${row.value}:${row.views}:${row.visits}`,
    );
    expect(traffic).toEqual([
      `path:${PATH}:1:1`,
      "page_type:park:1:0",
      "viewport:mobile:1:0",
      "browser:firefox:1:0",
      "os:linux:1:0",
      "country:de:1:0",
      "locale:de:1:0",
    ]);
    expect([...rows.entities.values()]).toEqual([
      {
        name: "tab_changed",
        locale: "de",
        entity: PARK_ID,
        kind: "park",
        count: 1,
      },
    ]);
  });
});
