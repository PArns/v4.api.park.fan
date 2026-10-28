import { Test } from "@nestjs/testing";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { GeoipService } from "../geoip/geoip.service";
import { SiteAnalyticsRedisMock } from "../../test/mocks/site-analytics-redis.mock";
import { SiteAnalyticsBlocklist } from "./site-analytics-blocklist.service";
import { SiteAnalyticsCollectService } from "./site-analytics-collect.service";
import {
  SiteAnalyticsStorageService,
  rowsOfFields,
} from "./site-analytics-storage.service";
import {
  MAX_HITS_PER_ADDRESS_PER_DAY,
  SITE_ANALYTICS_KEYS as KEYS,
  VISIT_IDLE_SECONDS,
} from "./site-analytics.constants";

const BROWSER =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1";
const DESKTOP =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const IP = "203.0.113.7";
const NOW = new Date("2026-10-28T21:16:30Z");
const WINDOW = "2026-10-28T21:15:00.000Z";
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
const event = (
  name: string,
  props: Record<string, unknown> = {},
  overrides: Record<string, unknown> = {},
) => ({
  t: "event",
  locale: "de",
  path: PATH,
  pageType: "park",
  viewport: "mobile",
  name,
  props,
  ...overrides,
});

/**
 * The hot path. What is pinned: a bot, a blocked name, a hit past the
 * address's budget, a new name past the daily limit and a fourth read of one
 * page never reach the buffer; a combination past the limit counts as
 * `_overflow`; a visit opens on a page view without a live visit key, carries
 * its entry, referrer and channel onto every later row and counts `engaged`
 * under its first page view at the second; no address or User-Agent reaches a
 * key, a field or a member; Redis down still counts, straight into Postgres.
 */
describe("SiteAnalyticsCollectService", () => {
  let service: SiteAnalyticsCollectService;
  let redis: SiteAnalyticsRedisMock;
  let writeDirect: jest.Mock;
  let blocked: Set<string>;
  let lookupCountry: jest.Mock;

  const build = async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [
        SiteAnalyticsCollectService,
        { provide: REDIS_CLIENT, useValue: redis },
        { provide: GeoipService, useValue: { lookupCountry } },
        { provide: SiteAnalyticsStorageService, useValue: { writeDirect } },
        {
          provide: SiteAnalyticsBlocklist,
          useValue: { names: async () => blocked },
        },
      ],
    }).compile();
    return moduleRef.get(SiteAnalyticsCollectService);
  };

  const collect = (
    hits: unknown[],
    overrides: { ip?: string | null; userAgent?: string | null } = {},
    now = NOW,
  ) => service.collect({ hits, ip: IP, userAgent: BROWSER, ...overrides }, now);

  /** The buffered window as rows, the way the flusher will write them. */
  const buffered = (window = WINDOW) =>
    rowsOfFields(
      Object.fromEntries(redis.hashes.get(KEYS.buffer(window)) ?? []),
    );
  const pageviewRows = () =>
    buffered().pageviews.map(({ dims, views, visits, engaged }) => ({
      path: dims[1],
      entry: dims[7],
      referrer: dims[8],
      channel: dims[9],
      views,
      visits,
      engaged,
    }));

  beforeEach(async () => {
    process.env.ANALYTICS_HASH_SECRET = "test-secret";
    redis = new SiteAnalyticsRedisMock();
    writeDirect = jest
      .fn()
      .mockResolvedValue({ pageviewRows: 0, eventRows: 0 });
    blocked = new Set();
    lookupCountry = jest.fn().mockReturnValue("nl");
    service = await build();
  });

  afterEach(() => {
    delete process.env.ANALYTICS_HASH_SECRET;
    delete process.env.PAGE_VIEWS_HASH_SECRET;
  });

  it("buffers a page view in the five-minute window with every dimension, as a visit from its channel", async () => {
    await expect(
      collect([pageview({ referrer: "www.google.de", campaign: undefined })]),
    ).resolves.toBe(1);
    expect(await redis.smembers(KEYS.windows)).toEqual([WINDOW]);
    expect(buffered().pageviews).toEqual([
      {
        dims: [
          "de",
          PATH,
          "park",
          "mobile",
          "safari",
          "ios",
          "nl",
          PATH,
          "google",
          "search",
          "",
        ],
        views: 1,
        visits: 1,
        engaged: 0,
      },
    ]);
    expect(writeDirect).not.toHaveBeenCalled();
  });

  it("carries the visit's entry and channel onto its later rows, and counts engaged under its first page view", async () => {
    await collect([pageview({ referrer: "bing.com" })]);
    await collect([pageview({ path: "/de/news", pageType: "news_index" })]);
    await collect([pageview({ path: "/de/news/a", pageType: "news" })]);
    expect(pageviewRows()).toEqual([
      // The start, and the engaged its second page view added.
      {
        path: PATH,
        entry: PATH,
        referrer: "bing",
        channel: "search",
        views: 1,
        visits: 1,
        engaged: 1,
      },
      {
        path: "/de/news",
        entry: PATH,
        referrer: "bing",
        channel: "search",
        views: 1,
        visits: 0,
        engaged: 0,
      },
      {
        path: "/de/news/a",
        entry: PATH,
        referrer: "bing",
        channel: "search",
        views: 1,
        visits: 0,
        engaged: 0,
      },
    ]);
  });

  it("opens a new visit after the idle time, and keeps it open on every hit", async () => {
    await collect([pageview()]);
    const visitKey = [...redis.hashes.keys()].find((k) =>
      k.startsWith("site-analytics:visit:"),
    )!;
    expect(redis.ttls.get(visitKey)).toBe(VISIT_IDLE_SECONDS);
    redis.expireKeys("site-analytics:visit:");
    await collect([pageview({ path: "/de/news", pageType: "news_index" })]);
    expect(pageviewRows().find((row) => row.path === "/de/news")).toMatchObject(
      { entry: "/de/news", visits: 1 },
    );
  });

  it("gives another browser on the same address its own visit", async () => {
    await collect([pageview()]);
    await collect([pageview()], { userAgent: DESKTOP });
    expect(
      buffered()
        .pageviews.map((row) => [row.dims[4], row.visits])
        .sort(),
    ).toEqual([
      ["chrome", 1],
      ["safari", 1],
    ]);
  });

  it("buffers an event with its props, entity and the visit's dimensions", async () => {
    await collect([
      pageview({ referrer: "chatgpt.com" }),
      event("tab_changed", { tab: "map" }, { entity: PARK_ID }),
      event("tab_changed", { tab: "map" }, { entity: PARK_ID }),
    ]);
    expect(buffered().events).toEqual([
      {
        dims: [
          "tab_changed",
          "de",
          PATH,
          "park",
          "mobile",
          "safari",
          "ios",
          "nl",
          PATH,
          "chatgpt",
          "ai",
          "",
          "tab=map",
          PARK_ID,
        ],
        count: 2,
      },
    ]);
  });

  it("counts nothing from a bot or a request without a User-Agent", async () => {
    for (const userAgent of [
      "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
      "curl/8.5.0",
      null,
    ]) {
      await expect(collect([pageview()], { userAgent })).resolves.toBe(0);
    }
    expect(redis.commands).toEqual([]);
  });

  it("drops a malformed hit alone and a blocked event name", async () => {
    blocked.add("noisy_event");
    await expect(
      collect([
        pageview({ pageType: "Bad" }),
        event("noisy_event"),
        event("ok_event"),
      ]),
    ).resolves.toBe(1);
    expect(buffered().events.map((row) => row.dims[0])).toEqual(["ok_event"]);
  });

  it("stops an address at its daily budget, splitting the request that crosses it", async () => {
    await collect([pageview()]);
    const capKey = [...redis.counters.keys()].find((k) =>
      k.startsWith("site-analytics:cap:"),
    )!;
    redis.counters.set(capKey, MAX_HITS_PER_ADDRESS_PER_DAY - 2);
    await expect(
      collect([pageview(), pageview(), pageview(), pageview()]),
    ).resolves.toBe(2);
    await expect(collect([pageview()])).resolves.toBe(0);
    // A whole IPv6 /64 is one address; the next /64 is another.
    await expect(
      collect([pageview()], { ip: "2001:db8:1:2::1" }),
    ).resolves.toBe(1);
  });

  it("drops a new event name past the day's limit, and takes it out of the set again", async () => {
    const names = KEYS.names("2026-10-28");
    redis.sets.set(
      names,
      new Set(Array.from({ length: 300 }, (_, i) => `n_${i}`)),
    );
    await expect(collect([event("n_1"), event("one_too_many")])).resolves.toBe(
      1,
    );
    expect(redis.sets.get(names)?.has("one_too_many")).toBe(false);
    expect(redis.sets.get(names)?.size).toBe(300);
  });

  it("counts a combination past the day's limit as _overflow, without its entity", async () => {
    const combos = KEYS.combinations("2026-10-28", "favorite_add");
    redis.sets.set(
      combos,
      new Set(Array.from({ length: 2000 }, (_, i) => `c${i}`)),
    );
    await collect([
      event("favorite_add", { type: "park" }, { entity: PARK_ID }),
    ]);
    expect(buffered().events[0].dims.slice(12)).toEqual(["_overflow=1", ""]);
  });

  it("files a page type past the day's limit as other", async () => {
    const types = KEYS.pageTypes("2026-10-28");
    redis.sets.set(
      types,
      new Set(Array.from({ length: 100 }, (_, i) => `t_${i}`)),
    );
    await collect([pageview({ pageType: "brand_new" })]);
    expect(buffered().pageviews[0].dims[2]).toBe("other");
  });

  it("counts three reads per reader, page and day, and never a read past the address's budget", async () => {
    const read = () => event("page_read", { kind: "news", key: "a-post" });
    const results: number[] = [];
    for (let i = 0; i < 5; i++) results.push(await collect([read()]));
    expect(results).toEqual([1, 1, 1, 0, 0]);
    // Another post is another page, and the language does not make one.
    await expect(
      collect([event("page_read", { kind: "news", key: "another-post" })]),
    ).resolves.toBe(1);
    await expect(
      collect([{ ...read(), locale: "en", path: "/en" }]),
    ).resolves.toBe(0);
  });

  it("exempts page_read from the combination limit, every park being one", async () => {
    const combos = KEYS.combinations("2026-10-28", "page_read");
    redis.sets.set(
      combos,
      new Set(Array.from({ length: 2000 }, (_, i) => `c${i}`)),
    );
    await collect([event("page_read", { kind: "park" }, { entity: PARK_ID })]);
    expect(buffered().events[0].dims.slice(12)).toEqual(["kind=park", PARK_ID]);
  });

  it("writes the address and the User-Agent nowhere: not in a key, a field or a member", async () => {
    await collect([
      pageview({ referrer: "google.com" }),
      event("search_opened", { surface: "hero" }),
    ]);
    const everything = JSON.stringify([
      redis.commands,
      [...redis.hashes.entries()].map(([k, v]) => [k, [...v.entries()]]),
      [...redis.sets.entries()].map(([k, v]) => [k, [...v]]),
    ]);
    expect(everything).not.toContain(IP);
    expect(everything).not.toContain("iPhone");
    for (const [command, key, member] of redis.commands) {
      if (command === "pfadd") expect(member).toMatch(/^[0-9a-f]{32}$/);
      expect(String(key)).toMatch(/^site-analytics:/);
    }
  });

  it("keys a visit on the secret: two instances holding it share it, the fallback secret works", async () => {
    await collect([pageview()]);
    const visitKeys = () =>
      [...redis.hashes.keys()].filter((k) =>
        k.startsWith("site-analytics:visit:"),
      );
    expect(visitKeys()).toHaveLength(1);
    await (
      await build()
    ).collect({ hits: [pageview()], ip: IP, userAgent: BROWSER }, NOW);
    expect(visitKeys()).toHaveLength(1);

    delete process.env.ANALYTICS_HASH_SECRET;
    process.env.PAGE_VIEWS_HASH_SECRET = "the-fallback";
    await (
      await build()
    ).collect({ hits: [pageview()], ip: IP, userAgent: BROWSER }, NOW);
    expect(visitKeys()).toHaveLength(2);
  });

  it("lets the daily keys expire a minute after their UTC day, the visitors three days later", async () => {
    await collect([pageview()]);
    const ttlOf = (prefix: string) =>
      [...redis.ttls.entries()].find(([key]) => key.startsWith(prefix))?.[1];
    // 21:16:30 → 2 h 43 min 30 s to midnight.
    const toMidnight = 2 * 3600 + 43 * 60 + 30;
    expect(ttlOf("site-analytics:cap:")).toBe(toMidnight + 60);
    expect(ttlOf("site-analytics:page-types:")).toBe(toMidnight + 60);
    expect(ttlOf("site-analytics:uv:2026-10-28:all")).toBe(
      toMidnight + 3 * 86400,
    );
    expect(ttlOf("site-analytics:uvh:2026102821:all")).toBe(toMidnight + 3600);
    expect(ttlOf("site-analytics:rt:uv:202610282116:all")).toBe(600);
    expect(ttlOf("site-analytics:rt:pv:202610282116:all")).toBe(2100);
    // The buffer has none: volatile-lru never evicts a key without a TTL.
    expect(redis.ttls.has(KEYS.buffer(WINDOW))).toBe(false);
  });

  it("writes straight to Postgres when Redis cannot take the counts", async () => {
    redis.failNext = ["cap down", "guards down", "buffer down"];
    await expect(collect([pageview()])).resolves.toBe(1);
    expect(writeDirect).toHaveBeenCalledTimes(1);
    const [window, increments] = writeDirect.mock.calls[0];
    expect(window).toBe(WINDOW);
    const fields = Object.fromEntries(
      [...(increments as Map<string, number>)].map(([k, v]) => [k, String(v)]),
    );
    // Without Redis there is no visit: a view, no visit, no channel.
    expect(rowsOfFields(fields).pageviews).toEqual([
      expect.objectContaining({ views: 1, visits: 0, engaged: 0 }),
    ]);
  });

  it("counts a hit without an address, uncapped and without a visit", async () => {
    await expect(collect([pageview()], { ip: null })).resolves.toBe(1);
    expect(lookupCountry).not.toHaveBeenCalled();
    expect(buffered().pageviews[0]).toMatchObject({ visits: 0 });
    expect(buffered().pageviews[0].dims[6]).toBe("zz");
  });
});
