import { Test } from "@nestjs/testing";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { dailyHashKey, readerHash } from "../common/utils/reader-hash.util";
import { GeoipService } from "../geoip/geoip.service";
import { SiteAnalyticsRedisMock } from "../../test/mocks/site-analytics-redis.mock";
import { SiteAnalyticsBlocklist } from "./site-analytics-blocklist.service";
import { SiteAnalyticsCollectService } from "./site-analytics-collect.service";
import {
  SiteAnalyticsStorageService,
  rowsOfFields,
  type PageviewCounts,
} from "./site-analytics-storage.service";
import { SiteEntityNames } from "./site-entity-names.service";
import {
  DROP_REASONS,
  FALLBACK_MAX_FIELDS,
  MAX_HITS_PER_ADDRESS_PER_DAY,
  MAX_HITS_PER_WINDOW,
  OVERFLOW_VALUE,
  SITE_ANALYTICS_KEYS as KEYS,
  SITE_GUARDS,
  LIVE_KEY_TTL_SECONDS,
  OPEN_VISIT_MAX_AGE_SECONDS,
  VISIT_KEY_TTL_SECONDS,
} from "./site-analytics.constants";

const BROWSER =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1";
const DESKTOP =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const IP = "203.0.113.7";
const DAY = "2026-10-28";
const NOW = new Date("2026-10-28T21:16:30Z");
const WINDOW = "2026-10-28T21:15:00.000Z";
const PARK_ID = "3f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
const RIDE_ID = "5a1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
const PATH = "/de/parks/europe/germany/bruehl/phantasialand";

const pageview = (overrides: Record<string, unknown> = {}) => ({
  t: "pageview",
  locale: "de",
  path: PATH,
  pageType: "park",
  ...overrides,
});
const engagement = (
  seconds: number,
  overrides: Record<string, unknown> = {},
) => ({
  t: "engagement",
  locale: "de",
  path: PATH,
  pageType: "park",
  seconds,
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
  name,
  props,
  ...overrides,
});

/** A row's counts: zero but for the given ones. */
const counts = (given: Partial<PageviewCounts>): PageviewCounts => ({
  views: 0,
  seconds: 0,
  visits: 0,
  engaged: 0,
  visitSeconds: 0,
  exits: 0,
  visitSpanSeconds: 0,
  ...given,
});

/** A set of `n` made-up members, to stand for a full guard. */
const filled = (n: number, prefix = "x") =>
  new Set(Array.from({ length: n }, (_, i) => `${prefix}${i}`));

/**
 * The hot path. What is pinned: a bot, a blocked name, a read of a park that
 * does not exist, a hit past the address's or the window's budget, a new name
 * past the daily guard and a read past its caps never reach the buffer; a
 * value past its guard is kept as an overflow value; one address cannot fill
 * a guard alone and yesterday's values pass a full one; a page view's row
 * carries no visit dimension, the visit's row carries all of them; no address
 * or User-Agent reaches a key, a field or a member, and the reader's keys
 * change with the day; Redis down counts in a bounded memory buffer, a
 * partly refused write falls back for the refused fields only.
 */
describe("SiteAnalyticsCollectService", () => {
  let service: SiteAnalyticsCollectService;
  let redis: SiteAnalyticsRedisMock;
  let writeDirect: jest.Mock;
  let blocked: Set<string>;
  let lookupCountry: jest.Mock;
  let known: { parks: Set<string>; attractions: Set<string> } | null;

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
        { provide: SiteEntityNames, useValue: { knownIds: async () => known } },
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
    buffered().pageviews.map(({ dims, counts }) => ({
      path: dims[1],
      entry: dims[8],
      referrer: dims[9],
      channel: dims[10],
      views: counts.views,
      visits: counts.visits,
      engaged: counts.engaged,
    }));
  const visitKeys = () =>
    [...redis.hashes.keys()].filter((k) =>
      k.startsWith("site-analytics:visit:"),
    );
  const drops = (day = DAY) =>
    Object.fromEntries(redis.hashes.get(KEYS.drops(day)) ?? []);

  beforeEach(async () => {
    process.env.ANALYTICS_HASH_SECRET = "test-secret";
    redis = new SiteAnalyticsRedisMock();
    writeDirect = jest
      .fn()
      .mockResolvedValue({ pageviewRows: 0, eventRows: 0 });
    blocked = new Set();
    lookupCountry = jest.fn().mockReturnValue("nl");
    known = { parks: new Set([PARK_ID]), attractions: new Set([RIDE_ID]) };
    service = await build();
  });

  afterEach(() => {
    delete process.env.ANALYTICS_HASH_SECRET;
    delete process.env.PAGE_VIEWS_HASH_SECRET;
  });

  it("buffers a page view without visit dimensions, and its visit on a row of its own", async () => {
    await expect(
      collect([pageview({ referrer: "www.google.de" })]),
    ).resolves.toBe(1);
    expect(await redis.smembers(KEYS.windows)).toEqual([WINDOW]);
    const page = ["de", PATH, "park", "mobile", "safari", "ios", "nl", "zz"];
    expect(buffered().pageviews).toEqual([
      { dims: [...page, "", "", ""], counts: counts({ views: 1 }) },
      {
        dims: [...page, PATH, "google", "search"],
        counts: counts({ visits: 1 }),
      },
    ]);
    expect(writeDirect).not.toHaveBeenCalled();
  });

  it("ties no later page to the visit's referrer, and counts engaged on the visit's row", async () => {
    await collect([pageview({ referrer: "bing.com" })]);
    await collect([pageview({ path: "/de/news", pageType: "news_index" })]);
    await collect([pageview({ path: "/de/news/a", pageType: "news" })]);
    expect(pageviewRows()).toEqual([
      {
        path: PATH,
        entry: "",
        referrer: "",
        channel: "",
        views: 1,
        visits: 0,
        engaged: 0,
      },
      // The visit's row: the start, and the engaged its second page view added.
      {
        path: PATH,
        entry: PATH,
        referrer: "bing",
        channel: "search",
        views: 0,
        visits: 1,
        engaged: 1,
      },
      {
        path: "/de/news",
        entry: "",
        referrer: "",
        channel: "",
        views: 1,
        visits: 0,
        engaged: 0,
      },
      {
        path: "/de/news/a",
        entry: "",
        referrer: "",
        channel: "",
        views: 1,
        visits: 0,
        engaged: 0,
      },
    ]);
  });

  it("opens a new visit once the old one is closed, and keeps it open on every hit", async () => {
    await collect([pageview()]);
    const visitKey = [...redis.hashes.keys()].find((k) =>
      k.startsWith("site-analytics:visit:"),
    )!;
    expect(redis.ttls.get(visitKey)).toBe(VISIT_KEY_TTL_SECONDS);
    // The flusher closed it, or it outlived its key.
    redis.expireKeys("site-analytics:visit:");
    await collect([pageview({ path: "/de/news", pageType: "news_index" })]);
    expect(pageviewRows()).toContainEqual(
      expect.objectContaining({
        path: "/de/news",
        entry: "/de/news",
        visits: 1,
      }),
    );
  });

  it("splits a visit at midnight: the visit key belongs to its UTC day", async () => {
    await collect([pageview()], {}, new Date("2026-10-28T23:59:00Z"));
    await collect([pageview()], {}, new Date("2026-10-29T00:01:00Z"));
    const visits = [...redis.hashes.keys()].filter((k) =>
      k.startsWith("site-analytics:visit:"),
    );
    expect(visits).toHaveLength(2);
  });

  it("gives another browser on the same address its own visit", async () => {
    await collect([pageview()]);
    await collect([pageview()], { userAgent: DESKTOP });
    expect(
      buffered()
        .pageviews.filter((row) => row.counts.visits > 0)
        .map((row) => row.dims[4])
        .sort(),
    ).toEqual(["chrome", "safari"]);
  });

  it("buffers an event with its props and entity, and the page's dimensions only", async () => {
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

  it("stops an address at its daily budget, keying IPv6 on the /56", async () => {
    await collect([pageview()]);
    const capKey = [...redis.counters.keys()].find((k) =>
      k.startsWith("site-analytics:cap:"),
    )!;
    redis.counters.set(capKey, MAX_HITS_PER_ADDRESS_PER_DAY - 2);
    await expect(
      collect([pageview(), pageview(), pageview(), pageview()]),
    ).resolves.toBe(2);
    await expect(collect([pageview()])).resolves.toBe(0);
    expect(drops()[DROP_REASONS.address]).toBe("3");

    const v6 = "2001:db8:1:2a01::1";
    await collect([pageview()], { ip: v6 });
    const v6Cap = [...redis.counters.keys()].find(
      (k) => k.startsWith("site-analytics:cap:") && k !== capKey,
    )!;
    redis.counters.set(v6Cap, MAX_HITS_PER_ADDRESS_PER_DAY);
    // Another /64 of the same /56 has no fresh budget; another /56 has.
    await expect(
      collect([pageview()], { ip: "2001:db8:1:2aff::1" }),
    ).resolves.toBe(0);
    await expect(
      collect([pageview()], { ip: "2001:db8:1:2b00::1" }),
    ).resolves.toBe(1);
  });

  it("sheds hits past the window's budget, whoever sends them", async () => {
    redis.counters.set(KEYS.windowHits(WINDOW), MAX_HITS_PER_WINDOW - 1);
    await expect(collect([pageview(), pageview()])).resolves.toBe(1);
    await expect(collect([pageview()], { ip: "198.51.100.9" })).resolves.toBe(
      0,
    );
    expect(drops()[DROP_REASONS.window]).toBe("2");
    // The next window starts afresh.
    await expect(
      collect([pageview()], {}, new Date("2026-10-28T21:20:30Z")),
    ).resolves.toBe(1);
  });

  it("drops a new event name past the day's guard, and lets the names it holds pass", async () => {
    const names = KEYS.guard("names", DAY);
    redis.sets.set(names, filled(SITE_GUARDS.names.limit, "n_"));
    await expect(collect([event("n_1"), event("one_too_many")])).resolves.toBe(
      1,
    );
    expect(redis.sets.get(names)?.has("one_too_many")).toBe(false);
    expect(drops()[DROP_REASONS.name]).toBe("1");
  });

  it("lets one address bring only a few new names a day, so it cannot fill the guard", async () => {
    const budget = SITE_GUARDS.names.perAddress;
    const fresh = Array.from({ length: budget + 5 }, (_, i) =>
      event(`junk_${i}`),
    );
    await expect(collect(fresh.slice(0, 20))).resolves.toBe(budget);
    await expect(collect(fresh.slice(20))).resolves.toBe(0);
    expect(redis.sets.get(KEYS.guard("names", DAY))?.size).toBe(budget);
    // Another address still can, and the names now known pass for anyone.
    await expect(
      collect([event("real_event"), event("junk_0")], { ip: "198.51.100.9" }),
    ).resolves.toBe(2);
  });

  it("admits yesterday's names into a full guard, page_read always", async () => {
    redis.sets.set(KEYS.guard("names", DAY), filled(SITE_GUARDS.names.limit));
    redis.sets.set(
      KEYS.guard("names", "2026-10-27"),
      new Set(["search_opened"]),
    );
    await expect(
      collect([
        event("search_opened", { surface: "hero" }),
        event("brand_new"),
        event("page_read", { kind: "park" }, { entity: PARK_ID }),
      ]),
    ).resolves.toBe(2);
    expect(
      buffered()
        .events.map((row) => row.dims[0])
        .sort(),
    ).toEqual(["page_read", "search_opened"]);
  });

  it("counts a combination past the day's guard as _overflow, without its entity", async () => {
    redis.sets.set(
      KEYS.guard("combinations", DAY, "favorite_add"),
      filled(SITE_GUARDS.combinations.limit),
    );
    await collect([
      event("favorite_add", { type: "park" }, { entity: PARK_ID }),
    ]);
    expect(buffered().events[0].dims.slice(8)).toEqual(["_overflow=1", ""]);
    expect(drops()[DROP_REASONS.combination]).toBe("1");
  });

  it("files a page type, path or referrer past its guard under an overflow value", async () => {
    redis.sets.set(
      KEYS.guard("pageTypes", DAY),
      filled(SITE_GUARDS.pageTypes.limit),
    );
    redis.sets.set(KEYS.guard("paths", DAY), filled(SITE_GUARDS.paths.limit));
    redis.sets.set(
      KEYS.guard("referrers", DAY),
      filled(SITE_GUARDS.referrers.limit),
    );
    await collect([
      pageview({
        pageType: "brand_new",
        path: "/de/never-seen",
        referrer: "forum.example.org",
      }),
    ]);
    const visit = buffered().pageviews.find((row) => row.counts.visits > 0)!;
    expect(visit.dims).toEqual([
      "de",
      OVERFLOW_VALUE,
      "other",
      "mobile",
      "safari",
      "ios",
      "nl",
      "zz",
      OVERFLOW_VALUE,
      OVERFLOW_VALUE,
      "referral",
    ]);
  });

  it("keeps the known search engines out of the referrer guard", async () => {
    redis.sets.set(
      KEYS.guard("referrers", DAY),
      filled(SITE_GUARDS.referrers.limit),
    );
    await collect([pageview({ referrer: "google.com" })]);
    expect(
      buffered().pageviews.find((row) => row.counts.visits > 0)!.dims[9],
    ).toBe("google");
  });

  it("counts three reads per reader and page, ten per address and page, whatever the User-Agent", async () => {
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

    // Rotating the User-Agent passes the per-reader cap, not the per-page one.
    let counted = 0;
    for (let i = 0; i < 20; i++) {
      counted += await collect(
        [event("page_read", { kind: "news", key: "third" })],
        {
          userAgent: `${DESKTOP} rotation/${i}`,
        },
      );
    }
    expect(counted).toBe(10);
  });

  it("refuses a read of a park or ride that does not exist", async () => {
    await expect(
      collect([
        event("page_read", { kind: "park" }, { entity: PARK_ID }),
        event("page_read", { kind: "attraction" }, { entity: RIDE_ID }),
        event("page_read", { kind: "park" }, { entity: RIDE_ID }),
        event(
          "page_read",
          { kind: "attraction" },
          { entity: "9f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d" },
        ),
      ]),
    ).resolves.toBe(2);
    expect(drops()[DROP_REASONS.entity]).toBe("2");
    // Without the id list (the database away since start-up) reads pass.
    known = null;
    service = await build();
    await expect(
      collect([event("page_read", { kind: "park" }, { entity: RIDE_ID })], {
        ip: "198.51.100.9",
      }),
    ).resolves.toBe(1);
  });

  it("exempts page_read from the combination guard, every park being one", async () => {
    redis.sets.set(
      KEYS.guard("combinations", DAY, "page_read"),
      filled(SITE_GUARDS.combinations.limit),
    );
    await collect([event("page_read", { kind: "park" }, { entity: PARK_ID })]);
    expect(buffered().events[0].dims.slice(8)).toEqual(["kind=park", PARK_ID]);
  });

  it("drops the entity of a location-derived event before it reaches a row", async () => {
    await collect([
      event(
        "nearby_parks_loaded",
        { type: "in_park", source: "gps" },
        { entity: PARK_ID },
      ),
      event("compass_ride_opened", { from: "bar" }, { entity: PARK_ID }),
    ]);
    expect(buffered().events.map((row) => row.dims[9])).toEqual(["", ""]);
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

  it("hashes a reader under the day's key, so two days' keys share nothing", async () => {
    await collect([pageview()], {}, new Date("2026-10-28T10:00:00Z"));
    await collect([pageview()], {}, new Date("2026-10-29T10:00:00Z"));
    const caps = [...redis.counters.keys()].filter((k) =>
      k.startsWith("site-analytics:cap:"),
    );
    expect(caps).toHaveLength(2);
    // The key is HMAC(HMAC(secret, day), …), not HMAC(secret, …).
    const expected = readerHash(
      dailyHashKey("test-secret", DAY),
      "site-analytics",
      "cap",
      IP,
    );
    expect(caps).toContain(KEYS.cap(expected));
    expect(caps).not.toContain(
      KEYS.cap(readerHash("test-secret", "site-analytics", "cap", DAY, IP)),
    );
  });

  it("keys a visit on the secret: two instances holding it share it, the fallback secret works", async () => {
    await collect([pageview()]);
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

  it("lets every reader key end within about a day: the visitors an hour after their day", async () => {
    await collect([pageview(), event("page_read", { kind: "news", key: "p" })]);
    const ttlOf = (prefix: string) =>
      [...redis.ttls.entries()].find(([key]) => key.startsWith(prefix))?.[1];
    // 21:16:30 → 2 h 43 min 30 s to midnight.
    const toMidnight = 2 * 3600 + 43 * 60 + 30;
    expect(ttlOf("site-analytics:cap:")).toBe(toMidnight + 60);
    expect(ttlOf("site-analytics:read:")).toBe(toMidnight + 60);
    expect(ttlOf("site-analytics:read-page:")).toBe(toMidnight + 60);
    expect(ttlOf("site-analytics:read-addr:")).toBe(toMidnight + 60);
    expect(ttlOf("site-analytics:new:")).toBe(toMidnight + 60);
    expect(ttlOf("site-analytics:uv:2026-10-28:all")).toBe(toMidnight + 3600);
    expect(ttlOf("site-analytics:uvh:2026102821:all")).toBe(toMidnight + 3600);
    expect(ttlOf("site-analytics:rt:uv:202610282116:all")).toBe(2100);
    expect(ttlOf("site-analytics:rt:pv:202610282116:all")).toBe(2100);
    expect(ttlOf("site-analytics:live:pages:202610282116:all")).toBe(2100);
    expect(ttlOf("site-analytics:visit:")).toBe(40 * 60);
    // A guard set lives through tomorrow, which admits its values as known.
    expect(ttlOf("site-analytics:guard:paths:")).toBe(toMidnight + 86400 + 60);
    // The buffer has none: volatile-lru never evicts a key without a TTL.
    expect(redis.ttls.has(KEYS.buffer(WINDOW))).toBe(false);
  });

  it("holds the counts in memory when Redis is away, and writes them a window at a time", async () => {
    redis.failNext = ["cap down"];
    await expect(collect([pageview(), pageview()])).resolves.toBe(2);
    expect(writeDirect).not.toHaveBeenCalled();
    await service.drainFallback();
    expect(writeDirect).toHaveBeenCalledTimes(1);
    const [window, increments] = writeDirect.mock.calls[0];
    expect(window).toBe(WINDOW);
    const fields = Object.fromEntries(
      [...(increments as Map<string, number>)].map(([k, v]) => [k, String(v)]),
    );
    // Without Redis there is no visit: two views, no visit, no channel.
    expect(rowsOfFields(fields).pageviews).toEqual([
      expect.objectContaining({ counts: counts({ views: 2 }) }),
    ]);
    await service.drainFallback();
    expect(writeDirect).toHaveBeenCalledTimes(1);
  });

  it("drops and counts what no longer fits in memory, and reports it once Redis is back", async () => {
    redis.failNext = Array(FALLBACK_MAX_FIELDS + 1).fill("down");
    let counted = 0;
    for (let i = 0; i <= FALLBACK_MAX_FIELDS; i++) {
      counted += await collect([pageview({ path: `/de/p${i}` })]);
    }
    expect(counted).toBe(FALLBACK_MAX_FIELDS);
    redis.failNext = [];
    await collect([pageview()]);
    expect(drops()[DROP_REASONS.fallback]).toBe("1");
  });

  it("falls back for the refused fields only, so nothing is counted twice", async () => {
    redis.failCommands.add("hincrby");
    await expect(collect([pageview()])).resolves.toBe(1);
    redis.failCommands.clear();
    // The visit was remembered in Redis; the counts wait in memory.
    expect(redis.hashes.get(KEYS.buffer(WINDOW))).toBeUndefined();
    await service.drainFallback();
    const fields = Object.fromEntries(
      [...(writeDirect.mock.calls[0][1] as Map<string, number>)].map(
        ([k, v]) => [k, String(v)],
      ),
    );
    expect(
      rowsOfFields(fields).pageviews.map((row) => [
        row.counts.views,
        row.counts.visits,
      ]),
    ).toEqual([
      [1, 0],
      [0, 1],
    ]);
  });

  it("counts a hit without an address under one shared budget, without a visit", async () => {
    await expect(collect([pageview()], { ip: null })).resolves.toBe(1);
    expect(lookupCountry).not.toHaveBeenCalled();
    expect(buffered().pageviews).toHaveLength(1);
    expect(buffered().pageviews[0].counts.visits).toBe(0);
    expect(buffered().pageviews[0].dims[6]).toBe("zz");
    expect(
      [...redis.counters.keys()].filter((k) =>
        k.startsWith("site-analytics:cap:"),
      ),
    ).toHaveLength(1);
  });

  it("keeps in the visit's key how it began, its first and last hit and its exit row, and lists it as open", async () => {
    const seconds = (at: Date) => Math.floor(at.getTime() / 1000);
    await collect([pageview({ referrer: "www.google.de" })]);
    const key = visitKeys()[0];
    const member = key.slice("site-analytics:visit:".length);
    const page = ["de", PATH, "park", "mobile", "safari", "ios", "nl", "zz"];
    const visitDims = [PATH, "google", "search"];
    const hash = () => Object.fromEntries(redis.hashes.get(key) ?? []);
    expect(hash()).toEqual({
      c: JSON.stringify({
        locale: "de",
        path: PATH,
        pageType: "park",
        referrer: "google",
        channel: "search",
      }),
      n: "1",
      s: String(seconds(NOW)),
      l: String(seconds(NOW)),
      x: JSON.stringify([...page, ...visitDims]),
    });
    expect(redis.zsets.get(KEYS.openVisits)?.get(member)).toBe(seconds(NOW));

    // A later page moves the exit; an engagement or an event only the last hit.
    const later = new Date(NOW.getTime() + 10 * 60_000);
    await collect(
      [pageview({ path: "/de/news", pageType: "news_index" })],
      {},
      later,
    );
    const news = ["de", "/de/news", "news_index", ...page.slice(3)];
    expect(hash()).toMatchObject({
      n: "2",
      s: String(seconds(NOW)),
      l: String(seconds(later)),
      x: JSON.stringify([...news, ...visitDims]),
    });
    const last = new Date(later.getTime() + 60_000);
    await collect([engagement(30, { path: "/de/news" })], {}, last);
    expect(hash()).toMatchObject({
      l: String(seconds(last)),
      x: JSON.stringify([...news, ...visitDims]),
    });
    expect(redis.zsets.get(KEYS.openVisits)?.get(member)).toBe(seconds(last));
  });

  it("drops open visits older than any key from the list, flusher or not", async () => {
    redis.zsets.set(
      KEYS.openVisits,
      new Map([
        [
          "forgotten",
          Math.floor(NOW.getTime() / 1000) - OPEN_VISIT_MAX_AGE_SECONDS - 1,
        ],
        ["recent", Math.floor(NOW.getTime() / 1000) - 60],
      ]),
    );
    await collect([pageview()]);
    expect(
      [...(redis.zsets.get(KEYS.openVisits)?.keys() ?? [])].sort(),
    ).toEqual(
      ["recent", visitKeys()[0].slice("site-analytics:visit:".length)].sort(),
    );
  });

  it("adds an engagement's seconds to its page and, with a visit, to the visit's row", async () => {
    await collect([pageview({ referrer: "bing.com" })]);
    await collect([engagement(42)]);
    const page = ["de", PATH, "park", "mobile", "safari", "ios", "nl", "zz"];
    expect(buffered().pageviews).toEqual([
      {
        dims: [...page, "", "", ""],
        counts: counts({ views: 1, seconds: 42 }),
      },
      {
        dims: [...page, PATH, "bing", "search"],
        counts: counts({ visits: 1, visitSeconds: 42 }),
      },
    ]);
    // Without an address there is no visit: the page's time only.
    redis = new SiteAnalyticsRedisMock();
    service = await build();
    await collect([engagement(7)], { ip: null });
    expect(buffered().pageviews).toEqual([
      {
        dims: [
          "de",
          PATH,
          "park",
          "mobile",
          "safari",
          "ios",
          "zz",
          "zz",
          "",
          "",
          "",
        ],
        counts: counts({ seconds: 7 }),
      },
    ]);
  });

  it("drops an engagement outside 1 to 1800 whole seconds", async () => {
    await expect(
      collect([engagement(0), engagement(1801), engagement(2.5)]),
    ).resolves.toBe(0);
    expect(redis.hashes.get(KEYS.buffer(WINDOW))).toBeUndefined();
  });

  it("stores the browser's first language on page view rows, never on events", async () => {
    await service.collect(
      {
        hits: [pageview(), event("tab_changed", { tab: "map" })],
        ip: IP,
        userAgent: BROWSER,
        acceptLanguage: "fr-CH, fr;q=0.9, en;q=0.8",
      },
      NOW,
    );
    const rows = buffered();
    expect(rows.pageviews.map((row) => row.dims[7])).toEqual(["fr", "fr"]);
    expect(rows.events[0].dims).not.toContain("fr");
  });

  it("takes the device class from the User-Agent, for page views, visits, events and the live view alike", async () => {
    const IPAD =
      "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1";
    for (const userAgent of [BROWSER, IPAD, DESKTOP]) {
      await collect([pageview(), event("search_opened", { surface: "hero" })], {
        userAgent,
      });
    }
    const device = (dims: string[]) => dims[3];
    expect(
      buffered()
        .pageviews.filter((row) => row.counts.views > 0)
        .map((row) => device(row.dims))
        .sort(),
    ).toEqual(["desktop", "mobile", "tablet"]);
    expect(
      buffered()
        .events.map((row) => row.dims[4])
        .sort(),
    ).toEqual(["desktop", "mobile", "tablet"]);
    expect(
      Object.fromEntries(
        redis.hashes.get(`site-analytics:live:devices:202610282116:all`) ?? [],
      ),
    ).toEqual({ desktop: "1", mobile: "1", tablet: "1" });
    // The visit's exit row carries it too, from the same request's User-Agent.
    const exits = visitKeys().map((key) =>
      JSON.parse(redis.hashes.get(key)?.get("x") ?? "[]"),
    );
    expect(exits.map(device).sort()).toEqual(["desktop", "mobile", "tablet"]);
  });

  it("counts a hit that still sends `viewport` and `campaign`, and stores neither", async () => {
    await expect(
      collect([
        pageview({
          viewport: "desktop",
          campaign: "summer-2026",
          referrer: "www.google.de",
        }),
        engagement(12, { viewport: "desktop", campaign: "summer-2026" }),
      ]),
    ).resolves.toBe(2);
    // The User-Agent is an iPhone's: the device is the server's, not the hit's.
    expect(pageviewRows().map((row) => row.path)).toEqual([PATH, PATH]);
    const visit = buffered().pageviews.find((row) => row.counts.visits > 0)!;
    expect(visit.dims).toEqual([
      "de",
      PATH,
      "park",
      "mobile",
      "safari",
      "ios",
      "nl",
      "zz",
      PATH,
      "google",
      "search",
    ]);
    const stored = JSON.stringify([
      [...redis.hashes.values()].map((hash) => [...hash]),
      [...redis.sets.values()].map((set) => [...set]),
    ]);
    expect(stored).not.toContain("summer-2026");
    expect(stored).not.toContain("desktop");
  });

  it("counts the live view's minute: pages, countries, devices, visits by channel and referrer, events", async () => {
    await collect([
      pageview({ referrer: "www.google.de" }),
      event("search_opened", { surface: "hero" }),
    ]);
    const live = (list: string, scope = "all") =>
      Object.fromEntries(
        redis.hashes.get(`site-analytics:live:${list}:202610282116:${scope}`) ??
          [],
      );
    expect(live("pages")).toEqual({ [JSON.stringify([PATH, "park"])]: "1" });
    expect(live("pages", "de")).toEqual(live("pages"));
    expect(live("countries")).toEqual({ nl: "1" });
    expect(live("devices")).toEqual({ mobile: "1" });
    expect(live("channels")).toEqual({ search: "1" });
    expect(live("referrers")).toEqual({ google: "1" });
    expect(live("events")).toEqual({ search_opened: "1" });
    expect(redis.counters.get("site-analytics:rt:ev:202610282116:all")).toBe(1);
    expect(redis.ttls.get("site-analytics:live:events:202610282116:de")).toBe(
      LIVE_KEY_TTL_SECONDS,
    );
    // The second page view of the visit starts nothing.
    await collect([pageview({ path: "/de/news", pageType: "news_index" })]);
    expect(live("channels")).toEqual({ search: "1" });
    expect(live("countries")).toEqual({ nl: "2" });
  });

  it("reads only the first 512 characters of a User-Agent", async () => {
    const long = `${DESKTOP}${" x".repeat(2000)} Googlebot/2.1`;
    await expect(collect([pageview()], { userAgent: long })).resolves.toBe(1);
  });
});
