import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { PageViewDay } from "./entities/page-view-day.entity";
import {
  PAGE_VIEW_STORAGE_OPTIONS,
  PageViewsService,
} from "./page-views.service";
import { isBotUserAgent } from "./bot-user-agent.util";
import { MAX_VIEWS_PER_ADDRESS_PER_DAY } from "./page-view.constants";

const BROWSER =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15";
const PHONE =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1";
const NOW = new Date("2026-10-27T21:15:00Z");
const PARK_ID = "3f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";

/**
 * The counter behind the "most read" lists. What is pinned: a bot, a fourth
 * view from one reader or a view past an address's daily budget never reaches
 * the table; a reader is an IPv6 /64 and a User-Agent, never the language; the
 * address is written neither to the database nor into a Redis key; the keys
 * depend on the secret and die with their UTC day; a Redis outage still
 * counts; every ranking is per language, the admin's optionally; a window of
 * N days is N UTC days with today among them.
 */
describe("PageViewsService", () => {
  let service: PageViewsService;
  let query: jest.Mock;
  let parksFindBy: jest.Mock;
  let attractionsFind: jest.Mock;
  let counters: Map<string, number>;
  let commands: Array<[string, ...unknown[]]>;
  let failRedis: boolean;

  /** A pipeline that runs INCR/EXPIRE against an in-memory map. */
  const redis = {
    pipeline: () => {
      const queued: Array<[string, ...unknown[]]> = [];
      const chain = {
        incr: (key: string) => (queued.push(["incr", key]), chain),
        expire: (key: string, ttl: number, mode: string) => (
          queued.push(["expire", key, ttl, mode]),
          chain
        ),
        exec: async () => {
          if (failRedis) throw new Error("Redis down");
          commands.push(...queued);
          return queued.map(([command, key]) => {
            if (command !== "incr") return [null, 1];
            counters.set(key as string, (counters.get(key as string) ?? 0) + 1);
            return [null, counters.get(key as string)];
          });
        },
      };
      return chain;
    },
  };
  const incrKeys = () =>
    commands.filter(([c]) => c === "incr").map(([, key]) => key as string);

  const build = async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [
        PageViewsService,
        { provide: getRepositoryToken(PageViewDay), useValue: { query } },
        {
          provide: getRepositoryToken(Park),
          useValue: { findBy: parksFindBy },
        },
        {
          provide: getRepositoryToken(Attraction),
          useValue: { find: attractionsFind },
        },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();
    return moduleRef.get(PageViewsService);
  };

  const view = (
    overrides: Partial<Parameters<PageViewsService["record"]>[0]> = {},
  ) => ({
    kind: "news" as const,
    key: "europa-park-voltron-2026",
    locale: "de" as const,
    ip: "203.0.113.7",
    userAgent: BROWSER,
    ...overrides,
  });

  beforeEach(async () => {
    process.env.PAGE_VIEWS_HASH_SECRET = "test-secret";
    query = jest.fn().mockResolvedValue([]);
    parksFindBy = jest.fn().mockResolvedValue([]);
    attractionsFind = jest.fn().mockResolvedValue([]);
    counters = new Map();
    commands = [];
    failRedis = false;
    service = await build();
  });

  afterEach(() => {
    delete process.env.PAGE_VIEWS_HASH_SECRET;
  });

  describe("record", () => {
    it("upserts one view on the reader's UTC day, in the reader's language", async () => {
      await expect(service.record(view(), NOW)).resolves.toBe(true);
      expect(query).toHaveBeenCalledTimes(1);
      const [sql, params] = query.mock.calls[0];
      expect(sql).toContain("ON CONFLICT (kind, key, locale, day)");
      expect(params).toEqual([
        "news",
        "europa-park-voltron-2026",
        "de",
        "2026-10-27",
      ]);
    });

    it("does not count a crawler, a headless browser or a missing User-Agent", async () => {
      for (const userAgent of [
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/131.0 Safari/537.36",
        "curl/8.5.0",
        null,
        "",
      ]) {
        await expect(service.record(view({ userAgent }), NOW)).resolves.toBe(
          false,
        );
      }
      expect(query).not.toHaveBeenCalled();
    });

    it("counts one reader at most three times per page and day", async () => {
      const results: boolean[] = [];
      for (let i = 0; i < 5; i++)
        results.push(await service.record(view(), NOW));
      expect(results).toEqual([true, true, true, false, false]);
      expect(query).toHaveBeenCalledTimes(3);
    });

    it("gives every page, browser, address and day its own cap, but not every language", async () => {
      for (let i = 0; i < 3; i++) await service.record(view(), NOW);
      await expect(service.record(view({ locale: "en" }), NOW)).resolves.toBe(
        false,
      );
      await expect(
        service.record(view({ key: "another-post" }), NOW),
      ).resolves.toBe(true);
      await expect(
        service.record(view({ userAgent: PHONE }), NOW),
      ).resolves.toBe(true);
      await expect(
        service.record(view({ ip: "198.51.100.4" }), NOW),
      ).resolves.toBe(true);
      await expect(
        service.record(view(), new Date("2026-10-28T00:05:00Z")),
      ).resolves.toBe(true);
    });

    it("treats a whole IPv6 /64 as one address", async () => {
      for (let i = 0; i < 3; i++) {
        await service.record(view({ ip: `2001:db8:1:2::${i + 1}` }), NOW);
      }
      await expect(
        service.record(view({ ip: "2001:db8:1:2:ffff::9" }), NOW),
      ).resolves.toBe(false);
      await expect(
        service.record(view({ ip: "2001:db8:1:3::1" }), NOW),
      ).resolves.toBe(true);
    });

    it("stops an address at its daily budget, whatever pages it names", async () => {
      const results: boolean[] = [];
      for (let i = 0; i < MAX_VIEWS_PER_ADDRESS_PER_DAY + 2; i++) {
        results.push(await service.record(view({ key: `post-${i}` }), NOW));
      }
      expect(results.filter(Boolean)).toHaveLength(
        MAX_VIEWS_PER_ADDRESS_PER_DAY,
      );
      expect(results.slice(-2)).toEqual([false, false]);
    });

    it("writes the address neither to the database nor into a key", async () => {
      await service.record(view(), NOW);
      expect(JSON.stringify(query.mock.calls)).not.toContain("203.0.113.7");
      const [reader, address] = incrKeys();
      expect(reader).toMatch(/^page-views:seen:[0-9a-f]{32}$/);
      expect(address).toMatch(/^page-views:addr:[0-9a-f]{32}$/);
      expect(JSON.stringify(commands)).not.toContain("203.0.113.7");
    });

    it("keys the caps on the secret, the same for every instance holding it", async () => {
      await service.record(view(), NOW);
      const other = await build();
      await other.record(view(), NOW);
      const keys = incrKeys();
      expect(keys[2]).toBe(keys[0]);

      process.env.PAGE_VIEWS_HASH_SECRET = "another-secret";
      await (await build()).record(view(), NOW);
      expect(incrKeys()[4]).not.toBe(keys[0]);
    });

    it("lets both keys expire a minute after their UTC day ends", async () => {
      await service.record(view(), NOW);
      // 21:15:00 UTC → 2 h 45 min to midnight, plus the 60 s grace.
      const expires = commands.filter(([c]) => c === "expire");
      expect(expires).toHaveLength(2);
      for (const [, , ttl, mode] of expires) {
        expect(ttl).toBe(2 * 3600 + 45 * 60 + 60);
        expect(mode).toBe("NX");
      }
    });

    it("still counts when Redis is down", async () => {
      failRedis = true;
      await expect(service.record(view(), NOW)).resolves.toBe(true);
      expect(query).toHaveBeenCalledTimes(1);
    });

    it("counts a view without an address, uncapped", async () => {
      for (let i = 0; i < 5; i++) {
        await expect(service.record(view({ ip: null }), NOW)).resolves.toBe(
          true,
        );
      }
      expect(commands).toEqual([]);
    });
  });

  describe("top", () => {
    it("asks one language over N UTC days with today included", async () => {
      query.mockResolvedValueOnce([
        { key: "b", views: "12" },
        { key: "a", views: 7 },
      ]);
      const items = await service.top("article", "nl", 30, 5, NOW);
      expect(items).toEqual([
        { key: "b", views: 12 },
        { key: "a", views: 7 },
      ]);
      const [sql, params] = query.mock.calls[0];
      expect(sql).toContain("locale = $2");
      expect(params).toEqual(["article", "nl", "2026-09-28", 5]);
    });

    it("makes a one-day window today alone", () => {
      expect(service.windowStart(1, NOW)).toBe("2026-10-27");
      expect(service.windowStart(7, NOW)).toBe("2026-10-21");
    });
  });

  describe("onModuleInit", () => {
    it("writes the storage options a fresh table lacks", async () => {
      query.mockResolvedValueOnce([{ reloptions: null }]);
      await service.onModuleInit();
      expect(query).toHaveBeenLastCalledWith(
        "ALTER TABLE page_view_days SET (fillfactor=80, autovacuum_vacuum_scale_factor=0.01)",
      );
    });

    it("leaves a table alone that has them", async () => {
      query.mockResolvedValueOnce([
        {
          reloptions: [...PAGE_VIEW_STORAGE_OPTIONS, "toast_tuple_target=128"],
        },
      ]);
      await service.onModuleInit();
      expect(query).toHaveBeenCalledTimes(1);
    });

    it("does not stop the start when it cannot", async () => {
      query.mockRejectedValueOnce(new Error("relation does not exist"));
      await expect(service.onModuleInit()).resolves.toBeUndefined();
    });
  });

  describe("adminOverview", () => {
    const totals = [
      { kind: "park", locale: "de", views: "40" },
      { kind: "park", locale: "nl", views: "5" },
      { kind: "park", locale: "en", views: "30" },
      { kind: "news", locale: "de", views: "9" },
    ];
    const topRows = [
      { kind: "news", key: "n-1", locale: "de", views: "9", rank: "1" },
      { kind: "park", key: PARK_ID, locale: "de", views: "40", rank: "1" },
      { kind: "park", key: PARK_ID, locale: "nl", views: "5", rank: "1" },
      { kind: "park", key: "junk", locale: "en", views: "30", rank: "2" },
    ];

    it("sums per kind and language, keeps the SQL's ranking, names parks and fills every day", async () => {
      query
        .mockResolvedValueOnce(totals)
        .mockResolvedValueOnce(topRows)
        .mockResolvedValueOnce([
          { day: "2026-10-26", kind: "park", views: "50" },
          { day: "2026-10-27", kind: "park", views: "25" },
          { day: "2026-10-27", kind: "news", views: "9" },
        ]);
      parksFindBy.mockResolvedValueOnce([
        {
          id: PARK_ID,
          name: "Efteling",
          slug: "efteling",
          city: "Kaatsheuvel",
          country: "Netherlands",
          continentSlug: "europe",
          countrySlug: "netherlands",
          citySlug: "kaatsheuvel",
        },
      ]);

      const overview = await service.adminOverview(3, 10, undefined, NOW);

      const parks = overview.kinds.find((kind) => kind.kind === "park")!;
      expect(parks.total).toBe(75);
      expect(parks.byLocale).toEqual({ de: 40, nl: 5, en: 30 });
      expect(parks.top.map((entry) => entry.key)).toEqual([PARK_ID, "junk"]);
      expect(parks.top[0]).toMatchObject({
        total: 45,
        byLocale: { de: 40, nl: 5 },
        name: "Efteling",
        context: "Kaatsheuvel, Netherlands",
      });
      expect(parks.top[0].url).toContain(
        "/parks/europe/netherlands/kaatsheuvel/efteling",
      );
      // A key that is no UUID never reaches the uuid column's IN (…), and keeps its row unnamed.
      expect(parksFindBy.mock.calls[0][0].id.value).toEqual([PARK_ID]);
      expect(parks.top[1].name).toBeUndefined();

      const news = overview.kinds.find((kind) => kind.kind === "news")!;
      expect(news.top).toEqual([{ key: "n-1", total: 9, byLocale: { de: 9 } }]);
      expect(
        overview.kinds.find((kind) => kind.kind === "article")!.total,
      ).toBe(0);

      expect(overview.from).toBe("2026-10-25");
      expect(overview.locale).toBeUndefined();
      expect(overview.daily).toEqual([
        { day: "2026-10-25", byKind: {} },
        { day: "2026-10-26", byKind: { park: 50 } },
        { day: "2026-10-27", byKind: { park: 25, news: 9 } },
      ]);
    });

    it("ranks by one language when asked, in SQL", async () => {
      query.mockResolvedValue([]);
      const overview = await service.adminOverview(30, 25, "nl", NOW);
      const [sql, params] = query.mock.calls[1];
      expect(sql).toContain("FILTER (WHERE $2::text IS NULL OR locale = $2)");
      expect(sql).toContain("DENSE_RANK() OVER (PARTITION BY kind");
      expect(params).toEqual(["2026-09-28", "nl", 25]);
      expect(overview.locale).toBe("nl");
    });
  });
});

describe("isBotUserAgent", () => {
  it("lets ordinary browsers through, CUBOT and Robot phones included", () => {
    for (const userAgent of [
      BROWSER,
      PHONE,
      "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
      "Mozilla/5.0 (Linux; Android 11; CUBOT X50) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
      "Mozilla/5.0 (Linux; Android 10; ROBOT 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Mobile Safari/537.36",
    ]) {
      expect(isBotUserAgent(userAgent)).toBe(false);
    }
  });

  it("drops crawlers, Google's test tools, AI agents and test runners", () => {
    for (const userAgent of [
      "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
      "AdsBot-Google (+http://www.google.com/adsbot.html)",
      "Mozilla/5.0 (compatible; Google-InspectionTool/1.0;)",
      "Mozilla/5.0 (compatible; GoogleOther)",
      "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; ChatGPT-User/1.0; +https://openai.com/bot",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 PTST/231015.172312",
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Cypress/13.6.0 Chrome/118.0.5993.117 Electron/27.0.3 Safari/537.36",
      "Twitterbot/1.0",
    ]) {
      expect(isBotUserAgent(userAgent)).toBe(true);
    }
  });
});
