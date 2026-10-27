import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { PageViewDay } from "./entities/page-view-day.entity";
import { PageViewsService } from "./page-views.service";
import { isBotUserAgent } from "./bot-user-agent.util";

const BROWSER =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15";
const NOW = new Date("2026-10-27T21:15:00Z");

/**
 * The counter behind the "most read" lists. What is pinned: a bot or a fourth
 * view from one address never reaches the table, the address is written
 * neither to the database nor into a Redis key, the cap's key depends on the
 * secret and dies with its UTC day, a Redis outage still counts, every ranking
 * is per language, and a window of N days is N UTC days with today among them.
 */
describe("PageViewsService", () => {
  let service: PageViewsService;
  let query: jest.Mock;
  let parksFindBy: jest.Mock;
  let attractionsFind: jest.Mock;
  let counters: Map<string, number>;
  let incr: jest.Mock;
  let expire: jest.Mock;

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
        { provide: REDIS_CLIENT, useValue: { incr, expire } },
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
    incr = jest.fn(async (key: string) => {
      counters.set(key, (counters.get(key) ?? 0) + 1);
      return counters.get(key);
    });
    expire = jest.fn().mockResolvedValue(1);
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

    it("counts one address at most three times per page and day", async () => {
      const results: boolean[] = [];
      for (let i = 0; i < 5; i++)
        results.push(await service.record(view(), NOW));
      expect(results).toEqual([true, true, true, false, false]);
      expect(query).toHaveBeenCalledTimes(3);
    });

    it("gives every page, language, address and day its own cap", async () => {
      for (let i = 0; i < 3; i++) await service.record(view(), NOW);
      await expect(service.record(view({ locale: "en" }), NOW)).resolves.toBe(
        true,
      );
      await expect(
        service.record(view({ key: "another-post" }), NOW),
      ).resolves.toBe(true);
      await expect(
        service.record(view({ ip: "198.51.100.4" }), NOW),
      ).resolves.toBe(true);
      await expect(
        service.record(view(), new Date("2026-10-28T00:05:00Z")),
      ).resolves.toBe(true);
    });

    it("writes the address neither to the database nor into a key", async () => {
      await service.record(view(), NOW);
      expect(JSON.stringify(query.mock.calls)).not.toContain("203.0.113.7");
      const key: string = incr.mock.calls[0][0];
      expect(key).toMatch(/^page-views:seen:[0-9a-f]{32}$/);
      expect(key).not.toContain("203.0.113.7");
    });

    it("keys the cap on the secret, the same for every instance holding it", async () => {
      await service.record(view(), NOW);
      const other = await build();
      await other.record(view(), NOW);
      expect(incr.mock.calls[1][0]).toBe(incr.mock.calls[0][0]);

      process.env.PAGE_VIEWS_HASH_SECRET = "another-secret";
      await (await build()).record(view(), NOW);
      expect(incr.mock.calls[2][0]).not.toBe(incr.mock.calls[0][0]);
    });

    it("lets the key expire a minute after its UTC day ends", async () => {
      await service.record(view(), NOW);
      // 21:15:00 UTC → 2 h 45 min to midnight, plus the 60 s grace.
      expect(expire).toHaveBeenCalledWith(
        expect.any(String),
        2 * 3600 + 45 * 60 + 60,
        "NX",
      );
    });

    it("still counts when Redis is down", async () => {
      incr.mockRejectedValueOnce(new Error("Redis down"));
      await expect(service.record(view(), NOW)).resolves.toBe(true);
      expect(query).toHaveBeenCalledTimes(1);
    });

    it("counts a view without an address, uncapped", async () => {
      for (let i = 0; i < 5; i++) {
        await expect(service.record(view({ ip: null }), NOW)).resolves.toBe(
          true,
        );
      }
      expect(incr).not.toHaveBeenCalled();
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

  describe("adminOverview", () => {
    it("sums per kind and per language, ranks across languages, and names parks", async () => {
      query
        .mockResolvedValueOnce([
          { kind: "park", key: "p-1", locale: "de", views: "40" },
          { kind: "park", key: "p-1", locale: "nl", views: "5" },
          { kind: "park", key: "p-2", locale: "en", views: "30" },
          { kind: "news", key: "n-1", locale: "de", views: "9" },
        ])
        .mockResolvedValueOnce([
          { day: "2026-10-26", kind: "park", views: "50" },
          { day: "2026-10-27", kind: "park", views: "25" },
          { day: "2026-10-27", kind: "news", views: "9" },
        ]);
      parksFindBy.mockResolvedValueOnce([
        {
          id: "p-1",
          name: "Efteling",
          slug: "efteling",
          city: "Kaatsheuvel",
          country: "Netherlands",
          continentSlug: "europe",
          countrySlug: "netherlands",
          citySlug: "kaatsheuvel",
        },
      ]);

      const overview = await service.adminOverview(30, 10, NOW);

      const parks = overview.kinds.find((kind) => kind.kind === "park")!;
      expect(parks.total).toBe(75);
      expect(parks.byLocale).toEqual({ de: 40, nl: 5, en: 30 });
      expect(parks.top.map((entry) => entry.key)).toEqual(["p-1", "p-2"]);
      expect(parks.top[0]).toMatchObject({
        total: 45,
        byLocale: { de: 40, nl: 5 },
        name: "Efteling",
        context: "Kaatsheuvel, Netherlands",
      });
      expect(parks.top[0].url).toContain(
        "/parks/europe/netherlands/kaatsheuvel/efteling",
      );
      // An id that no longer resolves keeps its row, unnamed.
      expect(parks.top[1].name).toBeUndefined();

      const news = overview.kinds.find((kind) => kind.kind === "news")!;
      expect(news.top).toEqual([{ key: "n-1", total: 9, byLocale: { de: 9 } }]);
      expect(
        overview.kinds.find((kind) => kind.kind === "article")!.total,
      ).toBe(0);

      expect(overview.from).toBe("2026-09-28");
      expect(overview.daily).toEqual([
        { day: "2026-10-26", byKind: { park: 50 } },
        { day: "2026-10-27", byKind: { park: 25, news: 9 } },
      ]);
    });
  });
});

describe("isBotUserAgent", () => {
  it("lets ordinary browsers through", () => {
    for (const userAgent of [
      BROWSER,
      "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
    ]) {
      expect(isBotUserAgent(userAgent)).toBe(false);
    }
  });
});
