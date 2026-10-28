import { Test } from "@nestjs/testing";
import { getDataSourceToken, getRepositoryToken } from "@nestjs/typeorm";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { SiteAnalyticsCollectService } from "../site-analytics/site-analytics-collect.service";
import { PageViewsService } from "./page-views.service";
import { isBotUserAgent } from "./bot-user-agent.util";

const BROWSER =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15";
const PHONE =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1";
const NOW = new Date("2026-10-27T21:15:00Z");
const PARK_ID = "3f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";

/**
 * The "most read" rankings, served from the site statistic's `page_read`
 * events. What is pinned: the deprecated write records a `page_read` with the
 * post's key or the park's id, through the collector that holds the guards;
 * every ranking is per language, the admin's optionally; a window of N days
 * is N UTC days with today among them; a merged park's reads count under the
 * survivor (the SQL joins `site_entity_merges`).
 */
describe("PageViewsService", () => {
  let service: PageViewsService;
  let query: jest.Mock;
  let parksFindBy: jest.Mock;
  let attractionsFind: jest.Mock;
  let record: jest.Mock;

  beforeEach(async () => {
    query = jest.fn().mockResolvedValue([]);
    parksFindBy = jest.fn().mockResolvedValue([]);
    attractionsFind = jest.fn().mockResolvedValue([]);
    record = jest.fn().mockResolvedValue(1);
    const moduleRef = await Test.createTestingModule({
      providers: [
        PageViewsService,
        { provide: getDataSourceToken(), useValue: { query } },
        {
          provide: getRepositoryToken(Park),
          useValue: { findBy: parksFindBy },
        },
        {
          provide: getRepositoryToken(Attraction),
          useValue: { find: attractionsFind },
        },
        { provide: SiteAnalyticsCollectService, useValue: { record } },
      ],
    }).compile();
    service = moduleRef.get(PageViewsService);
  });

  describe("record", () => {
    it("records a post's read as page_read with its key, through the collector's guards", async () => {
      await expect(
        service.record(
          {
            kind: "news",
            key: "europa-park-voltron-2026",
            locale: "de",
            ip: "203.0.113.7",
            userAgent: BROWSER,
          },
          NOW,
        ),
      ).resolves.toBe(true);
      const [[hit], ip, userAgent, now] = record.mock.calls[0];
      expect(hit).toMatchObject({
        t: "event",
        name: "page_read",
        locale: "de",
        pageType: "news",
        props: { key: "europa-park-voltron-2026", kind: "news" },
        propsKey: "key=europa-park-voltron-2026&kind=news",
        entity: null,
      });
      expect([ip, userAgent, now]).toEqual(["203.0.113.7", BROWSER, NOW]);
    });

    it("records a park's read with its id as the entity", async () => {
      await service.record(
        {
          kind: "park",
          key: PARK_ID,
          locale: "nl",
          ip: null,
          userAgent: PHONE,
        },
        NOW,
      );
      expect(record.mock.calls[0][0][0]).toMatchObject({
        props: { kind: "park" },
        propsKey: "kind=park",
        entity: PARK_ID,
      });
    });

    it("answers false for a read the collector did not count", async () => {
      record.mockResolvedValueOnce(0);
      await expect(
        service.record(
          {
            kind: "article",
            key: "a-guide",
            locale: "en",
            ip: null,
            userAgent: BROWSER,
          },
          NOW,
        ),
      ).resolves.toBe(false);
      expect(record.mock.calls[0][0][0].pageType).toBe("guide");
    });
  });

  describe("top", () => {
    it("asks one language over N UTC days with today included, merged parks under their survivor", async () => {
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
      expect(sql).toContain("e.name = 'page_read'");
      expect(sql).toContain("LEFT JOIN site_entity_merges m");
      expect(sql).toContain("e.props->>'kind' = $2 AND e.locale = $3");
      expect(params).toEqual(["2026-09-28T00:00:00Z", "article", "nl", 5]);
    });

    it("makes a one-day window today alone", () => {
      expect(service.windowStart(1, NOW)).toBe("2026-10-27");
      expect(service.windowStart(7, NOW)).toBe("2026-10-21");
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
      expect(params).toEqual(["2026-09-28T00:00:00Z", "nl", 25]);
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
