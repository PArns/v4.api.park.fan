import { NotFoundException } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { SiteTrafficDay } from "./entities/site-traffic-day.entity";
import {
  SiteAnalyticsService,
  daysBetween,
  rate,
  siteAnalyticsWindow,
} from "./site-analytics.service";
import { SEARCH_SURFACES, SITE_EVENT_NAMES } from "./site-analytics.schema";

const NOW = new Date("2026-10-28T07:12:40Z");
const PARK_ID = "3f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
const RIDE_ID = "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d";
const GEO = {
  continentSlug: "europe",
  countrySlug: "germany",
  citySlug: "bruehl",
  slug: "phantasialand",
};

/** A grouping-set row: the columns outside its set are NULL. */
const row = (fields: Record<string, string | null>, count: number) => ({
  name: null,
  p1: null,
  p2: null,
  p3: null,
  page_type: null,
  viewport: null,
  day: null,
  ...fields,
  count: String(count),
});

/**
 * The admin's reports. What is pinned: the window is N UTC days with today
 * and the N before it; every day, event, surface and closed value is listed
 * with zeros; rates divide by zero to zero; the grouping-set rows are told
 * apart by their NULLs; entities come with their frontend path; today's
 * visitors come from Redis; Redis down reads zero rather than failing.
 */
describe("SiteAnalyticsService", () => {
  let service: SiteAnalyticsService;
  let query: jest.Mock;
  let parksFindBy: jest.Mock;
  let attractionsFind: jest.Mock;
  let showsFind: jest.Mock;
  let redis: {
    pipeline: jest.Mock;
    pfcount: jest.Mock;
    mget: jest.Mock;
  };
  let pfcounts: Map<string, number>;

  beforeEach(async () => {
    query = jest.fn().mockResolvedValue([]);
    parksFindBy = jest.fn().mockResolvedValue([]);
    attractionsFind = jest.fn().mockResolvedValue([]);
    showsFind = jest.fn().mockResolvedValue([]);
    pfcounts = new Map();
    redis = {
      pipeline: jest.fn(() => {
        const keys: string[] = [];
        const chain = {
          pfcount: (key: string) => (keys.push(key), chain),
          exec: async () => keys.map((key) => [null, pfcounts.get(key) ?? 0]),
        };
        return chain;
      }),
      pfcount: jest.fn().mockResolvedValue(4),
      mget: jest.fn(async (...keys: string[]) =>
        keys.map((key) => (key.includes("202610280712") ? "9" : null)),
      ),
    };
    const moduleRef = await Test.createTestingModule({
      providers: [
        SiteAnalyticsService,
        { provide: getRepositoryToken(SiteTrafficDay), useValue: { query } },
        {
          provide: getRepositoryToken(Park),
          useValue: { findBy: parksFindBy },
        },
        {
          provide: getRepositoryToken(Attraction),
          useValue: { find: attractionsFind },
        },
        { provide: getRepositoryToken(Show), useValue: { find: showsFind } },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();
    service = moduleRef.get(SiteAnalyticsService);
  });

  describe("helpers", () => {
    it("makes a window of N UTC days with today, and the N days before it", () => {
      expect(siteAnalyticsWindow({ days: 30 }, NOW)).toEqual({
        from: "2026-09-29",
        to: "2026-10-28",
        days: 30,
        locale: null,
        previousFrom: "2026-08-30",
        previousTo: "2026-09-28",
      });
      expect(siteAnalyticsWindow({ days: 1, locale: "nl" }, NOW)).toMatchObject(
        {
          from: "2026-10-28",
          previousFrom: "2026-10-27",
          previousTo: "2026-10-27",
          locale: "nl",
        },
      );
    });

    it("rounds a rate to four places and divides by zero to zero", () => {
      expect(rate(1, 3)).toBe(0.3333);
      expect(rate(5, 0)).toBe(0);
      expect(rate(0, 0)).toBe(0);
    });

    it("lists every day of a range, both ends included", () => {
      expect(daysBetween("2026-10-30", "2026-11-02")).toEqual([
        "2026-10-30",
        "2026-10-31",
        "2026-11-01",
        "2026-11-02",
      ]);
    });
  });

  describe("overview", () => {
    it("sums both windows, fills every day, takes today's visitors from Redis and the realtime minute", async () => {
      query
        .mockResolvedValueOnce([
          { day: "2026-10-27", pageviews: "30", visits: "10" },
          { day: "2026-10-28", pageviews: "12", visits: "4" },
          { day: "2026-10-25", pageviews: "5", visits: "5" },
        ])
        .mockResolvedValueOnce([
          { name: "search_opened", day: "2026-10-28", count: "7" },
          { name: "tab_changed", day: "2026-10-27", count: "3" },
          { name: "tab_changed", day: "2026-10-25", count: "1" },
        ])
        .mockResolvedValueOnce([{ day: "2026-10-27", visitors: 8 }]);
      pfcounts.set("site-analytics:uv:2026-10-28:all", 3);
      pfcounts.set("site-analytics:uv:2026-10-27:all", 99);

      const overview = await service.overview({ days: 2 }, NOW);

      expect(overview).toMatchObject({
        from: "2026-10-27",
        to: "2026-10-28",
        days: 2,
        locale: null,
        totals: {
          pageviews: 42,
          visits: 14,
          visitors: 11,
          events: 10,
          viewsPerVisit: 3,
        },
        previous: {
          pageviews: 5,
          visits: 5,
          visitors: 0,
          events: 1,
          viewsPerVisit: 1,
        },
        series: [
          { day: "2026-10-27", pageviews: 30, visits: 10, visitors: 8 },
          { day: "2026-10-28", pageviews: 12, visits: 4, visitors: 3 },
        ],
      });
      // Visits are the visits started without a language, its visits with one.
      expect(query.mock.calls[0][0]).toContain(
        "THEN SUM(views) FILTER (WHERE dim = 'entry')",
      );
      expect(query.mock.calls[0][1]).toEqual([
        "2026-10-25",
        "2026-10-28",
        null,
      ]);

      expect(overview.realtime.activeVisitors).toBe(4);
      expect(redis.pfcount).toHaveBeenCalledWith(
        ...[8, 9, 10, 11, 12].map(
          (m) =>
            `site-analytics:rt:uv:2026102807${String(m).padStart(2, "0")}:all`,
        ),
      );
      expect(overview.realtime.perMinute).toHaveLength(30);
      expect(overview.realtime.perMinute[0]).toEqual({
        minute: "2026-10-28T06:43:00Z",
        pageviews: 0,
      });
      expect(overview.realtime.perMinute[29]).toEqual({
        minute: "2026-10-28T07:12:00Z",
        pageviews: 9,
      });
    });

    it("asks one language's rows and keys when given one", async () => {
      await service.overview({ days: 7, locale: "nl" }, NOW);
      expect(query.mock.calls[0][1][2]).toBe("nl");
      expect(query.mock.calls[2][1][0]).toBe("nl");
      expect(redis.mget.mock.calls[0][0]).toBe(
        "site-analytics:rt:pv:202610280643:nl",
      );
    });

    it("answers with zeros where Redis is down", async () => {
      redis.pfcount.mockRejectedValue(new Error("down"));
      redis.pipeline.mockImplementation(() => {
        throw new Error("down");
      });
      const overview = await service.overview({ days: 1 }, NOW);
      expect(overview.realtime.activeVisitors).toBe(0);
      expect(overview.realtime.perMinute.every((m) => m.pageviews === 0)).toBe(
        true,
      );
      expect(overview.totals.visitors).toBe(0);
      expect(overview.totals.viewsPerVisit).toBe(0);
    });
  });

  describe("breakdown", () => {
    it("lists values with their share of the total over all values", async () => {
      query.mockResolvedValueOnce([
        {
          value: "google.com",
          views: "6",
          visits: "6",
          total_views: "10",
          total_visits: "10",
        },
        {
          value: "bing.com",
          views: "3",
          visits: "3",
          total_views: "10",
          total_visits: "10",
        },
      ]);
      const result = await service.breakdown(
        { dim: "referrer", days: 30, limit: 2 },
        NOW,
      );
      expect(result).toEqual({
        from: "2026-09-29",
        to: "2026-10-28",
        days: 30,
        locale: null,
        dim: "referrer",
        total: { views: 10, visits: 10 },
        items: [
          { value: "google.com", views: 6, visits: 6, share: 0.6 },
          { value: "bing.com", views: 3, visits: 3, share: 0.3 },
        ],
      });
      expect(query.mock.calls[0][1]).toEqual([
        "referrer",
        "2026-09-29",
        "2026-10-28",
        null,
        2,
      ]);
    });

    it("answers an empty window with a zero total", async () => {
      const result = await service.breakdown(
        { dim: "path", days: 1, limit: 50, locale: "it" },
        NOW,
      );
      expect(result.total).toEqual({ views: 0, visits: 0 });
      expect(result.items).toEqual([]);
      expect(result.locale).toBe("it");
    });
  });

  describe("events", () => {
    it("lists every event of the schema, most counted first, with the previous window and every day", async () => {
      query.mockResolvedValueOnce([
        { name: "search_opened", day: "2026-10-28", count: "5" },
        { name: "search_opened", day: "2026-10-26", count: "2" },
        { name: "tab_changed", day: "2026-10-27", count: "9" },
      ]);
      const result = await service.events({ days: 2 }, NOW);
      expect(result.items).toHaveLength(SITE_EVENT_NAMES.length);
      expect(result.items[0]).toEqual({
        name: "tab_changed",
        count: 9,
        previous: 0,
        series: [
          { day: "2026-10-27", count: 9 },
          { day: "2026-10-28", count: 0 },
        ],
      });
      expect(result.items[1]).toMatchObject({
        name: "search_opened",
        count: 5,
        previous: 2,
      });
      // Zeros keep the schema's order.
      expect(result.items[2].name).toBe("search_typed");
      expect(result.items[result.items.length - 1]).toMatchObject({
        name: "pwa_installed",
        count: 0,
      });
    });
  });

  describe("event", () => {
    it("refuses a name the schema does not have", async () => {
      await expect(service.event("search_closed", { days: 7 })).rejects.toThrow(
        NotFoundException,
      );
      expect(query).not.toHaveBeenCalled();
    });

    it("splits the grouping sets, fills closed lists with zeros and keeps the property order", async () => {
      query.mockResolvedValueOnce([
        row({}, 10),
        row({ p1: "hero" }, 7),
        row({ p1: "header" }, 3),
        row({ p2: "park" }, 10),
        row({ p3: "1" }, 6),
        row({ p3: "11+" }, 4),
        row({ page_type: "home" }, 8),
        row({ page_type: "park" }, 2),
        row({ viewport: "mobile" }, 10),
        row({ day: "2026-10-28" }, 10),
      ]);
      const result = await service.event(
        "search_result_clicked",
        { days: 2 },
        NOW,
      );
      expect(result.total).toBe(10);
      expect(result.props.map((p) => p.prop)).toEqual([
        "surface",
        "result",
        "position",
      ]);
      expect(result.props[0].values).toHaveLength(SEARCH_SURFACES.length);
      expect(result.props[0].values.slice(0, 3)).toEqual([
        { value: "hero", count: 7 },
        { value: "header", count: 3 },
        { value: "keyboard", count: 0 },
      ]);
      expect(result.props[2].values[0]).toEqual({ value: "1", count: 6 });
      expect(result.byPageType[0]).toEqual({ value: "home", count: 8 });
      expect(result.byPageType).toHaveLength(16);
      expect(result.byViewport).toEqual([
        { value: "mobile", count: 10 },
        { value: "tablet", count: 0 },
        { value: "desktop", count: 0 },
      ]);
      expect(result.series).toEqual([
        { day: "2026-10-27", count: 0 },
        { day: "2026-10-28", count: 10 },
      ]);
      // No entity column on this event, so no second query.
      expect(query).toHaveBeenCalledTimes(1);
      expect(result.entities).toEqual([]);
    });

    it("lists a slug property's values as found, and names entities with their frontend path", async () => {
      query
        .mockResolvedValueOnce([row({ p1: "map" }, 5)])
        .mockResolvedValueOnce([
          { entity: PARK_ID, kind: "park", count: "4" },
          { entity: RIDE_ID, kind: "park", count: "1" },
        ]);
      parksFindBy.mockResolvedValueOnce([
        { id: PARK_ID, name: "Phantasialand", ...GEO },
      ]);
      const result = await service.event("tab_changed", { days: 7 }, NOW);
      expect(result.entities).toEqual([
        {
          id: PARK_ID,
          kind: "park",
          name: "Phantasialand",
          url: "/parks/europe/germany/bruehl/phantasialand",
          count: 4,
        },
        { id: RIDE_ID, kind: "park", name: null, url: null, count: 1 },
      ]);
      expect(query.mock.calls[1][1]).toEqual([
        "tab_changed",
        "2026-10-22",
        "2026-10-28",
        null,
        50,
      ]);
      expect(attractionsFind).not.toHaveBeenCalled();
    });

    it("resolves a favourite's rides and shows through their park", async () => {
      query.mockResolvedValueOnce([]).mockResolvedValueOnce([
        { entity: RIDE_ID, kind: "attraction", count: "2" },
        { entity: PARK_ID, kind: "show", count: "1" },
      ]);
      attractionsFind.mockResolvedValueOnce([
        { id: RIDE_ID, name: "Taron", slug: "taron", park: GEO },
      ]);
      showsFind.mockResolvedValueOnce([
        { id: PARK_ID, name: "Rauch", slug: "rauch", park: GEO },
      ]);
      const result = await service.event("favorite_add", { days: 7 }, NOW);
      expect(result.entities.map((e) => e.url)).toEqual([
        "/parks/europe/germany/bruehl/phantasialand/taron",
        "/parks/europe/germany/bruehl/phantasialand#shows",
      ]);
    });

    it("caps an open property's list", async () => {
      query.mockResolvedValueOnce(
        Array.from({ length: 120 }, (_, i) => row({ p1: `term-${i}` }, i)),
      );
      const result = await service.event(
        "glossary_term_viewed",
        { days: 7 },
        NOW,
      );
      expect(result.props[0].values).toHaveLength(100);
      expect(result.props[0].values[0]).toEqual({
        value: "term-119",
        count: 119,
      });
    });
  });

  describe("search", () => {
    it("puts every surface beside its rates, fills viewports, positions and days", async () => {
      query.mockResolvedValueOnce([
        row({ name: "search_opened", p1: "hero" }, 40),
        row({ name: "search_typed", p1: "hero" }, 20),
        row({ name: "search_result_clicked", p1: "hero" }, 9),
        row({ name: "search_browse_clicked", p1: "hero" }, 1),
        row({ name: "search_no_results", p1: "hero" }, 5),
        row({ name: "search_view_all", p1: "hero" }, 2),
        row({ name: "search_opened", p1: "header" }, 10),
        row({ name: "search_no_results", p1: "header" }, 1),
        row({ name: "search_opened", p1: "header", page_type: "park" }, 6),
        row({ name: "search_opened", p1: "header", page_type: "home" }, 4),
        row({ name: "search_typed", p1: "header", page_type: "home" }, 4),
        row({ name: "search_opened", p1: "hero", viewport: "mobile" }, 30),
        row({ name: "search_result_clicked", p1: "hero", p2: "park" }, 9),
        row(
          { name: "search_browse_clicked", p1: "hero", p2: "nearby_park" },
          1,
        ),
        row({ name: "search_result_clicked", p3: "1" }, 7),
        row({ name: "search_browse_clicked", p3: "1" }, 1),
        row({ name: "search_opened", p1: "hero", day: "2026-10-28" }, 40),
      ]);
      const result = await service.search({ days: 2 }, NOW);

      expect(result.surfaces).toHaveLength(7);
      expect(result.surfaces[0]).toEqual({
        surface: "hero",
        opened: 40,
        typed: 20,
        resultClicks: 9,
        browseClicks: 1,
        noResults: 5,
        viewAll: 2,
        clickRate: 0.25,
        noResultRate: 0.25,
      });
      // Typed nothing: the no-result rate divides by zero to zero.
      expect(result.surfaces[1]).toMatchObject({
        surface: "header",
        opened: 10,
        noResults: 1,
        clickRate: 0,
        noResultRate: 0,
      });
      expect(result.bySurfaceAndPageType).toEqual([
        { surface: "header", pageType: "park", opened: 6 },
        { surface: "header", pageType: "home", opened: 4 },
      ]);
      expect(result.bySurfaceAndViewport).toHaveLength(21);
      expect(result.bySurfaceAndViewport[0]).toEqual({
        surface: "hero",
        viewport: "mobile",
        opened: 30,
      });
      expect(result.resultTypes).toEqual([
        { surface: "hero", result: "park", clicks: 9 },
      ]);
      expect(result.browseItems).toEqual([
        { surface: "hero", item: "nearby_park", clicks: 1 },
      ]);
      // Positions are the result clicks', not the browse list's.
      expect(result.positions).toEqual([
        { position: "1", clicks: 7 },
        { position: "2", clicks: 0 },
        { position: "3", clicks: 0 },
        { position: "4-5", clicks: 0 },
        { position: "6-10", clicks: 0 },
        { position: "11+", clicks: 0 },
      ]);
      expect(result.series).toHaveLength(14);
      expect(result.series).toContainEqual({
        day: "2026-10-28",
        surface: "hero",
        opened: 40,
      });
      expect(result.series[0]).toEqual({
        day: "2026-10-27",
        surface: "hero",
        opened: 0,
      });
      expect(query.mock.calls[0][1][0]).toEqual([
        "search_opened",
        "search_typed",
        "search_result_clicked",
        "search_browse_clicked",
        "search_no_results",
        "search_view_all",
      ]);
    });
  });
});
