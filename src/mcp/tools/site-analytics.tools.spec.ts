import { z } from "zod";
import type { AnyMcpTool, McpToolContext } from "../mcp-tool";
import { analyticsOverviewTool } from "./analytics-overview.tool";
import { analyticsBreakdownTool } from "./analytics-breakdown.tool";
import { analyticsEventsTool } from "./analytics-events.tool";
import { analyticsEventTool } from "./analytics-event.tool";
import { analyticsCrosstabTool } from "./analytics-crosstab.tool";
import { analyticsTrafficCrosstabTool } from "./analytics-traffic-crosstab.tool";
import { analyticsLiveTool } from "./analytics-live.tool";
import { searchReportTool } from "./search-report.tool";
import { seoLandingPagesTool } from "./seo-landing-pages.tool";
import { seoTrendsTool } from "./seo-trends.tool";
import { seoNotFoundTool } from "./seo-not-found.tool";
import { seoUncoveredTool } from "./seo-uncovered.tool";
import { SITE_ANALYTICS_CAVEATS, siteQuery } from "./site-analytics.inputs";
import { VISITOR_DATA_DESCRIPTION, VISITOR_DATA_NOTICE } from "./visitor-data";

const WINDOW = {
  from: "2026-09-29",
  to: "2026-10-28",
  days: 30,
  interval: "day",
  locale: null,
  filters: [],
};

/** The service query of a tool given no window: every field null. */
const NO_WINDOW = { days: null, from: null, to: null, interval: null };

/** Runs a tool the way the server does: the input parsed by its schema first. */
async function run(
  tool: AnyMcpTool,
  raw: Record<string, unknown>,
  service: Record<string, jest.Mock>,
) {
  const input = z.object(tool.input).parse(raw);
  const ctx = {
    admin: {} as McpToolContext["admin"],
    get: () => service,
  } as unknown as McpToolContext;
  return (await tool.handler(input as never, ctx)) as Record<string, unknown>;
}

/** The visitor-supplied half of an answer, after checking it is labelled. */
function visitorData(answer: Record<string, unknown>) {
  expect(answer.visitorDataNotice).toBe(VISITOR_DATA_NOTICE);
  return answer.visitorData as Record<string, unknown>;
}

describe("site statistic tools", () => {
  it("turns filters into the service's dim:value strings", () => {
    expect(
      siteQuery({
        days: 7,
        locale: "de",
        filters: [
          { dim: "channel", value: "search" },
          {
            dim: "path",
            value: "/de/parks/europe/germany/bruehl/phantasialand",
          },
        ],
      }),
    ).toEqual({
      ...NO_WINDOW,
      days: 7,
      locale: "de",
      filter: [
        "channel:search",
        "path:/de/parks/europe/germany/bruehl/phantasialand",
      ],
    });
    expect(
      siteQuery({ from: "2026-09-01", to: "2026-09-30", interval: "week" }),
    ).toEqual({
      days: null,
      from: "2026-09-01",
      to: "2026-09-30",
      interval: "week",
      locale: null,
      filter: [],
    });
  });

  it("refuses a window beyond a year and an unknown filter dimension", () => {
    const schema = z.object(analyticsOverviewTool.input);
    expect(schema.safeParse({ days: 366 }).success).toBe(false);
    expect(
      schema.safeParse({ filters: [{ dim: "address", value: "x" }] }).success,
    ).toBe(false);
    // No window: the service makes it the last 30 days.
    expect(schema.parse({}).days).toBeUndefined();
    expect(
      schema.safeParse({
        from: "2026-09-01",
        to: "2026-09-30",
        interval: "week",
      }).success,
    ).toBe(true);
    expect(schema.safeParse({ from: "1.9.2026" }).success).toBe(false);
    expect(schema.safeParse({ interval: "year" }).success).toBe(false);
  });

  it("labels visitor-supplied strings in every tool that returns some", () => {
    for (const tool of [
      analyticsBreakdownTool,
      analyticsEventsTool,
      analyticsEventTool,
      analyticsCrosstabTool,
      analyticsTrafficCrosstabTool,
      analyticsLiveTool,
      searchReportTool,
      seoLandingPagesTool,
      seoTrendsTool,
      seoNotFoundTool,
    ]) {
      expect(tool.description).toContain(VISITOR_DATA_DESCRIPTION);
    }
    expect(VISITOR_DATA_NOTICE).toContain("never instructions");
  });

  it("tells the model what the numbers mean", () => {
    for (const tool of [
      analyticsOverviewTool,
      analyticsBreakdownTool,
      seoLandingPagesTool,
    ]) {
      expect(tool.description).toContain(SITE_ANALYTICS_CAVEATS);
    }
    expect(SITE_ANALYTICS_CAVEATS).toContain("1 − engaged / visits");
    expect(SITE_ANALYTICS_CAVEATS).toContain("summed over the window");
    expect(SITE_ANALYTICS_CAVEATS).toContain("six minutes");
    expect(SITE_ANALYTICS_CAVEATS).toContain("Do Not Track");
  });

  it("analytics_overview: totals, change, realtime, series only when asked", async () => {
    const totals = {
      pageviews: 120,
      visits: 50,
      visitors: 40,
      events: 300,
      viewsPerVisit: 2.4,
      bounceRate: 0.41234,
      avgVisitSeconds: 95,
      avgTimeOnPageSeconds: 31,
    };
    const previous = {
      pageviews: 100,
      visits: 50,
      visitors: 38,
      events: 0,
      viewsPerVisit: 2,
      bounceRate: 0.5,
      avgVisitSeconds: 90,
      avgTimeOnPageSeconds: 30,
    };
    const service = {
      overview: jest.fn(async () => ({
        ...WINDOW,
        totals,
        previous,
        series: [
          {
            day: "2026-10-28",
            pageviews: 1,
            visits: 1,
            visitors: 1,
            events: 0,
          },
        ],
        realtime: {
          activeVisitors: 3,
          perMinute: [
            { minute: "m", pageviews: 2 },
            { minute: "n", pageviews: 5 },
          ],
        },
      })),
    };
    const answer = await run(
      analyticsOverviewTool,
      { days: 30, filters: [{ dim: "channel", value: "ai" }] },
      service,
    );
    expect(service.overview).toHaveBeenCalledWith({
      ...NO_WINDOW,
      days: 30,
      locale: null,
      filter: ["channel:ai"],
    });
    expect(answer.interval).toBe("day");
    expect(answer.totals).toMatchObject({
      pageviews: 120,
      bounceRate: 0.412,
      avgVisitSeconds: 95,
      avgTimeOnPageSeconds: 31,
    });
    expect(answer.change).toEqual({ pageviews: 0.2, visits: 0, events: null });
    expect(answer.realtime).toEqual({
      activeVisitors: 3,
      pageviewsLast30Minutes: 7,
    });
    expect(answer).not.toHaveProperty("series");
    const withSeries = await run(
      analyticsOverviewTool,
      { series: true },
      service,
    );
    expect(withSeries.series).toHaveLength(1);

    // Past 180 days there is no window before, so no change either; under
    // a visit filter page views and events are null and stay null.
    service.overview.mockResolvedValueOnce({
      ...WINDOW,
      totals: { ...totals, pageviews: null, events: null, viewsPerVisit: null },
      previous: null,
      series: [],
      realtime: { activeVisitors: 0, perMinute: [] },
    } as never);
    const long = await run(analyticsOverviewTool, { days: 365 }, service);
    expect(long.previous).toBeNull();
    expect(long.change).toBeNull();
    expect(long.totals).toMatchObject({ pageviews: null, events: null });
  });

  it("offers the event and page-view tools no visit dimension to filter by", () => {
    const visit = { filters: [{ dim: "channel", value: "search" }] };
    const page = { filters: [{ dim: "page_type", value: "park" }] };
    for (const tool of [
      analyticsEventsTool,
      analyticsEventTool,
      searchReportTool,
      seoNotFoundTool,
    ]) {
      const schema = z.object(tool.input);
      expect(schema.safeParse({ ...visit, name: "tab_changed" }).success).toBe(
        false,
      );
      expect(schema.safeParse({ ...page, name: "tab_changed" }).success).toBe(
        true,
      );
    }
    expect(
      z.object(analyticsCrosstabTool.input).safeParse({
        ...visit,
        name: "tab_changed",
        row: "day",
        col: "viewport",
      }).success,
    ).toBe(false);
    // Traffic tools take them: they select visits.
    expect(z.object(analyticsOverviewTool.input).safeParse(visit).success).toBe(
      true,
    );
    // Events carry no browser language; the not-found page views do.
    const language = { filters: [{ dim: "browser_language", value: "de" }] };
    expect(
      z
        .object(analyticsEventsTool.input)
        .safeParse({ ...language, name: "tab_changed" }).success,
    ).toBe(false);
    expect(z.object(seoNotFoundTool.input).safeParse(language).success).toBe(
      true,
    );
  });

  it("seo_trends: takes at most 180 days, the window before being needed", () => {
    const schema = z.object(seoTrendsTool.input);
    expect(schema.safeParse({ days: 180 }).success).toBe(true);
    expect(schema.safeParse({ days: 181 }).success).toBe(false);
  });

  it("analytics_breakdown: passes dim and limit, rounds share and bounce", async () => {
    const service = {
      breakdown: jest.fn(async () => ({
        ...WINDOW,
        dim: "referrer",
        measure: "visits",
        total: { views: 10, visits: 10 },
        items: [
          {
            value: "google",
            views: 7,
            visits: 7,
            share: 0.70004,
            bounceRate: 0.33333,
          },
        ],
      })),
    };
    const answer = await run(
      analyticsBreakdownTool,
      { dim: "referrer", limit: 5 },
      service,
    );
    expect(service.breakdown).toHaveBeenCalledWith(
      expect.objectContaining({ dim: "referrer", limit: 5, ...NO_WINDOW }),
    );
    expect(answer.measure).toBe("visits");
    expect(visitorData(answer).items).toEqual([
      { value: "google", views: 7, visits: 7, share: 0.7, bounceRate: 0.333 },
    ]);
  });

  it("analytics_events: caps the list, adds the change, drops series by default", async () => {
    const items = Array.from({ length: 5 }, (_, i) => ({
      name: `event_${i}`,
      count: 10 - i,
      previous: 5,
      blocked: i === 1,
      series: [{ day: "2026-10-28", count: 1 }],
    }));
    const service = { events: jest.fn(async () => ({ ...WINDOW, items })) };
    const answer = await run(analyticsEventsTool, { limit: 2 }, service);
    expect(answer.total).toBe(5);
    expect(visitorData(answer).items).toEqual([
      { name: "event_0", count: 10, previous: 5, change: 1 },
      { name: "event_1", count: 9, previous: 5, change: 0.8, blocked: true },
    ]);
  });

  it("analytics_event: cuts each property's values and folds the cut into otherCount", async () => {
    const service = {
      event: jest.fn(async () => ({
        ...WINDOW,
        name: "search_result_clicked",
        blocked: false,
        total: 100,
        props: [
          {
            prop: "surface",
            values: [
              { value: "hero", count: 50 },
              { value: "header", count: 30 },
              { value: "keyboard", count: 15 },
            ],
            otherCount: 5,
          },
        ],
        combinations: [
          { props: { surface: "hero" }, count: 50 },
          { props: { surface: "header" }, count: 30 },
        ],
        byPageType: [{ value: "home", count: 60 }],
        byViewport: [{ value: "mobile", count: 70 }],
        entities: [
          {
            id: "p1",
            kind: "park",
            name: "Phantasialand",
            url: "/parks/europe/germany/bruehl/phantasialand",
            count: 9,
          },
          { id: "gone", kind: null, name: null, url: null, count: 2 },
        ],
        series: [],
      })),
    };
    const answer = await run(
      analyticsEventTool,
      { name: "search_result_clicked", limit: 2 },
      service,
    );
    expect(service.event).toHaveBeenCalledWith("search_result_clicked", {
      ...NO_WINDOW,
      locale: null,
      filter: [],
    });
    expect(visitorData(answer).props).toEqual([
      {
        prop: "surface",
        values: [
          { value: "hero", count: 50 },
          { value: "header", count: 30 },
        ],
        otherCount: 20,
      },
    ]);
    expect(visitorData(answer).combinations).toHaveLength(2);
    expect(answer).not.toHaveProperty("blocked");
    // Entities are ours, resolved; the one that no longer resolves is dropped.
    expect(answer.entities).toEqual([
      expect.objectContaining({ id: "p1", name: "Phantasialand" }),
    ]);
    expect(answer.unresolvedEntities).toBe(1);
    expect(
      z.object(analyticsEventTool.input).safeParse({ name: "Bad Name" })
        .success,
    ).toBe(false);
  });

  it("analytics_crosstab: answers a matrix", async () => {
    const service = {
      crosstab: jest.fn(async () => ({
        ...WINDOW,
        name: "search_opened",
        row: "prop:surface",
        col: "page_type",
        rows: ["hero", "header"],
        cols: ["home", "park"],
        cells: [
          { row: "hero", col: "home", count: 40 },
          { row: "header", col: "park", count: 25 },
          { row: "header", col: "home", count: 5 },
        ],
        total: 70,
      })),
    };
    const answer = await run(
      analyticsCrosstabTool,
      { name: "search_opened", row: "prop:surface", col: "page_type" },
      service,
    );
    expect(service.crosstab).toHaveBeenCalledWith(
      "search_opened",
      expect.objectContaining({ row: "prop:surface", col: "page_type" }),
    );
    expect(answer.total).toBe(70);
    expect(visitorData(answer)).toEqual({
      rows: ["hero", "header"],
      cols: ["home", "park"],
      counts: [
        [40, 0],
        [5, 25],
      ],
    });
    expect(
      z
        .object(analyticsCrosstabTool.input)
        .safeParse({ name: "x_y", row: "address", col: "day" }).success,
    ).toBe(false);
  });

  it("analytics_traffic_crosstab: answers a matrix of page views, visits on request", async () => {
    const service = {
      trafficCrosstab: jest.fn(async () => ({
        ...WINDOW,
        metric: "visits",
        row: "channel",
        col: "exit",
        rows: ["search", "direct"],
        cols: ["/de", "/de/news"],
        cells: [
          { row: "search", col: "/de", count: 4 },
          { row: "direct", col: "/de/news", count: 1 },
        ],
        total: 5,
      })),
    };
    const answer = await run(
      analyticsTrafficCrosstabTool,
      { metric: "visits", row: "channel", col: "exit" },
      service,
    );
    expect(service.trafficCrosstab).toHaveBeenCalledWith(
      expect.objectContaining({
        metric: "visits",
        row: "channel",
        col: "exit",
      }),
    );
    expect(answer).toMatchObject({ metric: "visits", total: 5 });
    expect(visitorData(answer)).toEqual({
      rows: ["search", "direct"],
      cols: ["/de", "/de/news"],
      counts: [
        [4, 0],
        [0, 1],
      ],
    });
    const schema = z.object(analyticsTrafficCrosstabTool.input);
    expect(schema.parse({ row: "os", col: "viewport" }).metric).toBe(
      "pageviews",
    );
    expect(schema.safeParse({ row: "prop:x", col: "os" }).success).toBe(false);
  });

  it("analytics_live: the visitor-supplied lists under visitorData", async () => {
    const live = {
      at: "2026-10-28T12:34:56Z",
      activeVisitors: 3,
      perMinute: [
        {
          minute: "2026-10-28T12:34:00Z",
          pageviews: 2,
          events: 1,
          visitors: 2,
        },
      ],
      pages: [{ path: "/de", pageType: "home", views: 2 }],
      countries: [{ country: "DE", views: 2 }],
      channels: [{ channel: "search", visits: 1 }],
      referrers: [{ referrer: "google", visits: 1 }],
      devices: [{ viewport: "mobile", views: 2 }],
      events: [{ name: "search_opened", count: 1 }],
    };
    const service = { live: jest.fn(async () => live) };
    const answer = await run(analyticsLiveTool, { locale: "de" }, service);
    expect(service.live).toHaveBeenCalledWith("de");
    expect(answer).toMatchObject({
      at: live.at,
      activeVisitors: 3,
      countries: live.countries,
      channels: live.channels,
      devices: live.devices,
    });
    expect(visitorData(answer)).toEqual({
      pages: live.pages,
      referrers: live.referrers,
      events: live.events,
    });
  });

  it("search_report: rounds the rates and caps the lists", async () => {
    const service = {
      search: jest.fn(async () => ({
        ...WINDOW,
        surfaces: [
          {
            surface: "hero",
            opened: 3,
            typed: 3,
            resultClicks: 1,
            browseClicks: 0,
            noResults: 1,
            viewAll: 0,
            clickRate: 0.33333,
            noResultRate: 0.33333,
          },
        ],
        bySurfaceAndPageType: [
          { surface: "hero", pageType: "home", opened: 3 },
          { surface: "hero", pageType: "park", opened: 1 },
        ],
        bySurfaceAndViewport: [],
        resultTypes: [],
        positions: [],
        browseItems: [],
        series: [{ day: "2026-10-28", surface: "hero", opened: 3 }],
      })),
    };
    const answer = await run(searchReportTool, { limit: 1 }, service);
    expect(visitorData(answer).surfaces).toEqual([
      expect.objectContaining({
        surface: "hero",
        clickRate: 0.333,
        noResultRate: 0.333,
      }),
    ]);
    expect(visitorData(answer).bySurfaceAndPageType).toHaveLength(1);
    expect(visitorData(answer)).not.toHaveProperty("series");
  });

  it("seo tools pass channel, kind and limit, and default to search and parks", async () => {
    const seo = {
      landingPages: jest.fn(async () => ({
        ...WINDOW,
        channel: "search",
        items: [
          {
            path: "/de",
            pageType: "home",
            visits: 9,
            previous: 3,
            change: 6,
            bounceRate: 0.55555,
            sources: [],
          },
        ],
      })),
      trends: jest.fn(async () => ({
        ...WINDOW,
        channel: "ai",
        rising: Array.from({ length: 25 }, (_, i) => ({
          path: `/en/${i}`,
          pageType: "park",
          visits: 20,
          previous: 1,
          change: 19,
        })),
        falling: [],
      })),
      notFound: jest.fn(async () => ({
        ...WINDOW,
        items: [
          {
            path: "/de/x",
            views: 4,
            referrers: Array.from({ length: 8 }, (_, i) => ({
              value: `site${i}`,
              visits: 1,
            })),
          },
        ],
      })),
      uncovered: jest.fn(async () => ({
        ...WINDOW,
        kind: "park",
        total: 150,
        uncovered: 1,
        items: [
          {
            id: "p",
            kind: "park",
            name: "Phantasialand",
            url: "/parks/europe/germany/bruehl/phantasialand",
          },
        ],
      })),
    };
    const landing = await run(seoLandingPagesTool, {}, seo);
    expect(seo.landingPages).toHaveBeenCalledWith(
      expect.objectContaining({ channel: "search", limit: 25 }),
    );
    expect(
      (visitorData(landing).items as Array<{ bounceRate: number }>)[0]
        .bounceRate,
    ).toBe(0.556);

    const trends = await run(seoTrendsTool, { channel: "ai", limit: 3 }, seo);
    expect(seo.trends).toHaveBeenCalledWith(
      expect.objectContaining({ channel: "ai" }),
    );
    expect(visitorData(trends).rising).toHaveLength(3);

    const notFound = await run(seoNotFoundTool, {}, seo);
    expect(
      (visitorData(notFound).items as Array<{ referrers: unknown[] }>)[0]
        .referrers,
    ).toHaveLength(5);

    const uncovered = await run(seoUncoveredTool, {}, seo);
    expect(seo.uncovered).toHaveBeenCalledWith(
      expect.objectContaining({ kind: "park", limit: 50 }),
    );
    expect(uncovered).toMatchObject({ total: 150, uncovered: 1 });
  });
});
