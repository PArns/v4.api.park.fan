import { z } from "zod";
import type { AnyMcpTool, McpToolContext } from "../mcp-tool";
import { analyticsOverviewTool } from "./analytics-overview.tool";
import { analyticsBreakdownTool } from "./analytics-breakdown.tool";
import { analyticsEventsTool } from "./analytics-events.tool";
import { analyticsEventTool } from "./analytics-event.tool";
import { analyticsCrosstabTool } from "./analytics-crosstab.tool";
import { searchReportTool } from "./search-report.tool";
import { seoLandingPagesTool } from "./seo-landing-pages.tool";
import { seoTrendsTool } from "./seo-trends.tool";
import { seoNotFoundTool } from "./seo-not-found.tool";
import { seoUncoveredTool } from "./seo-uncovered.tool";
import { SITE_ANALYTICS_CAVEATS, siteQuery } from "./site-analytics.inputs";

const WINDOW = {
  from: "2026-09-29",
  to: "2026-10-28",
  days: 30,
  locale: null,
  filters: [],
};

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
      days: 7,
      locale: "de",
      filter: [
        "channel:search",
        "path:/de/parks/europe/germany/bruehl/phantasialand",
      ],
    });
    expect(siteQuery({ days: 30 })).toEqual({
      days: 30,
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
    expect(schema.parse({}).days).toBe(30);
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
    };
    const previous = {
      pageviews: 100,
      visits: 50,
      visitors: 38,
      events: 0,
      viewsPerVisit: 2,
      bounceRate: 0.5,
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
      days: 30,
      locale: null,
      filter: ["channel:ai"],
    });
    expect(answer.totals).toMatchObject({ pageviews: 120, bounceRate: 0.412 });
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
  });

  it("analytics_breakdown: passes dim and limit, rounds share and bounce", async () => {
    const service = {
      breakdown: jest.fn(async () => ({
        ...WINDOW,
        dim: "referrer",
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
      expect.objectContaining({ dim: "referrer", limit: 5, days: 30 }),
    );
    expect(answer.items).toEqual([
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
    expect(answer.items).toEqual([
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
        entities: [],
        series: [],
      })),
    };
    const answer = await run(
      analyticsEventTool,
      { name: "search_result_clicked", limit: 2 },
      service,
    );
    expect(service.event).toHaveBeenCalledWith("search_result_clicked", {
      days: 30,
      locale: null,
      filter: [],
    });
    expect(answer.props).toEqual([
      {
        prop: "surface",
        values: [
          { value: "hero", count: 50 },
          { value: "header", count: 30 },
        ],
        otherCount: 20,
      },
    ]);
    expect(answer.combinations).toHaveLength(2);
    expect(answer).not.toHaveProperty("blocked");
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
    expect(answer).toMatchObject({
      rows: ["hero", "header"],
      cols: ["home", "park"],
      counts: [
        [40, 0],
        [5, 25],
      ],
      total: 70,
    });
    expect(
      z
        .object(analyticsCrosstabTool.input)
        .safeParse({ name: "x_y", row: "address", col: "day" }).success,
    ).toBe(false);
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
    expect(answer.surfaces).toEqual([
      expect.objectContaining({
        surface: "hero",
        clickRate: 0.333,
        noResultRate: 0.333,
      }),
    ]);
    expect(answer.bySurfaceAndPageType).toHaveLength(1);
    expect(answer).not.toHaveProperty("series");
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
              views: 1,
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
    expect((landing.items as Array<{ bounceRate: number }>)[0].bounceRate).toBe(
      0.556,
    );

    const trends = await run(seoTrendsTool, { channel: "ai", limit: 3 }, seo);
    expect(seo.trends).toHaveBeenCalledWith(
      expect.objectContaining({ channel: "ai" }),
    );
    expect(trends.rising).toHaveLength(3);

    const notFound = await run(seoNotFoundTool, {}, seo);
    expect(
      (notFound.items as Array<{ referrers: unknown[] }>)[0].referrers,
    ).toHaveLength(5);

    const uncovered = await run(seoUncoveredTool, {}, seo);
    expect(seo.uncovered).toHaveBeenCalledWith(
      expect.objectContaining({ kind: "park", limit: 50 }),
    );
    expect(uncovered).toMatchObject({ total: 150, uncovered: 1 });
  });
});
