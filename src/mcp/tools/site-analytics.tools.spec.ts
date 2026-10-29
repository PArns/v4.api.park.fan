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
import { plannerReportTool } from "./planner-report.tool";
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
const NO_WINDOW = {
  days: null,
  from: null,
  to: null,
  interval: null,
  datacenter: null,
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
      datacenter: null,
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
        col: "device",
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

  it("analytics_breakdown: carries each value's visitors, and a value with few visitors is folded however many views it has", async () => {
    const service = {
      breakdown: jest.fn(async () => ({
        ...WINDOW,
        dim: "country",
        measure: "views",
        total: { views: 100, visits: 41 },
        items: [
          { value: "de", views: 70, visits: 30, share: 0.7, visitors: 20 },
          { value: "nl", views: 25, visits: 10, share: 0.25, visitors: 3 },
          { value: "is", views: 5, visits: 1, share: 0.05, visitors: 1 },
        ],
      })),
    };
    const answer = await run(
      analyticsBreakdownTool,
      { dim: "country", limit: 5 },
      service,
    );
    // nl has 25 views and 3 visitors: the visitors decide, and add up in (other).
    expect(visitorData(answer).items).toEqual([
      { value: "de", views: 70, visits: 30, share: 0.7, visitors: 20 },
      { value: "(other)", views: 30, visits: 11, share: 0.3, visitors: 4 },
    ]);
    expect(analyticsBreakdownTool.description).toContain(
      "also gives each value's visitors, which then decide whether a value is small",
    );
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
        byDevice: [{ value: "mobile", count: 70 }],
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
      other: null,
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
          { row: "search", col: "/de", count: 40 },
          { row: "direct", col: "/de/news", count: 12 },
        ],
        total: 52,
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
    expect(answer).toMatchObject({ metric: "visits", total: 52 });
    expect(visitorData(answer)).toEqual({
      rows: ["search", "direct"],
      cols: ["/de", "/de/news"],
      counts: [
        [40, 0],
        [0, 12],
      ],
      other: null,
    });
    const schema = z.object(analyticsTrafficCrosstabTool.input);
    expect(schema.parse({ row: "os", col: "device" }).metric).toBe("pageviews");
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
      pages: [{ path: "/de", pageType: "home", views: 20 }],
      countries: [{ country: "DE", views: 20 }],
      channels: [{ channel: "search", visits: 11 }],
      referrers: [{ referrer: "google", visits: 11 }],
      devices: [{ device: "mobile", views: 20 }],
      events: [{ name: "search_opened", count: 15 }],
    };
    const service = { live: jest.fn(async () => live) };
    const answer = await run(analyticsLiveTool, { locale: "de" }, service);
    expect(service.live).toHaveBeenCalledWith(
      "de",
      expect.any(Date),
      "exclude",
    );
    await run(analyticsLiveTool, { datacenter: "only" }, service);
    expect(service.live).toHaveBeenLastCalledWith(
      null,
      expect.any(Date),
      "only",
    );
    expect(answer).toMatchObject({
      at: live.at,
      activeVisitors: 3,
      countries: live.countries,
      channels: live.channels,
      devices: live.devices,
    });
    // One language's minutes are a slice: a minute of 2 page views is hidden.
    expect(answer.perMinute).toEqual([
      {
        minute: "2026-10-28T12:34:00Z",
        pageviews: null,
        events: null,
        visitors: null,
      },
    ]);
    expect(answer.smallValuesHidden).toBe(true);
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
            opened: 30,
            typed: 30,
            resultClicks: 10,
            browseClicks: 0,
            noResults: 10,
            viewAll: 0,
            clickRate: 0.33333,
            noResultRate: 0.33333,
          },
        ],
        bySurfaceAndPageType: [
          { surface: "hero", pageType: "home", opened: 30 },
          { surface: "hero", pageType: "park", opened: 10 },
        ],
        bySurfaceAndDevice: [],
        resultTypes: [],
        positions: [],
        browseItems: [],
        series: [{ day: "2026-10-28", surface: "hero", opened: 30 }],
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

  it("planner_report: rounds the rates, folds small lists, names the planned parks and caps them", async () => {
    const count = (value: string, n: number) => ({ value, count: n });
    const service = {
      planner: jest.fn(async () => ({
        ...WINDOW,
        funnel: {
          opened: 40,
          started: 12,
          optimized: 4,
          shared: 2,
          closedEmpty: 28,
          startRate: 0.30001,
          optimizeRate: 0.33333,
          shareRate: 0.16667,
        },
        sources: [
          { source: "header", opened: 30, started: 6, startRate: 0.2 },
          { source: "kids-page", opened: 2, started: 1, startRate: 0.5 },
        ],
        wizardSteps: [count("park", 12), count("date", 8)],
        wizardFinished: 5,
        wizardWithHeadliners: 3,
        items: [
          { action: "added", kind: "ride", source: "search", count: 7 },
          { action: "added", kind: "show", source: "show_picker", count: 6 },
        ],
        optimized: [count("day", 30)],
        fitOpened: [],
        fitLevers: [],
        undo: [],
        dayCleared: 1,
        sharedOpened: [count("ready", 9)],
        sharedAdopted: 3,
        pushOn: 2,
        byPageTypeAndDevice: [],
        series: [{ day: "2026-10-28", opened: 40, started: 12, optimized: 4 }],
        topParks: [
          { id: "p1", name: "Phantasialand", url: "/parks/p", count: 10 },
          { id: "p2", name: "Efteling", url: "/parks/e", count: 8 },
        ],
        topRides: [],
      })),
    };
    const answer = await run(
      plannerReportTool,
      { limit: 1, datacenter: "include" },
      service,
    );
    expect(service.planner).toHaveBeenCalledWith(
      expect.objectContaining({ datacenter: "include" }),
    );
    expect(answer).toMatchObject({
      datacenter: "exclude",
      funnel: { startRate: 0.3, optimizeRate: 0.333, shareRate: 0.167 },
      wizardFinished: 5,
      pushOn: 2,
    });
    // A way in opened twice is folded: too few to tell anyone apart.
    expect(visitorData(answer).sources).toEqual([
      expect.objectContaining({ source: "header", startRate: 0.2 }),
      expect.objectContaining({ source: "(other)" }),
    ]);
    expect(visitorData(answer).topParks).toEqual([
      { id: "p1", name: "Phantasialand", url: "/parks/p", count: 10 },
    ]);
    expect(visitorData(answer).items).toHaveLength(1);
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
            views: 40,
            referrers: Array.from({ length: 8 }, (_, i) => ({
              value: `site${i}`,
              visits: 6,
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

  describe("small values", () => {
    const FOLDED = /fewer than 5 times are folded into one "\(other\)" row/;
    const SERIES =
      /values from 1 to 4 are null, and smallValuesHidden says whether any were/;

    it("tells the model, in every tool that lists values, that values under five are folded", () => {
      for (const tool of [
        analyticsBreakdownTool,
        analyticsTrafficCrosstabTool,
        analyticsCrosstabTool,
        analyticsEventsTool,
        analyticsEventTool,
        analyticsLiveTool,
        searchReportTool,
        seoLandingPagesTool,
        seoTrendsTool,
        seoNotFoundTool,
      ]) {
        expect([tool.name, tool.description]).toEqual([
          tool.name,
          expect.stringMatching(
            tool.name.includes("crosstab")
              ? /cells add up to fewer than 5/
              : FOLDED,
          ),
        ]);
      }
      for (const tool of [
        analyticsEventsTool,
        analyticsEventTool,
        searchReportTool,
      ]) {
        expect(tool.description).toMatch(SERIES);
      }
      expect(analyticsOverviewTool.description).toMatch(
        /series has its values from 1 to 4 .* as null, and smallValuesHidden/,
      );
      expect(SITE_ANALYTICS_CAVEATS).toContain("hourlyFrom");
      // visitorData stays where the visitor-supplied strings are.
      expect(analyticsBreakdownTool.description).toContain(
        VISITOR_DATA_DESCRIPTION,
      );
    });

    it("analytics_breakdown: folds the small values into (other), sums what adds up, and keeps the total", async () => {
      const service = {
        breakdown: jest.fn(async () => ({
          ...WINDOW,
          dim: "country",
          measure: "views",
          total: { views: 100, visits: 40 },
          items: [
            {
              value: "de",
              views: 90,
              visits: 35,
              share: 0.9,
              avgTimeOnPageSeconds: 30,
            },
            {
              value: "is",
              views: 3,
              visits: 2,
              share: 0.03,
              avgTimeOnPageSeconds: 80,
            },
            {
              value: "nz",
              views: 1,
              visits: 1,
              share: 0.01,
              avgTimeOnPageSeconds: 5,
            },
            {
              value: "nl",
              views: 6,
              visits: 2,
              share: 0.06,
              avgTimeOnPageSeconds: 20,
            },
          ],
        })),
      };
      const answer = await run(
        analyticsBreakdownTool,
        { dim: "country" },
        service,
      );
      expect(answer.total).toEqual({ views: 100, visits: 40 });
      expect(visitorData(answer).items).toEqual([
        {
          value: "de",
          views: 90,
          visits: 35,
          share: 0.9,
          avgTimeOnPageSeconds: 30,
        },
        {
          value: "nl",
          views: 6,
          visits: 2,
          share: 0.06,
          avgTimeOnPageSeconds: 20,
        },
        {
          value: "(other)",
          views: 4,
          visits: 3,
          share: 0.04,
          avgTimeOnPageSeconds: null,
        },
      ]);
    });

    it("analytics_breakdown: leaves bounce, average visit and exits null on the (other) row of the visit and path modes", async () => {
      const service = {
        breakdown: jest
          .fn()
          .mockResolvedValueOnce({
            ...WINDOW,
            dim: "channel",
            measure: "visits",
            total: { views: 60, visits: 60 },
            items: [
              {
                value: "search",
                views: 50,
                visits: 50,
                share: 0.83,
                bounceRate: 0.4,
                avgVisitSeconds: 90,
              },
              {
                value: "ai",
                views: 3,
                visits: 3,
                share: 0.05,
                bounceRate: 0.7,
                avgVisitSeconds: 10,
              },
            ],
          })
          .mockResolvedValueOnce({
            ...WINDOW,
            dim: "path",
            measure: "views",
            total: { views: 60, visits: 20 },
            items: [
              {
                value: "/de",
                views: 50,
                visits: 15,
                share: 0.83,
                avgTimeOnPageSeconds: 20,
                exits: 9,
                exitRate: 0.18,
              },
              {
                value: "/de/x",
                views: 2,
                visits: 0,
                share: 0.03,
                avgTimeOnPageSeconds: 4,
                exits: 1,
                exitRate: 0.5,
              },
            ],
          }),
      };
      const visits = await run(
        analyticsBreakdownTool,
        { dim: "channel" },
        service,
      );
      expect(visitorData(visits).items).toEqual([
        {
          value: "search",
          views: 50,
          visits: 50,
          share: 0.83,
          bounceRate: 0.4,
          avgVisitSeconds: 90,
        },
        {
          value: "(other)",
          views: 3,
          visits: 3,
          share: 0.05,
          bounceRate: null,
          avgVisitSeconds: null,
        },
      ]);
      const paths = await run(analyticsBreakdownTool, { dim: "path" }, service);
      expect((visitorData(paths).items as unknown[])[1]).toEqual({
        value: "(other)",
        views: 2,
        visits: 0,
        share: 0.03,
        avgTimeOnPageSeconds: null,
        exits: 1,
        exitRate: null,
      });
    });

    it("analytics_traffic_crosstab and analytics_crosstab: fold the sides, hide the small cells and count them in other", async () => {
      const cells = [
        { row: "ios", col: "mobile", count: 60 },
        { row: "ios", col: "tablet", count: 3 },
        { row: "windows", col: "desktop", count: 40 },
        { row: "plan9", col: "desktop", count: 2 },
      ];
      const traffic = {
        trafficCrosstab: jest.fn(async () => ({
          ...WINDOW,
          metric: "pageviews",
          row: "os",
          col: "device",
          rows: ["ios", "windows", "plan9"],
          cols: ["mobile", "tablet", "desktop"],
          cells,
          total: 105,
        })),
      };
      const answer = await run(
        analyticsTrafficCrosstabTool,
        { row: "os", col: "device" },
        traffic,
      );
      expect(answer.total).toBe(105);
      expect(visitorData(answer)).toEqual({
        rows: ["ios", "windows", "(other)"],
        cols: ["mobile", "desktop", "(other)"],
        counts: [
          [60, 0, null],
          [0, 40, 0],
          [0, null, 0],
        ],
        other: { cells: 2, count: 5 },
      });

      const event = {
        crosstab: jest.fn(async () => ({
          ...WINDOW,
          name: "tab_changed",
          row: "prop:tab",
          col: "day",
          rows: ["map", "shows"],
          cols: ["2026-10-27", "2026-10-28"],
          cells: [
            { row: "map", col: "2026-10-27", count: 2 },
            { row: "map", col: "2026-10-28", count: 30 },
            { row: "shows", col: "2026-10-28", count: 9 },
          ],
          total: 41,
        })),
      };
      const byDay = await run(
        analyticsCrosstabTool,
        { name: "tab_changed", row: "prop:tab", col: "day" },
        event,
      );
      // The days stay; the cell of two is hidden.
      expect(visitorData(byDay)).toEqual({
        rows: ["map", "shows"],
        cols: ["2026-10-27", "2026-10-28"],
        counts: [
          [null, 30],
          [0, 9],
        ],
        other: { cells: 1, count: 2 },
      });
    });

    it("analytics_events: folds the small names with their counts and previous, and hides small series values", async () => {
      const items = [
        {
          name: "search_opened",
          count: 80,
          previous: 40,
          blocked: false,
          series: [
            { day: "2026-10-27", count: 3 },
            { day: "2026-10-28", count: 77 },
          ],
        },
        { name: "rare_a", count: 2, previous: 1, blocked: false, series: [] },
        { name: "rare_b", count: 1, previous: 3, blocked: true, series: [] },
      ];
      const service = { events: jest.fn(async () => ({ ...WINDOW, items })) };
      const answer = await run(analyticsEventsTool, { series: true }, service);
      expect(answer.total).toBe(3);
      expect(answer.smallValuesHidden).toBe(true);
      expect(visitorData(answer).items).toEqual([
        {
          name: "search_opened",
          count: 80,
          previous: 40,
          change: 1,
          series: [
            { day: "2026-10-27", count: null },
            { day: "2026-10-28", count: 77 },
          ],
        },
        // 3 events against 4 before: a ratio of the sums.
        {
          name: "(other)",
          count: 3,
          previous: 4,
          change: -0.25,
          series: null,
        },
      ]);
      const without = await run(analyticsEventsTool, {}, service);
      expect(without).not.toHaveProperty("smallValuesHidden");
    });

    it("analytics_event: folds the small property values, combinations, page types, devices and entities, and hides small series values", async () => {
      const service = {
        event: jest.fn(async () => ({
          ...WINDOW,
          name: "search_opened",
          blocked: false,
          total: 100,
          props: [
            {
              prop: "surface",
              values: [
                { value: "hero", count: 90 },
                { value: "header", count: 6 },
                { value: "keyboard", count: 3 },
                { value: "page", count: 1 },
              ],
              otherCount: 0,
            },
          ],
          combinations: [
            { props: { surface: "hero" }, count: 90 },
            { props: { surface: "keyboard" }, count: 3 },
            { props: { surface: "page" }, count: 1 },
          ],
          byPageType: [
            { value: "home", count: 95 },
            { value: "park", count: 4 },
            { value: "news", count: 1 },
          ],
          byDevice: [
            { value: "mobile", count: 96 },
            { value: "tablet", count: 4 },
          ],
          entities: [
            {
              id: "p1",
              kind: "park",
              name: "Phantasialand",
              url: "/parks/x",
              count: 50,
            },
            {
              id: "p2",
              kind: "park",
              name: "Europa-Park",
              url: "/parks/y",
              count: 2,
            },
            {
              id: "p3",
              kind: "attraction",
              name: "Taron",
              url: "/a/z",
              count: 1,
            },
          ],
          series: [
            { day: "2026-10-27", count: 4 },
            { day: "2026-10-28", count: 96 },
          ],
        })),
      };
      const answer = await run(
        analyticsEventTool,
        { name: "search_opened", series: true },
        service,
      );
      expect(answer.entities).toEqual([
        expect.objectContaining({ name: "Phantasialand", count: 50 }),
        { id: null, kind: null, name: "(other)", url: null, count: 3 },
      ]);
      expect(answer.smallValuesHidden).toBe(true);
      const data = visitorData(answer);
      expect(data.props).toEqual([
        {
          prop: "surface",
          values: [
            { value: "hero", count: 90 },
            { value: "header", count: 6 },
            { value: "(other)", count: 4 },
          ],
          otherCount: 0,
        },
      ]);
      expect(data.combinations).toEqual([
        { props: { surface: "hero" }, count: 90 },
        { props: "(other)", count: 4 },
      ]);
      expect(data.byPageType).toEqual([
        { value: "home", count: 95 },
        { value: "(other)", count: 5 },
      ]);
      expect(data.byDevice).toEqual([
        { value: "mobile", count: 96 },
        { value: "(other)", count: 4 },
      ]);
      expect(data.series).toEqual([
        { day: "2026-10-27", count: null },
        { day: "2026-10-28", count: 96 },
      ]);
    });

    it("analytics_live: folds every list, and hides the small minutes of one language only", async () => {
      const live = {
        at: "2026-10-28T12:34:56Z",
        activeVisitors: 3,
        perMinute: [
          {
            minute: "2026-10-28T12:34:00Z",
            pageviews: 2,
            events: 1,
            visitors: 20,
          },
        ],
        pages: [
          { path: "/de", pageType: "home", views: 20 },
          { path: "/de/x", pageType: "park", views: 2 },
          { path: "/de/y", pageType: "park", views: 1 },
        ],
        countries: [
          { country: "DE", views: 20 },
          { country: "IS", views: 2 },
        ],
        channels: [
          { channel: "search", visits: 11 },
          { channel: "ai", visits: 1 },
        ],
        referrers: [{ referrer: "google", visits: 2 }],
        devices: [
          { device: "mobile", views: 18 },
          { device: "tablet", views: 4 },
        ],
        events: [{ name: "search_opened", count: 3 }],
      };
      const service = { live: jest.fn(async () => live) };
      const whole = await run(analyticsLiveTool, {}, service);
      // The whole site's minutes are totals.
      expect(whole.perMinute).toEqual(live.perMinute);
      expect(whole).not.toHaveProperty("smallValuesHidden");
      expect(whole.countries).toEqual([
        { country: "DE", views: 20 },
        { country: "(other)", views: 2 },
      ]);
      expect(whole.channels).toEqual([
        { channel: "search", visits: 11 },
        { channel: "(other)", visits: 1 },
      ]);
      expect(whole.devices).toEqual([
        { device: "mobile", views: 18 },
        { device: "(other)", views: 4 },
      ]);
      expect(visitorData(whole)).toEqual({
        pages: [
          { path: "/de", pageType: "home", views: 20 },
          { path: "(other)", pageType: null, views: 3 },
        ],
        referrers: [{ referrer: "(other)", visits: 2 }],
        events: [{ name: "(other)", count: 3 }],
      });
      const german = await run(analyticsLiveTool, { locale: "de" }, service);
      expect(german.perMinute).toEqual([
        {
          minute: "2026-10-28T12:34:00Z",
          pageviews: null,
          events: null,
          visitors: 20,
        },
      ]);
      expect(german.smallValuesHidden).toBe(true);
    });

    it("analytics_overview: hides the small values of a series under a filter or a language, with what is computed from them, and not those of the whole site", async () => {
      const point = (day: string, visits: number, pageviews: number) => ({
        day,
        pageviews,
        visits,
        visitors: null,
        events: 0,
        viewsPerVisit: 2.5,
        bounceRate: 0.5,
        avgVisitSeconds: 40,
        avgTimeOnPageSeconds: 10,
      });
      const totals = { ...point("t", 3, 9), day: undefined };
      const report = () => ({
        ...WINDOW,
        totals,
        previous: null,
        series: [point("2026-10-27", 3, 9), point("2026-10-28", 30, 90)],
        realtime: { activeVisitors: 3, perMinute: [] },
      });
      const service = { overview: jest.fn(async () => report()) };
      const whole = await run(analyticsOverviewTool, { series: true }, service);
      expect(whole.series).toEqual([
        expect.objectContaining({
          day: "2026-10-27",
          visits: 3,
          bounceRate: 0.5,
        }),
        expect.objectContaining({ day: "2026-10-28", visits: 30 }),
      ]);
      expect(whole).not.toHaveProperty("smallValuesHidden");

      const filtered = await run(
        analyticsOverviewTool,
        { series: true, filters: [{ dim: "country", value: "is" }] },
        service,
      );
      expect(filtered.series).toEqual([
        expect.objectContaining({
          day: "2026-10-27",
          visits: null,
          pageviews: 9,
          viewsPerVisit: null,
          bounceRate: null,
          avgVisitSeconds: null,
          avgTimeOnPageSeconds: 10,
        }),
        expect.objectContaining({
          day: "2026-10-28",
          visits: 30,
          bounceRate: 0.5,
        }),
      ]);
      expect(filtered.smallValuesHidden).toBe(true);
      // The totals are small cells too under a filter.
      expect(filtered.totals).toMatchObject({
        visits: null,
        pageviews: 9,
        viewsPerVisit: null,
      });
      const german = await run(
        analyticsOverviewTool,
        { series: true, locale: "de" },
        service,
      );
      expect(german.smallValuesHidden).toBe(true);
      expect(
        (german.series as Array<{ visits: number | null }>)[0].visits,
      ).toBeNull();
    });

    it("search_report: folds each list by its count, empties the rates of (other), and hides small series values", async () => {
      const service = {
        search: jest.fn(async () => ({
          ...WINDOW,
          surfaces: [
            {
              surface: "hero",
              opened: 30,
              typed: 20,
              resultClicks: 10,
              browseClicks: 1,
              noResults: 2,
              viewAll: 0,
              clickRate: 0.37,
              noResultRate: 0.1,
            },
            {
              surface: "page",
              opened: 3,
              typed: 2,
              resultClicks: 1,
              browseClicks: 0,
              noResults: 1,
              viewAll: 0,
              clickRate: 0.33,
              noResultRate: 0.5,
            },
            {
              surface: "keyboard",
              opened: 1,
              typed: 1,
              resultClicks: 0,
              browseClicks: 0,
              noResults: 0,
              viewAll: 1,
              clickRate: 0,
              noResultRate: 0,
            },
          ],
          bySurfaceAndPageType: [
            { surface: "hero", pageType: "home", opened: 30 },
            { surface: "page", pageType: "home", opened: 3 },
          ],
          bySurfaceAndDevice: [
            { surface: "hero", device: "mobile", opened: 28 },
            { surface: "hero", device: "tablet", opened: 2 },
          ],
          resultTypes: [
            { surface: "hero", result: "park", clicks: 9 },
            { surface: "hero", result: "show", clicks: 1 },
          ],
          positions: [
            { position: "1", clicks: 8 },
            { position: "2", clicks: 2 },
            { position: "11+", clicks: 1 },
          ],
          browseItems: [{ surface: "hero", item: "parks", clicks: 4 }],
          series: [
            { day: "2026-10-27", surface: "hero", opened: 2 },
            { day: "2026-10-28", surface: "hero", opened: 28 },
          ],
        })),
      };
      const answer = await run(searchReportTool, { series: true }, service);
      expect(answer.smallValuesHidden).toBe(true);
      const data = visitorData(answer);
      expect(data.surfaces).toEqual([
        expect.objectContaining({ surface: "hero", clickRate: 0.37 }),
        {
          surface: "(other)",
          opened: 4,
          typed: 3,
          resultClicks: 1,
          browseClicks: 0,
          noResults: 1,
          viewAll: 1,
          clickRate: null,
          noResultRate: null,
        },
      ]);
      expect(data.bySurfaceAndPageType).toEqual([
        { surface: "hero", pageType: "home", opened: 30 },
        { surface: "(other)", pageType: "(other)", opened: 3 },
      ]);
      expect(data.bySurfaceAndDevice).toEqual([
        { surface: "hero", device: "mobile", opened: 28 },
        { surface: "(other)", device: "(other)", opened: 2 },
      ]);
      expect(data.resultTypes).toEqual([
        { surface: "hero", result: "park", clicks: 9 },
        { surface: "(other)", result: "(other)", clicks: 1 },
      ]);
      expect(data.positions).toEqual([
        { position: "1", clicks: 8 },
        { position: "(other)", clicks: 3 },
      ]);
      expect(data.browseItems).toEqual([
        { surface: "(other)", item: "(other)", clicks: 4 },
      ]);
      expect(data.series).toEqual([
        { day: "2026-10-27", surface: "hero", opened: null },
        { day: "2026-10-28", surface: "hero", opened: 28 },
      ]);
    });

    it("seo_landing_pages: folds the small pages, and the small sources of each", async () => {
      const seo = {
        landingPages: jest.fn(async () => ({
          ...WINDOW,
          channel: "search",
          items: [
            {
              path: "/de",
              pageType: "home",
              visits: 50,
              previous: 40,
              change: 10,
              bounceRate: 0.4,
              avgVisitSeconds: 60,
              sources: [
                { value: "google", visits: 45 },
                { value: "bing", visits: 3 },
                { value: "ecosia", visits: 2 },
              ],
            },
            {
              path: "/de/a",
              pageType: "park",
              visits: 3,
              previous: 5,
              change: -2,
              bounceRate: 1,
              avgVisitSeconds: 5,
              sources: [{ value: "google", visits: 3 }],
            },
            {
              path: "/de/b",
              pageType: "news",
              visits: 2,
              previous: null,
              change: null,
              bounceRate: 0,
              avgVisitSeconds: null,
              sources: [],
            },
          ],
        })),
      };
      const answer = await run(seoLandingPagesTool, {}, seo);
      expect(visitorData(answer).items).toEqual([
        {
          path: "/de",
          pageType: "home",
          visits: 50,
          previous: 40,
          change: 10,
          bounceRate: 0.4,
          avgVisitSeconds: 60,
          sources: [
            { value: "google", visits: 45 },
            { value: "(other)", visits: 5 },
          ],
        },
        {
          path: "(other)",
          pageType: null,
          visits: 5,
          // One of the folded pages has no window before.
          previous: null,
          change: null,
          bounceRate: null,
          avgVisitSeconds: null,
          sources: null,
        },
      ]);
    });

    it("seo_trends: folds the small pages of each direction on their visits in the window", async () => {
      const page = (path: string, visits: number, previous: number) => ({
        path,
        pageType: "park",
        visits,
        previous,
        change: visits - previous,
        avgVisitSeconds: 30,
      });
      const seo = {
        trends: jest.fn(async () => ({
          ...WINDOW,
          channel: "search",
          rising: [page("/a", 30, 1), page("/b", 4, 1)],
          falling: [page("/c", 2, 20), page("/d", 1, 9)],
        })),
      };
      const answer = await run(seoTrendsTool, {}, seo);
      expect(visitorData(answer).rising).toEqual([
        page("/a", 30, 1),
        {
          path: "(other)",
          pageType: null,
          visits: 4,
          previous: 1,
          change: 3,
          avgVisitSeconds: null,
        },
      ]);
      expect(visitorData(answer).falling).toEqual([
        {
          path: "(other)",
          pageType: null,
          visits: 3,
          previous: 29,
          change: -26,
          avgVisitSeconds: null,
        },
      ]);
    });

    it("seo_not_found: folds the small paths and the small referrers of each, and leaves seo_uncovered's list alone", async () => {
      const seo = {
        notFound: jest.fn(async () => ({
          ...WINDOW,
          items: [
            {
              path: "/de/x",
              views: 40,
              referrers: [
                { value: "google", visits: 30 },
                { value: "bing", visits: 2 },
              ],
            },
            {
              path: "/de/y",
              views: 3,
              referrers: [{ value: "google", visits: 3 }],
            },
            { path: "/de/z", views: 1, referrers: [] },
          ],
        })),
        uncovered: jest.fn(async () => ({
          ...WINDOW,
          kind: "park",
          total: 150,
          uncovered: 2,
          items: [
            { id: "a", kind: "park", name: "A", url: "/parks/a" },
            { id: "b", kind: "park", name: "B", url: "/parks/b" },
          ],
        })),
      };
      const answer = await run(seoNotFoundTool, {}, seo);
      expect(visitorData(answer).items).toEqual([
        {
          path: "/de/x",
          views: 40,
          referrers: [
            { value: "google", visits: 30 },
            { value: "(other)", visits: 2 },
          ],
        },
        { path: "(other)", views: 4, referrers: null },
      ]);
      // Our own parks and rides without a single search visit: no count to fold.
      const uncovered = await run(seoUncoveredTool, {}, seo);
      expect(uncovered.items).toEqual([
        { id: "a", kind: "park", name: "A", url: "/parks/a" },
        { id: "b", kind: "park", name: "B", url: "/parks/b" },
      ]);
      expect(seoUncoveredTool.description).toContain("no counts to fold");
    });

    it("echoes hourlyFrom in every tool that echoes the window", async () => {
      const service = {
        breakdown: jest.fn(async () => ({
          ...WINDOW,
          hourlyFrom: "2026-07-30",
          dim: "country",
          measure: "views",
          total: { views: 0, visits: 0 },
          items: [],
        })),
        events: jest.fn(async () => ({ ...WINDOW, items: [] })),
      };
      expect(
        await run(analyticsBreakdownTool, { dim: "country" }, service),
      ).toMatchObject({ hourlyFrom: "2026-07-30" });
      // A report that sends none (an older answer) reads as covered.
      expect(await run(analyticsEventsTool, {}, service)).toMatchObject({
        hourlyFrom: null,
      });
    });
  });
});
