import { z } from "zod";
import {
  McpToolError,
  type AnyMcpTool,
  type McpToolContext,
} from "../mcp-tool";
import { SiteAnalyticsTripService } from "../../site-analytics/site-analytics-trip.service";
import {
  TRIP_EVENTS,
  tripPlannerFigures,
  type TripEventRow,
} from "../../site-analytics/site-analytics-trip";
import { TicketCatalogService } from "../../trip-regions/ticket-catalog.service";
import { TripRequestMetricsService } from "../../trip-regions/metrics/trip-request-metrics.service";
import { tripPlannerReportTool } from "./trip-planner-report.tool";
import { opsWindow, tripPlannerOpsTool } from "./trip-planner-ops.tool";
import { SMALL_CELLS_DESCRIPTION } from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, VISITOR_DATA_NOTICE } from "./visitor-data";

const WINDOW = {
  from: "2026-11-01",
  to: "2026-11-30",
  days: 30,
  interval: "day",
  locale: null,
  filters: [],
  datacenter: "exclude",
  hourlyFrom: null,
};

/** Runs a tool the way the server does: the input parsed by its schema first. */
async function run(
  tool: AnyMcpTool,
  raw: Record<string, unknown>,
  services: Map<unknown, unknown>,
) {
  const input = z.object(tool.input).parse(raw);
  const ctx = {
    admin: {} as McpToolContext["admin"],
    get: (token: unknown) => services.get(token),
  } as unknown as McpToolContext;
  return (await tool.handler(input as never, ctx)) as Record<string, any>;
}

const OLD = "2026-11-12";
const NEW = "2026-11-20";
const orlando = (
  name: string,
  props: Record<string, string>,
  count: number,
  day = NEW,
) =>
  ({
    name,
    props: { region: "orlando", ...props },
    count,
    day,
  }) as TripEventRow;

/** The old planner to the 17th, the dates-first planner from the 18th. */
const ROWS: TripEventRow[] = [
  orlando(TRIP_EVENTS.started, { source: "hero" }, 20, OLD),
  orlando(TRIP_EVENTS.planShown, { mode: "dates", outcome: "ok" }, 18, OLD),
  orlando(TRIP_EVENTS.ticketsAnswered, { answer: "yes" }, 6, OLD),
  orlando(TRIP_EVENTS.daysSaved, { days: "5" }, 1, OLD),
  orlando(TRIP_EVENTS.started, { source: "dates" }, 40),
  orlando(TRIP_EVENTS.started, { source: "link" }, 2),
  orlando(TRIP_EVENTS.planRequested, { mode: "dates", parks: "0" }, 50),
  orlando(TRIP_EVENTS.planRequested, { mode: "when", parks: "4" }, 6),
  orlando(TRIP_EVENTS.planShown, { mode: "dates", outcome: "ok" }, 45),
  orlando(
    TRIP_EVENTS.planShown,
    { mode: "dates", outcome: "error", status: "0" },
    3,
  ),
  orlando(TRIP_EVENTS.editOpened, { panel: "parks", from: "answer" }, 12),
  orlando(
    TRIP_EVENTS.optionChanged,
    { option: "rest", value: "none", by: "visitor" },
    8,
  ),
  orlando(TRIP_EVENTS.ticketsOpened, { from: "card", first: "true" }, 9),
  orlando(TRIP_EVENTS.daysSaved, { days: "5" }, 3),
  orlando(TRIP_EVENTS.linkCopied, {}, 4),
  {
    name: "ticket_link_clicked",
    props: {
      product: "wdw-date-based",
      kind: "official",
      position: "1",
      seller: "Disney",
    },
    count: 9,
    day: NEW,
  },
  {
    name: "ticket_link_clicked",
    props: {
      product: "wdw-date-based",
      kind: "sponsored",
      position: "3",
      seller: "Some Shop",
    },
    count: 2,
    day: NEW,
  },
];

/** A report service that answers the figures of `rows` for the options it gets. */
function services(
  rows: TripEventRow[] = ROWS,
  newFunnelFrom: string | null = "2026-11-18",
) {
  const trip = {
    report: jest.fn(async (_query: unknown, options: any) => ({
      ...WINDOW,
      ...tripPlannerFigures(rows, {
        ...options,
        window: { from: WINDOW.from, to: WINDOW.to },
        newFunnelFrom,
      }),
      ...(options.series && {
        series: [
          {
            day: "2026-11-13",
            started: 30,
            requested: 2,
            shown: 0,
          },
          {
            day: "2026-11-14",
            started: 12,
            requested: 54,
            shown: 48,
          },
        ],
      }),
    })),
  };
  const catalog = {
    catalog: jest.fn(async (region: { slug: string }) => ({
      region: region.slug,
      version: "v1",
      products:
        region.slug === "orlando"
          ? [{ id: "wdw-date-based" }, { id: "archived-product" }]
          : [{ id: `${region.slug}-1-day` }],
    })),
  };
  return {
    trip,
    catalog,
    map: new Map<unknown, unknown>([
      [SiteAnalyticsTripService, trip],
      [TicketCatalogService, catalog],
    ]),
  };
}

describe("trip_planner_report", () => {
  it("says what it answers, where the visitor strings are and how small values fold", () => {
    const text = tripPlannerReportTool.description;
    expect(text).toContain(VISITOR_DATA_DESCRIPTION);
    expect(text).toContain(SMALL_CELLS_DESCRIPTION);
    expect(text).toContain("planner_report");
    expect(text).toContain("trip_planner_ops");
    expect(text).toContain("started (trip_planner_started");
    expect(text).toContain("ofShown");
    expect(text).toContain("newFunnelFrom");
    // The old planner's wizard steps are gone from the funnel and its description.
    expect(text).not.toContain("not_yet, yes, skipped");
    expect(text).not.toContain("wizard");
    const schema = z.object(tripPlannerReportTool.input);
    expect(schema.safeParse({ region: "atlantis" }).success).toBe(false);
    expect(
      schema.safeParse({ filters: [{ dim: "channel", value: "search" }] })
        .success,
    ).toBe(false);
  });

  it("walks the dates-first funnel, each era by its own events, and rates over the days both steps count", async () => {
    const { trip, catalog, map } = services();
    const answer = await run(tripPlannerReportTool, { limit: 5 }, map);

    // Every region's catalog attributes the purchase links.
    expect(catalog.catalog).toHaveBeenCalledWith(
      expect.objectContaining({ slug: "orlando" }),
    );
    const options = trip.report.mock.calls[0][1];
    expect(options).toMatchObject({ region: null, series: false });
    expect(options.productRegions.get("wdw-date-based")).toBe("orlando");
    expect(options.productRegions.get("japan-1-day")).toBe("japan");

    const step = (name: string) =>
      answer.funnel.find((s: { step: string }) => s.step === name);
    expect(answer.newFunnelFrom).toBe("2026-11-18");
    expect(answer.funnel.map((s: { step: string }) => s.step)).toEqual([
      "started",
      "requested",
      "shown",
      "changed",
      "tickets",
      "kept",
    ]);
    expect(step("started")).toMatchObject({
      count: 62,
      before: 20,
      since: 42,
      ofStarted: null,
      fromPrevious: null,
      dropOff: null,
    });
    expect(step("requested")).toMatchObject({
      count: 56,
      before: 0,
      since: 56,
      fromPrevious: 0.903,
      dropOff: 0.097,
    });
    // More answers than requests in the old planner's days: no drop-off to speak of.
    expect(step("shown")).toMatchObject({
      count: 66,
      before: 18,
      since: 48,
      fromPrevious: 1.179,
      dropOff: null,
      ok: 63,
      error: 3,
      errorRate: 0.045,
    });
    // Optional steps are shares of the answers shown, never a drop-off; changes were
    // counted only since the cut-over, so against those days' answers alone.
    expect(step("changed")).toMatchObject({
      count: 20,
      before: null,
      since: 20,
      ofShown: 0.417,
      ofStarted: 0.476,
      events: {
        before: null,
        since: ["trip_edit_opened", "trip_option_changed"],
      },
    });
    expect(step("changed")).not.toHaveProperty("dropOff");
    expect(step("tickets")).toMatchObject({
      count: 15,
      before: 6,
      since: 9,
      ticketsOpened: 9,
      ticketsAnsweredYes: 6,
      ofShown: 0.227,
    });
    expect(step("tickets")).not.toHaveProperty("fromPrevious");
    // Kept follows the answers shown, past the optional steps.
    expect(step("kept")).toMatchObject({
      count: 8,
      fromPrevious: 0.121,
      dropOff: 0.879,
      daysSaved: 4,
      linkCopied: 4,
      dayPlannerOpened: { tripDay: 0, tripFirst: 0, hub: 0 },
    });
    expect(answer.requests).toMatchObject({
      planRequested: 56,
      datesKnown: 50,
      datesSearched: 6,
      parksHeadliners: 50,
    });
    expect(answer.ticketLinks).toMatchObject({
      clicks: 11,
      unattributedTicketLinks: 0,
    });
    // The whole planner's totals are as counted.
    expect(answer).not.toHaveProperty("smallValuesHidden");

    expect(answer.visitorDataNotice).toBe(VISITOR_DATA_NOTICE);
    const data = answer.visitorData;
    // A way in used twice is folded: too few to tell anyone apart.
    expect(data.started.sources).toEqual([
      { source: "dates", count: 40 },
      { source: "hero", count: 20 },
      { source: "(other)", count: 2 },
    ]);
    expect(data.ticketLinks.byKindAndPosition).toEqual([
      { kind: "official", position: "1", count: 9 },
      { kind: "(other)", position: "(other)", count: 2 },
    ]);
    expect(data.answers.errors).toEqual([
      { mode: "(other)", status: "(other)", count: 3 },
    ]);
    expect(data.changes.options).toEqual([
      { option: "rest", value: "none", by: "visitor", count: 8 },
    ]);
    expect(data.regions).toEqual([
      expect.objectContaining({
        region: "orlando",
        started: 62,
        ticketLinks: 11,
      }),
    ]);
    expect(data).not.toHaveProperty("series");
  });

  it("hides one region's small counts, per era too, and every rate worked out from them", async () => {
    const { trip, map } = services([
      orlando(TRIP_EVENTS.started, { source: "hero" }, 3, OLD),
      orlando(TRIP_EVENTS.started, { source: "dates" }, 20),
      orlando(TRIP_EVENTS.planShown, { mode: "dates", outcome: "ok" }, 12),
      orlando(TRIP_EVENTS.editOpened, { panel: "parks", from: "answer" }, 2),
      orlando(TRIP_EVENTS.daysSaved, { days: "4" }, 1),
    ]);
    const answer = await run(tripPlannerReportTool, { region: "orlando" }, map);
    expect(trip.report.mock.calls[0][1]).toMatchObject({ region: "orlando" });
    const step = (name: string) =>
      answer.funnel.find((s: { step: string }) => s.step === name);
    // Three starts before the cut-over: the step's whole count would give them away.
    expect(step("started")).toMatchObject({
      count: null,
      before: null,
      since: 20,
    });
    expect(step("requested")).toMatchObject({ count: 0, fromPrevious: null });
    expect(step("shown")).toMatchObject({ count: 12, ofStarted: null });
    expect(step("changed")).toMatchObject({ count: null, ofShown: null });
    expect(step("kept")).toMatchObject({ count: null, daysSaved: null });
    expect(answer).toMatchObject({
      region: "orlando",
      smallValuesHidden: true,
    });
  });

  it("reads the old planner's events alone until the new ones are counted", async () => {
    const { map } = services(ROWS, null);
    const answer = await run(tripPlannerReportTool, {}, map);
    const step = (name: string) =>
      answer.funnel.find((s: { step: string }) => s.step === name);
    expect(answer.newFunnelFrom).toBeNull();
    expect(step("changed")).toMatchObject({ count: null, since: null });
    expect(step("tickets")).toMatchObject({ count: 6, before: 6 });
  });

  it("reads the tile as a way in beside the sources: shown, clicked by link, per region", async () => {
    const page =
      "/de/parks/north-america/united-states/orlando/magic-kingdom-park";
    const { trip, map } = services([
      ...ROWS,
      {
        name: "section_seen",
        props: { section: "trip_tile" },
        count: 40,
        path: page,
      },
      orlando(TRIP_EVENTS.tileClicked, { link: "plan" }, 8),
      orlando(TRIP_EVENTS.tileClicked, { link: "calendar" }, 2),
    ]);
    const answer = await run(tripPlannerReportTool, {}, map);
    // The region parks' pages, from regions.ts.
    expect(
      trip.report.mock.calls[0][1].parkRegions.get(
        "north-america/united-states/orlando/magic-kingdom-park",
      ),
    ).toBe("orlando");
    expect(answer.tile).toEqual({
      shown: 40,
      clicked: 10,
      plan: 8,
      calendar: 2,
      clickRate: 0.25,
    });
    expect(answer.visitorData.started.tile).toEqual([
      {
        region: "orlando",
        shown: 40,
        clicked: 10,
        plan: 8,
        calendar: 2,
        clickRate: 0.25,
      },
    ]);
    expect(answer.visitorData.started.tileClicks).toEqual([
      { link: "plan", count: 8 },
      { link: "(other)", count: 2 },
    ]);
  });

  it("draws the series on request, its small values hidden", async () => {
    const { map } = services();
    const answer = await run(tripPlannerReportTool, { series: true }, map);
    expect(answer.visitorData.series).toEqual([
      { day: "2026-11-13", started: 30, requested: null, shown: 0 },
      { day: "2026-11-14", started: 12, requested: 54, shown: 48 },
    ]);
    expect(answer.smallValuesHidden).toBe(true);
  });
});

describe("trip_planner_ops", () => {
  const NOW = new Date("2026-11-14T10:00:00Z");

  it("reads the last seven UTC days by default, or a window inside the retention", () => {
    expect(opsWindow({}, NOW)).toEqual({
      from: "2026-11-08",
      to: "2026-11-14",
    });
    expect(opsWindow({ days: 1 }, NOW)).toEqual({
      from: "2026-11-14",
      to: "2026-11-14",
    });
    expect(opsWindow({ from: "2026-10-01", to: "2026-10-31" }, NOW)).toEqual({
      from: "2026-10-01",
      to: "2026-10-31",
    });
    for (const bad of [
      { from: "2026-10-01" },
      { from: "2026-10-31", to: "2026-10-01" },
      { from: "2026-11-01", to: "2026-11-15" },
      { from: "2026-08-01", to: "2026-08-31" },
      { days: 3, from: "2026-11-01", to: "2026-11-02" },
    ]) {
      expect(() => opsWindow(bad, NOW)).toThrow(McpToolError);
    }
    const schema = z.object(tripPlannerOpsTool.input);
    expect(schema.safeParse({ days: 91 }).success).toBe(false);
    expect(schema.safeParse({ route: "calendar" }).success).toBe(false);
    expect(schema.safeParse({ region: "(unknown)" }).success).toBe(true);
  });

  it("passes the window, region, route, limit and series to the metrics and answers what they say", async () => {
    const report = { totals: { requests: 3 }, routes: [] };
    const metrics = { ops: jest.fn(async () => report) };
    const answer = await run(
      tripPlannerOpsTool,
      { days: 3, region: "orlando", route: "when", limit: 4, series: true },
      new Map([[TripRequestMetricsService, metrics]]),
    );
    expect(answer).toBe(report);
    expect(metrics.ops).toHaveBeenCalledWith(
      expect.objectContaining({
        region: "orlando",
        route: "when",
        limit: 4,
        series: true,
      }),
    );
    const [query] = metrics.ops.mock.calls[0] as unknown[] as [
      { from: string; to: string },
    ];
    expect((Date.parse(query.to) - Date.parse(query.from)) / 86_400_000).toBe(
      2,
    );
  });

  it("says what is not recorded, and that it carries no visitor data", () => {
    expect(tripPlannerOpsTool.description).toContain("Cloudflare");
    expect(tripPlannerOpsTool.description).toContain("no visitor data");
    expect(tripPlannerOpsTool.description).toContain("90 days");
  });
});
