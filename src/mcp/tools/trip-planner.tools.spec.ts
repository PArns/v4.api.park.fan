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
  from: "2026-10-16",
  to: "2026-11-14",
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

const orlando = (name: string, props: Record<string, string>, count: number) =>
  ({ name, props: { region: "orlando", ...props }, count }) as TripEventRow;

const ROWS: TripEventRow[] = [
  orlando(TRIP_EVENTS.started, { source: "hero" }, 40),
  orlando(TRIP_EVENTS.started, { source: "link" }, 2),
  orlando(TRIP_EVENTS.ticketsAnswered, { answer: "not_yet" }, 30),
  orlando(TRIP_EVENTS.planRequested, { mode: "dates", parks: "0" }, 50),
  orlando(TRIP_EVENTS.planRequested, { mode: "when", parks: "4" }, 6),
  orlando(TRIP_EVENTS.planShown, { mode: "dates", outcome: "ok" }, 45),
  orlando(
    TRIP_EVENTS.planShown,
    { mode: "dates", outcome: "error", status: "0" },
    3,
  ),
  orlando(TRIP_EVENTS.viewChanged, { view: "week" }, 12),
  orlando(TRIP_EVENTS.dayOpened, { kind: "park", view: "list" }, 20),
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
  },
];

/** A report service that answers the figures of `rows` for the options it gets. */
function services(rows: TripEventRow[] = ROWS) {
  const trip = {
    report: jest.fn(async (_query: unknown, options: any) => ({
      ...WINDOW,
      ...tripPlannerFigures(rows, options),
      ...(options.series && {
        series: [
          { day: "2026-11-13", started: 30, planRequested: 2, planShown: 0 },
          { day: "2026-11-14", started: 12, planRequested: 54, planShown: 48 },
        ],
      }),
    })),
  };
  const catalog = {
    catalog: jest.fn(async (region: { slug: string }) => ({
      region: region.slug,
      version: "v1",
      products: [{ id: "wdw-date-based" }, { id: "archived-product" }],
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
    expect(tripPlannerReportTool.description).toContain(
      VISITOR_DATA_DESCRIPTION,
    );
    expect(tripPlannerReportTool.description).toContain(
      SMALL_CELLS_DESCRIPTION,
    );
    expect(tripPlannerReportTool.description).toContain("planner_report");
    expect(tripPlannerReportTool.description).toContain("trip_planner_ops");
    const schema = z.object(tripPlannerReportTool.input);
    expect(schema.safeParse({ region: "atlantis" }).success).toBe(false);
    expect(
      schema.safeParse({ filters: [{ dim: "channel", value: "search" }] })
        .success,
    ).toBe(false);
  });

  it("walks the funnel with its rates, reads dates and parks off the requests, and folds small lists", async () => {
    const { trip, catalog, map } = services();
    const answer = await run(tripPlannerReportTool, { limit: 5 }, map);

    // Every region's catalog attributes the purchase links.
    expect(catalog.catalog).toHaveBeenCalledWith(
      expect.objectContaining({ slug: "orlando" }),
    );
    const options = trip.report.mock.calls[0][1];
    expect(options).toMatchObject({ region: null, series: false });
    expect([...options.productRegions]).toEqual([
      ["wdw-date-based", "orlando"],
      ["archived-product", "orlando"],
    ]);

    const step = (name: string) =>
      answer.funnel.find((s: { step: string }) => s.step === name);
    expect(answer.funnel.map((s: { step: string }) => s.step)).toEqual([
      "started",
      "ticketsAnswered",
      "planRequested",
      "planShown",
      "viewChanged",
      "dayOpened",
      "kept",
    ]);
    expect(step("started")).toMatchObject({
      count: 42,
      ofStarted: null,
      fromPrevious: null,
    });
    expect(step("ticketsAnswered")).toMatchObject({
      count: 30,
      ofStarted: 0.714,
      fromPrevious: 0.714,
      dropOff: 0.286,
    });
    // Requests outnumber the answers before them: no drop-off to speak of.
    expect(step("planRequested")).toMatchObject({
      count: 56,
      fromPrevious: 1.867,
      dropOff: null,
      dates: { known: 50, searched: 6 },
      parks: { named: 6, headliners: 50 },
    });
    expect(step("planShown")).toMatchObject({
      ok: 45,
      error: 3,
      errorRate: 0.063,
    });
    expect(step("kept")).toMatchObject({
      count: 7,
      events: ["trip_days_saved", "trip_ics_exported", "trip_link_copied"],
      daysSaved: 3,
      linkCopied: 4,
    });
    expect(answer.findDates).toMatchObject({ requested: 6 });
    expect(answer.ticketLinks).toMatchObject({
      clicks: 11,
      unattributedTicketLinks: 0,
    });
    // The whole planner's totals are as counted.
    expect(answer).not.toHaveProperty("smallValuesHidden");

    expect(answer.visitorDataNotice).toBe(VISITOR_DATA_NOTICE);
    const data = answer.visitorData;
    // A way in used twice is folded: too few to tell anyone apart.
    expect(data.sources).toEqual([
      { value: "hero", count: 40 },
      { value: "(other)", count: 2 },
    ]);
    expect(data.ticketLinks.byKindAndPosition).toEqual([
      { kind: "official", position: "1", count: 9 },
      { kind: "(other)", position: "(other)", count: 2 },
    ]);
    expect(data.errors).toEqual([
      { mode: "(other)", status: "(other)", count: 3 },
    ]);
    expect(data.regions).toEqual([
      expect.objectContaining({
        region: "orlando",
        started: 42,
        ticketLinks: 11,
      }),
    ]);
    expect(data).not.toHaveProperty("series");
  });

  it("hides one region's small totals and every rate worked out from them", async () => {
    const { trip, map } = services([
      orlando(TRIP_EVENTS.started, { source: "hero" }, 20),
      orlando(TRIP_EVENTS.ticketsAnswered, { answer: "yes" }, 3),
      orlando(TRIP_EVENTS.planRequested, { mode: "dates", parks: "2" }, 12),
      orlando(TRIP_EVENTS.daysSaved, { days: "4" }, 1),
    ]);
    const answer = await run(tripPlannerReportTool, { region: "orlando" }, map);
    expect(trip.report.mock.calls[0][1]).toMatchObject({ region: "orlando" });
    const step = (name: string) =>
      answer.funnel.find((s: { step: string }) => s.step === name);
    expect(step("started").count).toBe(20);
    expect(step("ticketsAnswered")).toMatchObject({
      count: null,
      ofStarted: null,
      fromPrevious: null,
      dropOff: null,
    });
    // Its own rate needs the hidden step before it.
    expect(step("planRequested")).toMatchObject({
      count: 12,
      ofStarted: 0.6,
      fromPrevious: null,
    });
    expect(step("kept")).toMatchObject({ count: null, daysSaved: null });
    expect(answer).toMatchObject({
      region: "orlando",
      smallValuesHidden: true,
    });
  });

  it("draws the series on request, its small values hidden", async () => {
    const { map } = services();
    const answer = await run(tripPlannerReportTool, { series: true }, map);
    expect(answer.visitorData.series).toEqual([
      { day: "2026-11-13", started: 30, planRequested: null, planShown: 0 },
      { day: "2026-11-14", started: 12, planRequested: 54, planShown: 48 },
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
