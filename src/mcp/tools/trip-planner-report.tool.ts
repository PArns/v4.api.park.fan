import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsTripService } from "../../site-analytics/site-analytics-trip.service";
import {
  TICKET_LINK_EVENT,
  TRIP_EVENTS,
  TRIP_FUNNEL,
  type TripFunnelStep,
  type TripTotals,
  type ValueCount,
} from "../../site-analytics/site-analytics-trip";
import { TRIP_REGIONS } from "../../trip-regions/regions";
import { TicketCatalogService } from "../../trip-regions/ticket-catalog.service";
import {
  HOURLY_FROM_DESCRIPTION,
  eventWindowInput,
  round3,
  siteQuery,
  windowOf,
} from "./site-analytics.inputs";
import {
  SMALL_CELLS_DESCRIPTION,
  SMALL_SERIES_DESCRIPTION,
  foldSmallRows,
  hideSmallInSeries,
  isNarrowed,
} from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

const REGIONS = Object.keys(TRIP_REGIONS) as [string, ...string[]];

/** The event each funnel step counts; `kept` is three. */
const STEP_EVENTS: Record<TripFunnelStep, string[]> = {
  started: [TRIP_EVENTS.started],
  ticketsAnswered: [TRIP_EVENTS.ticketsAnswered],
  planRequested: [TRIP_EVENTS.planRequested],
  planShown: [TRIP_EVENTS.planShown],
  viewChanged: [TRIP_EVENTS.viewChanged],
  dayOpened: [TRIP_EVENTS.dayOpened],
  kept: [
    TRIP_EVENTS.daysSaved,
    TRIP_EVENTS.icsExported,
    TRIP_EVENTS.linkCopied,
  ],
};

/** A ratio to three places, null without both counts or a base. */
const ratio = (count: number | null, base: number | null): number | null =>
  count === null || base === null || base === 0 ? null : round3(count / base);

export const tripPlannerReportTool = defineMcpTool({
  name: "trip_planner_report",
  title: "Trip planner (vacation) usage",
  description:
    "How park.fan's multi-day trip planner (the region hubs: Orlando and the regions after it) " +
    "is used, from its trip_* events and ticket_link_clicked, per region or for all (`region`). " +
    "Not the day planner: that is planner_report. The funnel in the order a visitor meets it: " +
    "started → ticketsAnswered → planRequested → planShown → viewChanged → dayOpened → kept " +
    "(days saved to the day planner + .ics exported + link copied), each with ofStarted, " +
    "fromPrevious and dropOff (1 − fromPrevious, null where a step outnumbers the one before). " +
    "Counts are events, not visitors: planRequested and planShown fire once per distinct " +
    "request, so they can exceed started. The wizard's dates and parks steps send no event of " +
    "their own; they are read off planRequested: dates known (mode dates) or searched (mode " +
    "when, find dates), parks named or the headliners (parks 0). Then: ways in; ticket answers " +
    "(not_yet, yes, skipped) and what the requests asked (mode, trip length bucket, parks, " +
    "owned tickets, hopper none/some/all, blocks, rest); answers shown by mode and outcome (ok, " +
    "relaxed with the rule, error with the HTTP status, 0 = no answer), the parks a plan could " +
    "not place; find dates: searches, answers, windows found, windows planned by rank; tickets " +
    "added (picker or combo, by list entry), starts, details opened, adopted, long-stay, a day " +
    "outside its ticket; hopper toggled on and off; views switched to and days opened by kind " +
    "and view; pins, swaps, days sent; what was kept; ticket purchase links by seller kind " +
    "(official, sponsored, plain) and position, surface, ticket mode, locale match, seller and " +
    "product, attributed to a region by the product's catalog (a product no catalog has is " +
    "counted in unattributedTicketLinks); and the regions side by side (a region value park.fan " +
    "does not have is (other)). The server's own record of the plan, when and best-days answers " +
    "is trip_planner_ops. UTC days; up to six minutes behind the site; Do Not Track, GPC and " +
    "opted-out readers are missing. " +
    VISITOR_DATA_DESCRIPTION +
    " " +
    SMALL_CELLS_DESCRIPTION +
    " A region counts as narrowing like a filter. " +
    SMALL_SERIES_DESCRIPTION +
    " " +
    HOURLY_FROM_DESCRIPTION,
  input: {
    ...eventWindowInput,
    region: z
      .enum(REGIONS)
      .optional()
      .describe("Only this region's events; every region's when absent"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(20)
      .describe("Entries per breakdown list"),
    series: z
      .boolean()
      .default(false)
      .describe("Include starts, plan requests and plans shown per period"),
  },
  async handler({ region, limit, series, ...input }, ctx) {
    const catalogs = ctx.get(TicketCatalogService);
    const productRegions = new Map<string, string>();
    for (const config of Object.values(TRIP_REGIONS)) {
      const catalog = await catalogs.catalog(config);
      for (const product of catalog.products) {
        productRegions.set(product.id, config.slug);
      }
    }
    const report = await ctx
      .get(SiteAnalyticsTripService)
      .report(siteQuery(input), {
        region: region ?? null,
        knownRegions: REGIONS,
        productRegions,
        series,
      });

    // The whole planner's counts are as counted; one region's, or a filtered
    // window's, are small cells.
    const narrowed = isNarrowed(input) || Boolean(region);
    const keys = Object.keys(report.totals) as Array<keyof TripTotals>;
    const masked = narrowed
      ? hideSmallInSeries([report.totals], keys)
      : { series: [report.totals], hidden: false };
    const t = masked.series[0] as { [K in keyof TripTotals]: number | null };
    const hide = series
      ? hideSmallInSeries(report.series ?? [], [
          "started",
          "planRequested",
          "planShown",
        ])
      : null;

    const funnel = TRIP_FUNNEL.map((step, i) => {
      const count = t[step];
      const fromPrevious = i === 0 ? null : ratio(count, t[TRIP_FUNNEL[i - 1]]);
      return {
        step,
        events: STEP_EVENTS[step],
        count,
        ofStarted: i === 0 ? null : ratio(count, t.started),
        fromPrevious,
        dropOff:
          fromPrevious !== null && fromPrevious <= 1
            ? round3(1 - fromPrevious)
            : null,
        ...(step === "planRequested" && {
          dates: { known: t.datesKnown, searched: t.datesSearched },
          parks: { named: t.parksNamed, headliners: t.parksHeadliners },
        }),
        ...(step === "planShown" && {
          ok: t.shownOk,
          relaxed: t.shownRelaxed,
          error: t.shownError,
          errorRate: ratio(t.shownError, t.planShown),
        }),
        ...(step === "kept" && {
          daysSaved: t.daysSaved,
          icsExported: t.icsExported,
          linkCopied: t.linkCopied,
        }),
      };
    });

    const cut = <R extends object>(rows: readonly R[], dims: string[]) =>
      foldSmallRows(rows.slice(0, limit), {
        dims,
        count: "count",
        sum: ["count"],
      });
    const values = (rows: readonly ValueCount[]) => cut(rows, ["value"]);

    return withVisitorData(
      {
        ...windowOf(report),
        region: report.region,
        funnel,
        findDates: {
          requested: t.findDatesRequested,
          shown: t.findDatesShown,
          windowsPlanned: t.windowsPlanned,
          plannedPerShown: ratio(t.windowsPlanned, t.findDatesShown),
        },
        tickets: {
          removed: t.ticketRemoved,
          adopted: t.ticketsAdopted,
          longStayUsed: t.longStayUsed,
        },
        actions: {
          daySwapped: t.daySwapped,
          daySent: t.daySent,
          eveningLinkClicked: t.eveningLinkClicked,
        },
        ticketLinks: {
          event: TICKET_LINK_EVENT,
          clicks: t.ticketLinks,
          unattributedTicketLinks: t.unattributedTicketLinks,
        },
        ...((series || narrowed) && {
          smallValuesHidden: (hide?.hidden ?? false) || masked.hidden,
        }),
      },
      {
        regions: foldSmallRows(report.regions, {
          dims: ["region"],
          count: "started",
          sum: [
            "started",
            "planRequested",
            "planShown",
            "errors",
            "kept",
            "ticketLinks",
          ],
        }),
        sources: values(report.sources),
        ticketAnswers: values(report.ticketAnswers),
        requests: {
          mode: values(report.requests.mode),
          days: values(report.requests.days),
          parks: values(report.requests.parks),
          tickets: values(report.requests.tickets),
          hopper: values(report.requests.hopper),
          blocks: values(report.requests.blocks),
          rest: values(report.requests.rest),
        },
        shown: cut(report.shown, ["mode", "outcome"]),
        relaxed: values(report.relaxed),
        unplaced: values(report.unplaced),
        errors: cut(report.errors, ["mode", "status"]),
        findDates: {
          windowsFound: values(report.windowsFound),
          windowRanks: values(report.windowRanks),
        },
        tickets: {
          added: cut(report.ticketsAdded, ["via", "entry"]),
          starts: values(report.ticketStarts),
          detailsOpened: values(report.ticketDetails),
          outsideTicket: values(report.outsideTicket),
        },
        hopper: {
          toggled: values(report.hopperToggled),
          requested: values(report.requests.hopper),
        },
        views: {
          changed: values(report.viewsChanged),
          dayOpened: cut(report.dayOpened, ["kind", "view"]),
          dayOpenedByView: values(report.dayOpenedByView),
        },
        dayPinned: values(report.dayPinned),
        kept: {
          savedDays: values(report.savedDays),
          icsDays: values(report.icsDays),
        },
        ticketLinks: {
          byKindAndPosition: cut(report.ticketLinksByKindAndPosition, [
            "kind",
            "position",
          ]),
          bySurface: values(report.ticketLinksBySurface),
          byTicketMode: values(report.ticketLinksByMode),
          byLocaleMatch: values(report.ticketLinksByLocaleMatch),
          bySeller: cut(report.ticketLinksBySeller, ["seller", "host", "kind"]),
          byProduct: values(report.ticketLinksByProduct),
        },
        ...(hide && { series: hide.series }),
      },
    );
  },
});
