import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsTripService } from "../../site-analytics/site-analytics-trip.service";
import {
  TICKET_LINK_EVENT,
  TRIP_FUNNEL,
  TRIP_FUNNEL_EVENTS,
  TRIP_LISTS,
  type TripFunnelEra,
  type TripListKey,
  type TripTotalKey,
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
const ERAS: readonly TripFunnelEra[] = ["before", "since"];

/** A ratio to three places, null without both counts or a base. */
const ratio = (count: number | null, base: number | null): number | null =>
  count === null || base === null || base === 0 ? null : round3(count / base);

export const tripPlannerReportTool = defineMcpTool({
  name: "trip_planner_report",
  title: "Trip planner (vacation) usage",
  description:
    "How park.fan's multi-day trip planner (the region hubs) is used, from its trip_* events and " +
    "ticket_link_clicked, per region or for all (`region`). Not the day planner: that is " +
    "planner_report. The funnel of the dates-first planner (release 2.21.0): datesPicked " +
    "(trip_planner_started) → planShown (trip_plan_shown) → changed (trip_edit_opened, " +
    "trip_option_changed by the visitor) → ticketsEntered (trip_tickets_opened) → kept (days " +
    "saved to the day planner, .ics exported, link copied), each with ofDatesPicked, " +
    "fromPrevious and dropOff (1 − fromPrevious, null where a step outnumbers the one before). " +
    "newFunnelFrom is the first UTC day the dates-first planner's events were counted; each " +
    "step counts its `before` days by the old planner's events (a start, an answer of yes to " +
    "the ticket question; nothing for changed) and its `since` days by the new ones, and every " +
    "rate is taken over the days both steps count, so a window across the deploy still adds " +
    "up. Counts are events, not visitors: an answer fires once per distinct request. Then: ways " +
    "in (source, via), the requests (mode dates or when, trip length bucket, parks with 0 for " +
    "the headliners, owned tickets, hopper none/some/all, blocks, rest), answers by mode and " +
    "outcome (ok, relaxed with the rule, error with the HTTP status, 0 = no answer), parks left " +
    "out and the fix chosen, cities of a route, find dates (toggles, periods, windows found and " +
    "planned by rank), the edits opened (panel, from) and options changed (option, value, by " +
    "visitor or parky), the stay suggestion, ticket openings (from, first) and entries, hopper, " +
    "the views switched to and seen, the date grid's resort mode, days opened, pins, the " +
    "accuracy note, what was kept, ticket purchase links by seller kind (official, sponsored, " +
    "plain) and position, surface, mode, locale match, seller and product, attributed to a " +
    "region by the product's catalog (a product no catalog has is in unattributedTicketLinks); " +
    "the regions side by side (a region value park.fan does not have is (other)) and the page " +
    "types the planner was used from (trip_hub, trip_month, trip_best_time, other …). An event " +
    "the frontend does not send yet lists nothing. The server's own record of the plan, when " +
    "and best-days answers is trip_planner_ops. UTC days; up to six minutes behind the site; Do " +
    "Not Track, GPC and opted-out readers are missing. " +
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
      .describe(
        "Include dates picked, plan requests and plans shown per period",
      ),
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
    // window's, are small cells: each era's count, each total.
    const flat: Record<string, number | null> = { ...report.totals };
    for (const step of report.funnel) {
      for (const era of ERAS) flat[`${step.step}.${era}`] = step[era];
    }
    const narrowed = isNarrowed(input) || Boolean(region);
    const masked = narrowed
      ? hideSmallInSeries([flat], Object.keys(flat))
      : { series: [flat], hidden: false };
    const v = masked.series[0];
    const t = (key: TripTotalKey) => v[key] ?? null;
    const era = (step: string, e: TripFunnelEra) => v[`${step}.${e}`] ?? null;
    const measured = (step: string, e: TripFunnelEra) =>
      report.funnel.find((s) => s.step === step)?.[e] !== null;
    /** A step's count over the eras it counts; null when one of them is hidden. */
    const countOf = (step: string, eras = ERAS) => {
      const counted = eras.filter((e) => measured(step, e));
      if (counted.length === 0) return null;
      const values = counted.map((e) => era(step, e));
      return values.some((x) => x === null)
        ? null
        : (values as number[]).reduce((a, b) => a + b, 0);
    };
    /** `step` over `base`, over the eras both count. */
    const rateOf = (step: string, base: string) => {
      const both = ERAS.filter((e) => measured(step, e) && measured(base, e));
      return both.length === 0
        ? null
        : ratio(countOf(step, both), countOf(base, both));
    };
    const hide = series
      ? hideSmallInSeries(report.series ?? [], [
          "datesPicked",
          "planRequested",
          "planShown",
        ])
      : null;

    const funnel = TRIP_FUNNEL.map((step, i) => {
      const fromPrevious = i === 0 ? null : rateOf(step, TRIP_FUNNEL[i - 1]);
      return {
        step,
        events: TRIP_FUNNEL_EVENTS[step],
        count: countOf(step),
        before: measured(step, "before") ? era(step, "before") : null,
        since: measured(step, "since") ? era(step, "since") : null,
        ofDatesPicked: i === 0 ? null : rateOf(step, "datesPicked"),
        fromPrevious,
        dropOff:
          fromPrevious !== null && fromPrevious <= 1
            ? round3(1 - fromPrevious)
            : null,
        ...(step === "planShown" && {
          ok: t("shownOk"),
          relaxed: t("shownRelaxed"),
          error: t("shownError"),
          errorRate: ratio(t("shownError"), countOf("planShown")),
        }),
        ...(step === "changed" && {
          editOpened: t("editOpened"),
          optionChanged: t("optionChanged"),
          optionChangedByParky: t("optionChangedByParky"),
        }),
        ...(step === "ticketsEntered" && {
          ticketsOpened: t("ticketsOpened"),
          ticketsAnsweredYes: t("ticketsAnsweredYes"),
        }),
        ...(step === "kept" && {
          daysSaved: t("daysSaved"),
          icsExported: t("icsExported"),
          linkCopied: t("linkCopied"),
        }),
      };
    });

    const list = (key: TripListKey) =>
      foldSmallRows(report.lists[key].slice(0, limit), {
        dims: [...TRIP_LISTS[key].props],
        count: "count",
        sum: ["count"],
      });

    return withVisitorData(
      {
        ...windowOf(report),
        region: report.region,
        newFunnelFrom: report.newFunnelFrom,
        funnel,
        requests: {
          planRequested: t("planRequested"),
          datesKnown: t("datesKnown"),
          datesSearched: t("datesSearched"),
          parksHeadliners: t("parksHeadliners"),
        },
        findDates: {
          searched: t("datesSearched"),
          shown: t("findDatesShown"),
          windowsPlanned: t("windowsPlanned"),
          plannedPerShown: ratio(t("windowsPlanned"), t("findDatesShown")),
        },
        tickets: {
          removed: t("ticketRemoved"),
          adopted: t("ticketsAdopted"),
          longStayUsed: t("longStayUsed"),
        },
        views: { switched: t("viewChanged"), dayOpened: t("dayOpened") },
        actions: {
          daySwapped: t("daySwapped"),
          daySent: t("daySent"),
          eveningLinkClicked: t("eveningLinkClicked"),
        },
        ticketLinks: {
          event: TICKET_LINK_EVENT,
          clicks: t("ticketLinks"),
          unattributedTicketLinks: t("unattributedTicketLinks"),
        },
        ...((series || narrowed) && {
          smallValuesHidden: (hide?.hidden ?? false) || masked.hidden,
        }),
      },
      {
        regions: foldSmallRows(report.regions, {
          dims: ["region"],
          count: "datesPicked",
          sum: [
            "datesPicked",
            "planRequested",
            "planShown",
            "errors",
            "kept",
            "ticketLinks",
          ],
        }),
        pageTypes: foldSmallRows(
          report.pageTypes.map((row) => ({
            ...row,
            events: row.datesPicked + row.planRequested + row.planShown,
          })),
          {
            dims: ["pageType"],
            count: "events",
            sum: ["datesPicked", "planRequested", "planShown", "events"],
          },
        ),
        datesPicked: { sources: list("sources"), via: list("startedVia") },
        requests: {
          mode: list("requestMode"),
          days: list("requestDays"),
          parks: list("requestParks"),
          tickets: list("requestTickets"),
          hopper: list("requestHopper"),
          blocks: list("requestBlocks"),
          rest: list("requestRest"),
        },
        answers: {
          shown: list("shown"),
          relaxed: list("relaxed"),
          unplaced: list("unplaced"),
          leftOut: list("leftOut"),
          leftOutFixes: list("leftOutFixes"),
          stays: list("stays"),
          errors: list("errors"),
        },
        findDates: {
          toggled: list("findToggled"),
          periods: list("findPeriods"),
          windowsFound: list("windowsFound"),
          windowRanks: list("windowRanks"),
        },
        changes: {
          edits: list("edits"),
          options: list("options"),
          staySuggestion: list("staySuggestion"),
          dayPinned: list("dayPinned"),
          pinsActions: list("pinsActions"),
        },
        tickets: {
          answers: list("ticketAnswers"),
          opened: list("ticketsOpened"),
          added: list("ticketsAdded"),
          starts: list("ticketStarts"),
          detailsOpened: list("ticketDetails"),
          outsideTicket: list("outsideTicket"),
        },
        hopper: {
          toggled: list("hopperToggled"),
          requested: list("requestHopper"),
        },
        views: {
          switched: list("viewsChanged"),
          seen: list("viewsSeen"),
          calendarMode: list("calendarMode"),
          dayOpened: list("dayOpened"),
          dayOpenedByView: list("dayOpenedByView"),
        },
        accuracyOpened: list("accuracyOpened"),
        kept: { savedDays: list("savedDays"), icsDays: list("icsDays") },
        ticketLinks: {
          byKindAndPosition: list("linksByKindAndPosition"),
          bySurface: list("linksBySurface"),
          byTicketMode: list("linksByMode"),
          byLocaleMatch: list("linksByLocaleMatch"),
          bySeller: list("linksBySeller"),
          byProduct: list("linksByProduct"),
        },
        ...(hide && { series: hide.series }),
      },
    );
  },
});
