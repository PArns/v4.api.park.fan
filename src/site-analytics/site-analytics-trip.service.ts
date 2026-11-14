import { Injectable } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { EVENT_DIMS, OVERFLOW_VALUE } from "./site-analytics.constants";
import {
  periodStartOf,
  refuseFiltersBeyond,
  seriesPeriods,
  siteAnalyticsWindow,
  type SiteAnalyticsQuery,
} from "./site-analytics.query";
import { SiteReportRunner } from "./site-analytics-report-runner";
import { echo } from "./site-analytics.service";
import { firstDayOf, readEventProps } from "./site-analytics-event-props";
import {
  DAY_PLANNER_OPENED,
  NEW_FUNNEL_EVENTS,
  TICKET_LINK_EVENT,
  TILE_SEEN,
  TRIP_EVENTS,
  tripPlannerFigures,
  TRIP_FUNNEL,
  TRIP_FUNNEL_BASE,
  TRIP_FUNNEL_EVENTS,
  TRIP_LISTS,
  type TripFigureOptions,
  type TripFunnelEra,
  type TripListKey,
  type TripPlannerFigures,
  type TripTotalKey,
} from "./site-analytics-trip";
import { TRIP_REGION_SLUGS, TRIP_REGIONS } from "../trip-regions/regions";
import { TicketCatalogService } from "../trip-regions/ticket-catalog.service";
import { ratio, round3, windowOf } from "../mcp/tools/site-analytics.inputs";

/** Each region park's region by its page (`continent/country/city/slug`), for the tile's views. */
const PARK_REGIONS = new Map(
  Object.values(TRIP_REGIONS).flatMap((region) =>
    region.parks.map(
      (park) =>
        [
          `${park.continent}/${park.country}/${park.city}/${park.slug}`,
          region.slug,
        ] as const,
    ),
  ),
);
const ERAS: readonly TripFunnelEra[] = ["before", "since"];

/** How a list of the answer is read: what names a row, what counts it, what adds up. */
export interface TripRowSpec {
  dims: readonly string[];
  count: string;
  sum: readonly string[];
}

/**
 * How an answer treats small counts. The MCP tool folds small rows and hides
 * small counts, since its reader is a model that passes them on; the admin's
 * own page shows them as counted.
 */
export interface TripAnswerGuard {
  /** The window is narrowed by a filter, a language or the data centres. */
  narrowed: boolean;
  rows: (rows: readonly object[], spec: TripRowSpec) => readonly object[];
  series: <P extends object>(
    series: readonly P[],
    counts: readonly string[],
  ) => { series: P[]; hidden: boolean };
}

/** The admin's guard: every count as counted. */
export const SHOW_EVERY_COUNT: TripAnswerGuard = {
  narrowed: false,
  rows: (rows) => rows,
  series: (series) => ({ series: [...series], hidden: false }),
};

export type SiteTripPlannerReport = ReturnType<typeof echo> &
  TripPlannerFigures & {
    series?: Array<{
      day: string;
      partial?: true;
      started: number;
      requested: number;
      shown: number;
    }>;
  };

/**
 * The trip planner's report over its `trip_*` events and the purchase links
 * of its ticket catalog: the events' properties summed per period and page
 * type over the window (`readEventProps`), the planner tile's views on the
 * region parks' pages by page, and the first day the dates-first
 * planner's events were counted, which splits the funnel between the two
 * flows. The figures are `tripPlannerFigures`'. `answer` shapes them for the
 * MCP tool `trip_planner_report` and `GET /v1/admin/analytics/trip-planner`.
 * docs/admin/mcp.md.
 */
@Injectable()
export class SiteAnalyticsTripService {
  constructor(
    private readonly runner: SiteReportRunner,
    private readonly moduleRef: ModuleRef,
  ) {}

  async report(
    query: SiteAnalyticsQuery,
    options: TripFigureOptions & { series: boolean },
    now = new Date(),
  ): Promise<SiteTripPlannerReport> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      refuseFiltersBeyond(window, "The trip planner report", EVENT_DIMS);
      const [rows, tileViews, newFunnelFrom] = await Promise.all([
        readEventProps(
          this.runner,
          window,
          [
            ...Object.values(TRIP_EVENTS),
            TICKET_LINK_EVENT,
            DAY_PLANNER_OPENED,
          ],
          { period: true, pageType: true },
        ),
        // The tile's views by page, whose park names the region.
        readEventProps(this.runner, window, [TILE_SEEN.name], {
          path: true,
          propsEqual: { section: TILE_SEEN.section },
        }),
        firstDayOf(this.runner, NEW_FUNNEL_EVENTS),
      ]);
      const figures = tripPlannerFigures([...rows, ...tileViews], {
        ...options,
        window: { from: window.from, to: window.to },
        newFunnelFrom,
      });
      if (!options.series) return { ...echo(window), ...figures };

      // The series by the rows' periods, folded into the window's.
      const known = new Set(options.knownRegions);
      const counted = new Map<string, number>();
      for (const row of rows) {
        if (row.period === undefined) continue;
        const value = row.props.region ?? "";
        const region = known.has(value) ? value : OVERFLOW_VALUE;
        if (options.region !== null && region !== options.region) continue;
        const k = `${periodStartOf(row.period, window.interval)}|${row.name}`;
        counted.set(k, (counted.get(k) ?? 0) + row.count);
      }
      return {
        ...echo(window),
        ...figures,
        series: seriesPeriods(window, now).map((point) => ({
          day: point.day,
          ...(point.partial && { partial: true as const }),
          started: counted.get(`${point.day}|${TRIP_EVENTS.started}`) ?? 0,
          requested:
            counted.get(`${point.day}|${TRIP_EVENTS.planRequested}`) ?? 0,
          shown: counted.get(`${point.day}|${TRIP_EVENTS.planShown}`) ?? 0,
        })),
      };
    });
  }

  /**
   * The report as its readers get it: the funnel with its rates, the totals,
   * and the lists cut to `limit`, each purchase link attributed to its region
   * by the ticket catalogs. `head` holds the counts park.fan derived, `lists`
   * the values visitors' browsers sent (paths, property values), which the MCP
   * tool hands on under `visitorData`. The MCP tool `trip_planner_report` and
   * `GET /v1/admin/analytics/trip-planner` both answer this.
   */
  async answer(
    query: SiteAnalyticsQuery,
    {
      region,
      limit,
      series,
    }: { region: string | null; limit: number; series: boolean },
    guard: TripAnswerGuard,
  ) {
    const catalogs = this.moduleRef.get(TicketCatalogService, {
      strict: false,
    });
    const productRegions = new Map<string, string>();
    for (const config of Object.values(TRIP_REGIONS)) {
      const catalog = await catalogs.catalog(config);
      for (const product of catalog.products) {
        productRegions.set(product.id, config.slug);
      }
    }
    const report = await this.report(query, {
      region,
      knownRegions: TRIP_REGION_SLUGS,
      productRegions,
      parkRegions: PARK_REGIONS,
      series,
    });

    // The whole planner's counts are as counted; one region's, or a filtered
    // window's, are small cells: each era's count, each total.
    const flat: Record<string, number | null> = { ...report.totals };
    for (const step of report.funnel) {
      for (const era of ERAS) flat[`${step.step}.${era}`] = step[era];
    }
    const narrowed = guard.narrowed || Boolean(region);
    const masked = narrowed
      ? guard.series([flat], Object.keys(flat))
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
      ? guard.series(report.series ?? [], ["started", "requested", "shown"])
      : null;

    const funnel = TRIP_FUNNEL.map((step) => {
      const base = TRIP_FUNNEL_BASE[step];
      const rate = base ? rateOf(step, base.step) : null;
      return {
        step,
        events: TRIP_FUNNEL_EVENTS[step],
        count: countOf(step),
        before: measured(step, "before") ? era(step, "before") : null,
        since: measured(step, "since") ? era(step, "since") : null,
        ofStarted: base ? rateOf(step, "started") : null,
        ...(base?.share
          ? { ofShown: rate }
          : {
              fromPrevious: rate,
              dropOff: rate !== null && rate <= 1 ? round3(1 - rate) : null,
            }),
        ...(step === "shown" && {
          ok: t("shownOk"),
          relaxed: t("shownRelaxed"),
          error: t("shownError"),
          errorRate: ratio(t("shownError"), countOf("shown")),
        }),
        ...(step === "changed" && {
          editOpened: t("editOpened"),
          optionChanged: t("optionChanged"),
          optionChangedByParky: t("optionChangedByParky"),
        }),
        ...(step === "tickets" && {
          ticketsOpened: t("ticketsOpened"),
          ticketsAnsweredYes: t("ticketsAnsweredYes"),
        }),
        ...(step === "kept" && {
          daysSaved: t("daysSaved"),
          icsExported: t("icsExported"),
          linkCopied: t("linkCopied"),
          dayPlannerOpened: {
            tripDay: t("dayPlannerTripDay"),
            tripFirst: t("dayPlannerTripFirst"),
            hub: t("dayPlannerHub"),
          },
        }),
      };
    });

    const list = (key: TripListKey) =>
      guard.rows(report.lists[key].slice(0, limit), {
        dims: [...TRIP_LISTS[key].props],
        count: "count",
        sum: ["count"],
      });

    return {
      head: {
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
        tile: {
          shown: t("tileShown"),
          clicked: t("tileClicked"),
          plan: t("tilePlan"),
          calendar: t("tileCalendar"),
          clickRate: ratio(t("tileClicked"), t("tileShown")),
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
      lists: {
        regions: guard.rows(report.regions, {
          dims: ["region"],
          count: "started",
          sum: [
            "started",
            "requested",
            "shown",
            "errors",
            "kept",
            "ticketLinks",
          ],
        }),
        pageTypes: guard.rows(
          report.pageTypes.map((row) => ({
            ...row,
            events: row.started + row.requested + row.shown,
          })),
          {
            dims: ["pageType"],
            count: "events",
            sum: ["started", "requested", "shown", "events"],
          },
        ),
        started: {
          sources: list("sources"),
          via: list("startedVia"),
          tile: guard.rows(
            report.tile.map((row) => ({
              ...row,
              clickRate: ratio(row.clicked, row.shown),
            })),
            {
              dims: ["region"],
              count: "shown",
              sum: ["shown", "clicked", "plan", "calendar"],
            },
          ),
          tileClicks: list("tileClicks"),
        },
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
    };
  }
}
