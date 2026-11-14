import { Injectable } from "@nestjs/common";
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
  NEW_FUNNEL_EVENTS,
  TICKET_LINK_EVENT,
  TILE_SEEN,
  TRIP_EVENTS,
  tripPlannerFigures,
  type TripFigureOptions,
  type TripPlannerFigures,
} from "./site-analytics-trip";

export type SiteTripPlannerReport = ReturnType<typeof echo> &
  TripPlannerFigures & {
    series?: Array<{
      day: string;
      partial?: true;
      datesPicked: number;
      planRequested: number;
      planShown: number;
    }>;
  };

/**
 * The trip planner's report over its `trip_*` events and the purchase links
 * of its ticket catalog: the events' properties summed per period and page
 * type over the window (`readEventProps`), the planner tile's views on the
 * region parks' pages by page, and the first day the dates-first
 * planner's events were counted, which splits the funnel between the two
 * flows. The figures are `tripPlannerFigures`'. Read by the MCP tool
 * `trip_planner_report`, which has the catalog that attributes a purchase
 * link to its region; no admin route serves it yet. docs/admin/mcp.md.
 */
@Injectable()
export class SiteAnalyticsTripService {
  constructor(private readonly runner: SiteReportRunner) {}

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
          [...Object.values(TRIP_EVENTS), TICKET_LINK_EVENT],
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
          datesPicked: counted.get(`${point.day}|${TRIP_EVENTS.started}`) ?? 0,
          planRequested:
            counted.get(`${point.day}|${TRIP_EVENTS.planRequested}`) ?? 0,
          planShown: counted.get(`${point.day}|${TRIP_EVENTS.planShown}`) ?? 0,
        })),
      };
    });
  }
}
