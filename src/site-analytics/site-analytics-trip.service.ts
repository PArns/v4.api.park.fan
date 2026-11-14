import { Injectable } from "@nestjs/common";
import { EVENT_DIMS, OVERFLOW_VALUE } from "./site-analytics.constants";
import { rowsOf } from "./site-analytics.rollup";
import {
  SqlParams,
  num,
  periodOf,
  periodStartOf,
  periodText,
  refuseFiltersBeyond,
  seriesPeriods,
  siteAnalyticsWindow,
  type Count,
  type SiteAnalyticsQuery,
} from "./site-analytics.query";
import { SiteReportRunner } from "./site-analytics-report-runner";
import { echo } from "./site-analytics.service";
import {
  TICKET_LINK_EVENT,
  TRIP_EVENTS,
  tripPlannerFigures,
  type TripFigureOptions,
  type TripPlannerFigures,
} from "./site-analytics-trip";

/** The events the series draws, per period. */
const DRAWN = [
  TRIP_EVENTS.started,
  TRIP_EVENTS.planRequested,
  TRIP_EVENTS.planShown,
] as const;

export type SiteTripPlannerReport = ReturnType<typeof echo> &
  TripPlannerFigures & {
    series?: Array<{
      day: string;
      partial?: true;
      started: number;
      planRequested: number;
      planShown: number;
    }>;
  };

/**
 * The trip planner's report over its `trip_*` events and the purchase links
 * of its ticket catalog: one statement over the properties summed for the
 * window (`rowsOf`, so whole rolled-up days come from the daily rows), a
 * second for the series when asked. The figures are `tripPlannerFigures`'.
 * Read by the MCP tool `trip_planner_report`, which has the catalog that
 * attributes a purchase link to its region; no admin route serves it yet.
 * docs/admin/mcp.md.
 */
@Injectable()
export class SiteAnalyticsTripService {
  constructor(private readonly runner: SiteReportRunner) {}

  private readonly rolled = (
    source: Parameters<SiteReportRunner["rolledDays"]>[0],
    cuboid: number,
  ) => this.runner.rolledDays(source, cuboid);

  async report(
    query: SiteAnalyticsQuery,
    options: TripFigureOptions & { series: boolean },
    now = new Date(),
  ): Promise<SiteTripPlannerReport> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      refuseFiltersBeyond(window, "The trip planner report", EVENT_DIMS);
      const read = async (
        names: readonly string[],
        collapse: boolean,
        select: string,
      ) => {
        const params = new SqlParams();
        const namesParam = params.add(names);
        const source = await rowsOf(
          this.rolled,
          "events",
          params,
          window,
          "current",
          ["name", "props_key"],
          { alias: "e", collapse },
        );
        return {
          sql: `${select} FROM ${source.from}
                 WHERE e.name = ANY(${namesParam}::text[]) AND ${source.where}`,
          params,
        };
      };

      const props = await read(
        [...Object.values(TRIP_EVENTS), TICKET_LINK_EVENT],
        true,
        "SELECT e.name, e.props, e.count",
      );
      const drawn = options.series
        ? await read(
            DRAWN,
            false,
            `SELECT e.name, e.props->>'region' AS region,
                    ${periodOf(window.hourly, "e")} AS period, e.count`,
          )
        : null;
      const [propRows, seriesRows]: [
        Array<{ name: string; props: Record<string, string>; count: Count }>,
        Array<{
          name: string;
          region: string | null;
          period: string;
          count: Count;
        }>,
      ] = await Promise.all([
        this.runner.query(
          `SELECT name, props, SUM(count) AS count FROM (${props.sql}) s
            GROUP BY name, props`,
          props.params.values,
        ),
        drawn
          ? this.runner.query(
              `SELECT name, region, period, SUM(count) AS count
                 FROM (${drawn.sql}) s
                GROUP BY name, region, period`,
              drawn.params.values,
            )
          : Promise.resolve([]),
      ]);

      const figures = tripPlannerFigures(
        propRows.map((row) => ({
          name: row.name,
          props: row.props ?? {},
          count: num(row.count),
        })),
        options,
      );
      if (!options.series) return { ...echo(window), ...figures };

      const known = new Set(options.knownRegions);
      const counted = new Map<string, number>();
      for (const row of seriesRows) {
        const region =
          row.region && known.has(row.region) ? row.region : OVERFLOW_VALUE;
        if (options.region !== null && region !== options.region) continue;
        const start = periodStartOf(
          periodText(row.period, window.hourly) as string,
          window.interval,
        );
        const k = `${start}|${row.name}`;
        counted.set(k, (counted.get(k) ?? 0) + num(row.count));
      }
      return {
        ...echo(window),
        ...figures,
        series: seriesPeriods(window, now).map((point) => ({
          day: point.day,
          ...(point.partial && { partial: true as const }),
          started: counted.get(`${point.day}|${TRIP_EVENTS.started}`) ?? 0,
          planRequested:
            counted.get(`${point.day}|${TRIP_EVENTS.planRequested}`) ?? 0,
          planShown: counted.get(`${point.day}|${TRIP_EVENTS.planShown}`) ?? 0,
        })),
      };
    });
  }
}
