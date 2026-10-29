import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsService } from "../../site-analytics/site-analytics.service";
import {
  HOURLY_FROM_DESCRIPTION,
  round3,
  siteQuery,
  eventWindowInput,
  windowOf,
} from "./site-analytics.inputs";
import {
  SMALL_CELLS_DESCRIPTION,
  SMALL_SERIES_DESCRIPTION,
  foldSmallRows,
  hideSmallInSeries,
} from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

/** A value → count list, its small values folded. */
const counts = (items: Array<{ value: string; count: number }>) =>
  foldSmallRows(items, { dims: ["value"], count: "count", sum: ["count"] });

export const plannerReportTool = defineMcpTool({
  name: "planner_report",
  title: "Trip planner usage",
  description:
    "How park.fan's trip planner is used, from its events: the funnel (openings of the panel, " +
    "days started with a first block, optimisations, plan links shared, openings closed empty) " +
    "with startRate = started / opened, optimizeRate and shareRate per started day, each at most " +
    "1; every way in (source) with the days started in an opening that came that way; the " +
    "wizard's steps reached and finishes; blocks added and removed by kind (ride, show, block) " +
    "and source; the optimiser's modes, the fit assistant and its levers, undo, cleared days; " +
    "shared plans opened (by outcome) and adopted, push switched on; openings by page type and " +
    "device; the parks with the most days started and the rides added most, with names. A day " +
    "can start without the panel open (a ride page's button), so started can exceed opened. UTC " +
    "days; up to six minutes behind the site; Do Not Track, GPC and opted-out readers are " +
    "missing. " +
    VISITOR_DATA_DESCRIPTION +
    " " +
    SMALL_CELLS_DESCRIPTION +
    " " +
    SMALL_SERIES_DESCRIPTION +
    " " +
    HOURLY_FROM_DESCRIPTION,
  input: {
    ...eventWindowInput,
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
      .describe("Include openings, started days and optimisations per period"),
  },
  async handler({ limit, series, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsService)
      .planner(siteQuery(input));
    const hide = hideSmallInSeries(report.series, [
      "opened",
      "started",
      "optimized",
    ]);
    const { funnel } = report;
    return withVisitorData(
      {
        ...windowOf(report),
        funnel: {
          ...funnel,
          startRate: round3(funnel.startRate),
          optimizeRate: round3(funnel.optimizeRate),
          shareRate: round3(funnel.shareRate),
        },
        wizardFinished: report.wizardFinished,
        wizardWithHeadliners: report.wizardWithHeadliners,
        dayCleared: report.dayCleared,
        sharedAdopted: report.sharedAdopted,
        pushOn: report.pushOn,
        ...(series && { smallValuesHidden: hide.hidden }),
      },
      {
        sources: foldSmallRows(report.sources, {
          dims: ["source"],
          count: "opened",
          sum: ["opened", "started"],
        }).map((source) => ({
          ...source,
          startRate: round3(source.startRate),
        })),
        wizardSteps: counts(report.wizardSteps),
        items: foldSmallRows(report.items.slice(0, limit), {
          dims: ["action", "kind", "source"],
          count: "count",
          sum: ["count"],
        }),
        optimized: counts(report.optimized),
        fitOpened: counts(report.fitOpened),
        fitLevers: counts(report.fitLevers),
        undo: counts(report.undo),
        sharedOpened: counts(report.sharedOpened),
        byPageTypeAndDevice: foldSmallRows(
          report.byPageTypeAndDevice.slice(0, limit),
          { dims: ["pageType", "device"], count: "opened", sum: ["opened"] },
        ),
        topParks: foldSmallRows(report.topParks.slice(0, limit), {
          dims: ["id", "name", "url"],
          count: "count",
          sum: ["count"],
        }),
        topRides: foldSmallRows(report.topRides.slice(0, limit), {
          dims: ["id", "name", "url"],
          count: "count",
          sum: ["count"],
        }),
        ...(series && { series: hide.series }),
      },
    );
  },
});
