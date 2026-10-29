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
  isNarrowed,
} from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

/** A value → count list, its small values folded. */
const counts = (items: Array<{ value: string; count: number }>) =>
  foldSmallRows(items, { dims: ["value"], count: "count", sum: ["count"] });

/** The report's counts outside its lists, each a small cell when narrowed. */
const TOTALS = [
  "opened",
  "started",
  "optimized",
  "shared",
  "closedEmpty",
  "startedInOpening",
  "startedPanelShut",
  "wizardFinished",
  "wizardWithHeadliners",
  "dayCleared",
  "sharedAdopted",
  "pushOn",
];

/** The figures worked out from those counts, and the counts each comes from. */
const DERIVED = {
  startRate: ["opened", "startedInOpening"],
  optimizedPerStarted: ["started", "optimized"],
  sharedPerStarted: ["started", "shared"],
};

/** A ranked park or ride list: the ones still known, folded. */
const ranked = (
  items: Array<{
    id: string;
    name: string | null;
    url: string | null;
    count: number;
  }>,
  limit: number,
) =>
  foldSmallRows(items.filter((item) => item.name !== null).slice(0, limit), {
    dims: ["id", "name", "url"],
    count: "count",
    sum: ["count"],
  });

export const plannerReportTool = defineMcpTool({
  name: "planner_report",
  title: "Trip planner usage",
  description:
    "How park.fan's trip planner is used, from its events: the funnel (openings of the panel, " +
    "days started with a first block, optimisations, plan links shared, openings closed empty) " +
    "with startedInOpening (days begun in an opening) and startedPanelShut (days begun from a " +
    "ride page with the panel shut; days from before the frontend sent a source are in neither), " +
    "startRate = startedInOpening / opened (at most 1), and optimizedPerStarted and " +
    "sharedPerStarted, counts per started day that can exceed 1; every way in (source) with the " +
    "days started in an opening that came that way; the " +
    "wizard's steps reached and finishes; blocks added and removed by kind (ride, show, block) " +
    "and source; the optimiser's modes, the fit assistant and its levers, undo, cleared days; " +
    "shared plans opened (by outcome) and adopted, push switched on; openings by page type and " +
    "device; the parks with the most days started and the rides added most (at most 20 each), " +
    "with names, a merged park or ride counted for the one it was merged into and one no longer " +
    "known left out and counted in unresolvedEntities. UTC " +
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
      .describe("Entries per breakdown list; the parks and rides stop at 20"),
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
    const figures = {
      ...funnel,
      startRate: round3(funnel.startRate),
      optimizedPerStarted: round3(funnel.optimizedPerStarted),
      sharedPerStarted: round3(funnel.sharedPerStarted),
      wizardFinished: report.wizardFinished,
      wizardWithHeadliners: report.wizardWithHeadliners,
      dayCleared: report.dayCleared,
      sharedAdopted: report.sharedAdopted,
      pushOn: report.pushOn,
    };
    // The whole site's figures are as counted; narrowed, each is a small cell.
    const narrowed = isNarrowed(input);
    const totals = narrowed
      ? hideSmallInSeries([figures], TOTALS, DERIVED)
      : { series: [figures], hidden: false };
    const {
      wizardFinished,
      wizardWithHeadliners,
      dayCleared,
      sharedAdopted,
      pushOn,
      ...shownFunnel
    } = totals.series[0];
    const known = [...report.topParks, ...report.topRides];
    return withVisitorData(
      {
        ...windowOf(report),
        funnel: shownFunnel,
        wizardFinished,
        wizardWithHeadliners,
        dayCleared,
        sharedAdopted,
        pushOn,
        unresolvedEntities: known.filter((item) => item.name === null).length,
        ...((series || narrowed) && {
          smallValuesHidden: (series && hide.hidden) || totals.hidden,
        }),
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
        topParks: ranked(report.topParks, limit),
        topRides: ranked(report.topRides, limit),
        ...(series && { series: hide.series }),
      },
    );
  },
});
