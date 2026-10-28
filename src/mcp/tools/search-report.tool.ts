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

export const searchReportTool = defineMcpTool({
  name: "search_report",
  title: "Site search usage",
  description:
    "How park.fan's own search is used, from the search_* events: per surface (hero, header, " +
    "keyboard, search page, the listings' search, the guides panel …) how often it was opened, a " +
    'query typed, a result or a suggestion clicked, a dead end hit (no results) and "view all" ' +
    "used, with clickRate = (result + suggestion clicks) / openings, at most 1, and noResultRate = no results / " +
    "typed; openings by page type and device; which kinds of result were clicked and at which " +
    "position; optionally the openings per period and surface. Search queries themselves are never " +
    "recorded. UTC days; up to six minutes behind the site; Do Not Track, GPC and opted-out readers " +
    "are missing. " +
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
      .default(30)
      .describe("Entries per breakdown list"),
    series: z
      .boolean()
      .default(false)
      .describe("Include openings per period of the interval and surface"),
  },
  async handler({ limit, series, ...input }, ctx) {
    const report = await ctx.get(SiteAnalyticsService).search(siteQuery(input));
    const hide = hideSmallInSeries(report.series, ["opened"]);
    return withVisitorData(
      {
        ...windowOf(report),
        ...(series && { smallValuesHidden: hide.hidden }),
      },
      {
        surfaces: foldSmallRows(report.surfaces, {
          dims: ["surface"],
          count: "opened",
          sum: [
            "opened",
            "typed",
            "resultClicks",
            "browseClicks",
            "noResults",
            "viewAll",
          ],
        }).map((surface) => ({
          ...surface,
          clickRate: round3(surface.clickRate),
          noResultRate: round3(surface.noResultRate),
        })),
        bySurfaceAndPageType: foldSmallRows(
          report.bySurfaceAndPageType.slice(0, limit),
          { dims: ["surface", "pageType"], count: "opened", sum: ["opened"] },
        ),
        bySurfaceAndDevice: foldSmallRows(
          report.bySurfaceAndDevice.slice(0, limit),
          { dims: ["surface", "device"], count: "opened", sum: ["opened"] },
        ),
        resultTypes: foldSmallRows(report.resultTypes.slice(0, limit), {
          dims: ["surface", "result"],
          count: "clicks",
          sum: ["clicks"],
        }),
        positions: foldSmallRows(report.positions, {
          dims: ["position"],
          count: "clicks",
          sum: ["clicks"],
        }),
        browseItems: foldSmallRows(report.browseItems.slice(0, limit), {
          dims: ["surface", "item"],
          count: "clicks",
          sum: ["clicks"],
        }),
        ...(series && { series: hide.series }),
      },
    );
  },
});
