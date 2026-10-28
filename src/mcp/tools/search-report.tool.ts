import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsService } from "../../site-analytics/site-analytics.service";
import {
  round3,
  siteQuery,
  siteWindowInput,
  windowOf,
} from "./site-analytics.inputs";

export const searchReportTool = defineMcpTool({
  name: "search_report",
  title: "Site search usage",
  description:
    "How park.fan's own search is used, from the search_* events: per surface (hero, header, " +
    "keyboard, search page, the listings' search, the guides panel …) how often it was opened, a " +
    'query typed, a result or a suggestion clicked, a dead end hit (no results) and "view all" ' +
    "used, with clickRate = (result + suggestion clicks) / openings and noResultRate = no results / " +
    "typed; openings by page type and viewport; which kinds of result were clicked and at which " +
    "position; optionally the openings per day and surface. Search queries themselves are never " +
    "recorded. UTC days; up to six minutes behind the site; Do Not Track, GPC and opted-out readers " +
    "are missing.",
  input: {
    ...siteWindowInput,
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
      .describe("Include openings per day and surface"),
  },
  async handler({ limit, series, ...input }, ctx) {
    const report = await ctx.get(SiteAnalyticsService).search(siteQuery(input));
    return {
      ...windowOf(report),
      surfaces: report.surfaces.map((surface) => ({
        ...surface,
        clickRate: round3(surface.clickRate),
        noResultRate: round3(surface.noResultRate),
      })),
      bySurfaceAndPageType: report.bySurfaceAndPageType.slice(0, limit),
      bySurfaceAndViewport: report.bySurfaceAndViewport.slice(0, limit),
      resultTypes: report.resultTypes.slice(0, limit),
      positions: report.positions,
      browseItems: report.browseItems.slice(0, limit),
      ...(series && { series: report.series }),
    };
  },
});
