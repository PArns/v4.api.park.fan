import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsSeoService } from "../../site-analytics/site-analytics-seo.service";
import type { SiteTrendDto } from "../../site-analytics/dto/site-analytics.dto";
import { CHANNELS } from "../../site-analytics/site-analytics.constants";
import {
  HOURLY_FROM_DESCRIPTION,
  comparableWindowInput,
  siteQuery,
  windowOf,
} from "./site-analytics.inputs";
import { SMALL_CELLS_DESCRIPTION, foldSmallRows } from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

/** The pages of one direction, those under the small-value limit folded. */
const foldTrends = (pages: SiteTrendDto[]) =>
  foldSmallRows(pages, {
    dims: ["path"],
    count: "visits",
    sum: ["visits", "previous", "change"],
  });

export const seoTrendsTool = defineMcpTool({
  name: "seo_trends",
  title: "SEO risers and fallers",
  description:
    "Landing pages of one channel (search by default) that gained and lost the most visits against " +
    "the window of the same length right before: path, page type, visits, previous, change " +
    "(absolute visits) and the window's average visit in seconds. Only pages with at least 10 visits over both windows, so noise does not " +
    "rank. Use a window of 7 or 28 days to compare like weekdays; at most 180. The window before " +
    "ends at the current hour, like today. UTC days; up to six minutes " +
    "behind the site; Do Not Track, GPC and opted-out readers are missing. " +
    VISITOR_DATA_DESCRIPTION +
    " " +
    SMALL_CELLS_DESCRIPTION +
    " A page counts by its visits in the window; previous is shown as it is. " +
    HOURLY_FROM_DESCRIPTION,
  input: {
    channel: z
      .enum(CHANNELS)
      .default("search")
      .describe("Where the visits came from"),
    ...comparableWindowInput,
    limit: z
      .number()
      .int()
      .min(1)
      .max(25)
      .default(15)
      .describe("Pages per direction"),
  },
  async handler({ channel, limit, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsSeoService)
      .trends({ ...siteQuery(input), channel });
    return withVisitorData(
      { ...windowOf(report), channel: report.channel },
      {
        rising: foldTrends(report.rising.slice(0, limit)),
        falling: foldTrends(report.falling.slice(0, limit)),
      },
    );
  },
});
