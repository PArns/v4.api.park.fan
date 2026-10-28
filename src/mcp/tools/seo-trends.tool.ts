import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsSeoService } from "../../site-analytics/site-analytics-seo.service";
import { CHANNELS } from "../../site-analytics/site-analytics.constants";
import { siteQuery, siteWindowInput, windowOf } from "./site-analytics.inputs";

export const seoTrendsTool = defineMcpTool({
  name: "seo_trends",
  title: "SEO risers and fallers",
  description:
    "Landing pages of one channel (search by default) that gained and lost the most visits against " +
    "the window of the same length right before: path, page type, visits, previous, change " +
    "(absolute visits). Only pages with at least 10 visits over both windows, so noise does not " +
    "rank. Use a window of 7 or 28 days to compare like weekdays. UTC days; up to six minutes " +
    "behind the site; Do Not Track, GPC and opted-out readers are missing.",
  input: {
    channel: z
      .enum(CHANNELS)
      .default("search")
      .describe("Where the visits came from"),
    ...siteWindowInput,
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
    return {
      ...windowOf(report),
      channel: report.channel,
      rising: report.rising.slice(0, limit),
      falling: report.falling.slice(0, limit),
    };
  },
});
