import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsService } from "../../site-analytics/site-analytics.service";
import { BREAKDOWN_DIMS } from "../../site-analytics/site-analytics.constants";
import {
  FILTER_DIM_HELP,
  SITE_ANALYTICS_CAVEATS,
  round3,
  siteQuery,
  siteWindowInput,
  windowOf,
} from "./site-analytics.inputs";

export const analyticsBreakdownTool = defineMcpTool({
  name: "analytics_breakdown",
  title: "Traffic by one dimension",
  description:
    "Page views and visits per value of one dimension, largest first, with each value's share of " +
    "the total: which pages, page types, countries, devices, browsers, languages — or, for the " +
    "visit dimensions entry, referrer, channel and campaign, where visits came from and landed, " +
    "counted as visits started (measure: visits, a view is a visit) with a bounce rate each. A " +
    "filter on a visit dimension switches any breakdown to visits started: dim=path with " +
    "channel=search lists the pages search visits entered on (not every page they read later), " +
    "dim=referrer with channel=ai which AI assistants send readers. Direct visits have no " +
    "referrer. " +
    SITE_ANALYTICS_CAVEATS,
  input: {
    dim: z
      .enum(BREAKDOWN_DIMS)
      .describe(`The dimension to group by: ${FILTER_DIM_HELP}`),
    ...siteWindowInput,
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(20)
      .describe("Values to list, largest first"),
  },
  async handler({ dim, limit, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsService)
      .breakdown({ ...siteQuery(input), dim, limit });
    return {
      ...windowOf(report),
      dim: report.dim,
      measure: report.measure,
      total: report.total,
      items: report.items.map((item) => ({
        value: item.value,
        views: item.views,
        visits: item.visits,
        share: round3(item.share),
        ...(item.bounceRate !== undefined && {
          bounceRate: round3(item.bounceRate),
        }),
      })),
    };
  },
});
