import { z } from "zod";
import { capped, defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsService } from "../../site-analytics/site-analytics.service";
import {
  changeRatio,
  siteQuery,
  siteWindowInput,
  windowOf,
} from "./site-analytics.inputs";

export const analyticsEventsTool = defineMcpTool({
  name: "analytics_events",
  title: "Site events",
  description:
    "Every event name the site sent in the window or the one before (search_opened, " +
    "search_result_clicked, tab_changed, favorite_add, page_read …), most frequent first, with the " +
    "count, the previous window's count, the change as a ratio, and whether an admin blocked it " +
    "(a blocked name is no longer recorded). Use analytics_event for one event's properties and " +
    "analytics_crosstab to cut it by two sides. UTC days; up to six minutes behind the site; " +
    "Do Not Track, GPC and opted-out readers are missing.",
  input: {
    ...siteWindowInput,
    limit: z
      .number()
      .int()
      .min(1)
      .max(300)
      .default(50)
      .describe("Event names to list, most frequent first"),
    series: z
      .boolean()
      .default(false)
      .describe("Include each event's count per day"),
  },
  async handler({ limit, series, ...input }, ctx) {
    const report = await ctx.get(SiteAnalyticsService).events(siteQuery(input));
    const list = capped(report.items, limit);
    return {
      ...windowOf(report),
      total: list.total,
      items: list.items.map((item) => ({
        name: item.name,
        count: item.count,
        previous: item.previous,
        change: changeRatio(item.count, item.previous),
        ...(item.blocked && { blocked: true }),
        ...(series && { series: item.series }),
      })),
    };
  },
});
