import { z } from "zod";
import { capped, defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsService } from "../../site-analytics/site-analytics.service";
import {
  HOURLY_FROM_DESCRIPTION,
  changeRatio,
  siteQuery,
  eventWindowInput,
  windowOf,
} from "./site-analytics.inputs";
import {
  SMALL_CELLS_DESCRIPTION,
  SMALL_SERIES_DESCRIPTION,
  foldSmallRows,
  hideSmallInSeries,
  hideSmallTotal,
  isNarrowed,
} from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

export const analyticsEventsTool = defineMcpTool({
  name: "analytics_events",
  title: "Site events",
  description:
    "Every event name the site sent in the window or the one before (search_opened, " +
    "search_result_clicked, tab_changed, favorite_add, page_read …), most frequent first, with the " +
    "count, the previous window's count and the change as a ratio (null beyond 180 days), and " +
    "whether an admin blocked it " +
    "(a blocked name is no longer recorded). Use analytics_event for one event's properties and " +
    "analytics_crosstab to cut it by two sides. UTC days; up to six minutes behind the site; " +
    "Do Not Track, GPC and opted-out readers are missing. " +
    VISITOR_DATA_DESCRIPTION +
    " " +
    SMALL_CELLS_DESCRIPTION +
    " The series of an event under (other) is null. " +
    SMALL_SERIES_DESCRIPTION +
    " " +
    HOURLY_FROM_DESCRIPTION,
  input: {
    ...eventWindowInput,
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
      .describe("Include each event's count per period of the interval"),
  },
  async handler({ limit, series, ...input }, ctx) {
    const report = await ctx.get(SiteAnalyticsService).events(siteQuery(input));
    const list = capped(report.items, limit);
    let hidden = false;
    const items = foldSmallRows(list.items, {
      dims: ["name"],
      count: "count",
      sum: ["count", "previous"],
    }).map((item) => {
      const hide = item.series
        ? hideSmallInSeries(item.series, ["count"])
        : null;
      hidden ||= hide?.hidden ?? false;
      return {
        name: item.name,
        count: item.count,
        previous: item.previous,
        change: changeRatio(item.count, item.previous),
        ...(item.blocked && { blocked: true }),
        ...(series && { series: hide ? hide.series : null }),
      };
    });
    const total = hideSmallTotal(list.total, input);
    return withVisitorData(
      {
        ...windowOf(report),
        total: total.total,
        ...((series || isNarrowed(input)) && {
          smallValuesHidden: hidden || total.hidden,
        }),
      },
      { items },
    );
  },
});
