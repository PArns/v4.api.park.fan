import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsService } from "../../site-analytics/site-analytics.service";
import {
  SITE_ANALYTICS_CAVEATS,
  changeRatio,
  round3,
  siteQuery,
  siteWindowInput,
  windowOf,
} from "./site-analytics.inputs";
import type { SiteTotalsDto } from "../../site-analytics/dto/site-analytics.dto";

function totals(t: SiteTotalsDto) {
  return {
    pageviews: t.pageviews,
    visits: t.visits,
    visitors: t.visitors,
    events: t.events,
    viewsPerVisit: round3(t.viewsPerVisit),
    bounceRate: round3(t.bounceRate),
  };
}

export const analyticsOverviewTool = defineMcpTool({
  name: "analytics_overview",
  title: "Site traffic overview",
  description:
    "park.fan's traffic over a window: page views, visits, visitors, events, views per visit and " +
    "bounce rate, the same for the window before with the change as a ratio (0.25 = +25 %; both " +
    "null beyond 180 days), the " +
    "active visitors of the last five minutes and page views of the last 30, and optionally a " +
    "point per day (per hour when days is 1). Start here, then narrow with analytics_breakdown. " +
    SITE_ANALYTICS_CAVEATS,
  input: {
    ...siteWindowInput,
    series: z
      .boolean()
      .default(false)
      .describe("Include a point per day (per hour for one day)"),
  },
  async handler({ series, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsService)
      .overview(siteQuery(input));
    return {
      ...windowOf(report),
      totals: totals(report.totals),
      previous: report.previous ? totals(report.previous) : null,
      change: report.previous
        ? {
            pageviews: changeRatio(
              report.totals.pageviews,
              report.previous.pageviews,
            ),
            visits: changeRatio(report.totals.visits, report.previous.visits),
            events: changeRatio(report.totals.events, report.previous.events),
          }
        : null,
      realtime: {
        activeVisitors: report.realtime.activeVisitors,
        pageviewsLast30Minutes: report.realtime.perMinute.reduce(
          (sum, minute) => sum + minute.pageviews,
          0,
        ),
      },
      ...(series && { series: report.series }),
    };
  },
});
