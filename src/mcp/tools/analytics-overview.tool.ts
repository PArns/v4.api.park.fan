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
import { MCP_MIN_CELL, hideSmallInSeries, isNarrowed } from "./small-cells";
import type { SiteTotalsDto } from "../../site-analytics/dto/site-analytics.dto";

function totals(t: SiteTotalsDto) {
  return {
    pageviews: t.pageviews,
    visits: t.visits,
    visitors: t.visitors,
    events: t.events,
    viewsPerVisit: round3(t.viewsPerVisit),
    bounceRate: round3(t.bounceRate),
    avgVisitSeconds: t.avgVisitSeconds,
    avgTimeOnPageSeconds: t.avgTimeOnPageSeconds,
  };
}

export const analyticsOverviewTool = defineMcpTool({
  name: "analytics_overview",
  title: "Site traffic overview",
  description:
    "park.fan's traffic over a window: page views, visits, visitors, events, views per visit, " +
    "bounce rate, average visit and average time on page in seconds, the same for the window " +
    "before with the change as a ratio (0.25 = +25 %; both null beyond 180 days), the " +
    "active visitors of the last five minutes and page views of the last 30, and optionally a " +
    "point per period of the interval with every one of those figures. Start here, then narrow " +
    "with analytics_breakdown; analytics_live shows the last half hour minute by minute. " +
    `Under a filter or a language the series has its values from 1 to ${MCP_MIN_CELL - 1} (and the ` +
    "rates and averages computed from them) as null, and smallValuesHidden says whether any were; " +
    "totals and a series of the whole site are as counted. " +
    SITE_ANALYTICS_CAVEATS,
  input: {
    ...siteWindowInput,
    series: z
      .boolean()
      .default(false)
      .describe(
        "Include a point per period of the interval (an hour for one day, a day up to 92 days)",
      ),
  },
  async handler({ series, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsService)
      .overview(siteQuery(input));
    const points = report.series.map((point) => ({
      ...point,
      viewsPerVisit: round3(point.viewsPerVisit),
      bounceRate: round3(point.bounceRate),
    }));
    // A series of the whole site's totals is as it is; under a filter or a
    // language each point is a small cell.
    const shown = isNarrowed(input)
      ? hideSmallInSeries(
          points,
          ["pageviews", "visits", "visitors", "events"],
          {
            viewsPerVisit: ["visits", "pageviews"],
            bounceRate: ["visits"],
            avgVisitSeconds: ["visits"],
            avgTimeOnPageSeconds: ["pageviews"],
          },
        )
      : { series: points, hidden: false };
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
      ...(series && {
        series: shown.series,
        ...(isNarrowed(input) && { smallValuesHidden: shown.hidden }),
      }),
    };
  },
});
