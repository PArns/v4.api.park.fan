import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsLiveService } from "../../site-analytics/site-analytics-live.service";
import { SITE_LOCALES } from "../../site-analytics/site-analytics.constants";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

export const analyticsLiveTool = defineMcpTool({
  name: "analytics_live",
  title: "Site traffic right now",
  description:
    "Who is on park.fan now, from the collector's per-minute counters: distinct visitors with a " +
    "hit in the last five minutes, page views, events and visitors for each of the last 30 whole " +
    "minutes and the current one, the top pages, every country and the devices of the last five " +
    "minutes, and the channels, top referrers and top events of visits and hits in the last 30 " +
    "minutes. A minute's visitors are distinct within that minute only. Nothing older than about " +
    "half an hour is kept; use analytics_overview for any window. " +
    VISITOR_DATA_DESCRIPTION,
  input: {
    locale: z
      .enum(SITE_LOCALES)
      .optional()
      .describe("Only this language of the site; all six when absent"),
  },
  async handler({ locale }, ctx) {
    const live = await ctx.get(SiteAnalyticsLiveService).live(locale ?? null);
    return withVisitorData(
      {
        at: live.at,
        activeVisitors: live.activeVisitors,
        perMinute: live.perMinute,
        countries: live.countries,
        channels: live.channels,
        devices: live.devices,
      },
      { pages: live.pages, referrers: live.referrers, events: live.events },
    );
  },
});
