import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsLiveService } from "../../site-analytics/site-analytics-live.service";
import { SITE_LOCALES } from "../../site-analytics/site-analytics.constants";
import {
  MCP_MIN_CELL,
  SMALL_CELLS_DESCRIPTION,
  foldSmallRows,
  hideSmallInSeries,
} from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";
import { datacenterInput } from "./site-analytics.inputs";

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
    VISITOR_DATA_DESCRIPTION +
    " " +
    SMALL_CELLS_DESCRIPTION +
    ` With locale, a minute's values from 1 to ${MCP_MIN_CELL - 1} are null and ` +
    "smallValuesHidden says whether any were; the minutes of the whole site are as counted.",
  input: {
    locale: z
      .enum(SITE_LOCALES)
      .optional()
      .describe("Only this language of the site; all six when absent"),
    datacenter: datacenterInput,
  },
  async handler({ locale, datacenter }, ctx) {
    const live = await ctx
      .get(SiteAnalyticsLiveService)
      .live(locale ?? null, new Date(), datacenter ?? "exclude");
    // A minute of the whole site is a total; one language's is a slice.
    const minutes = locale
      ? hideSmallInSeries(live.perMinute, ["pageviews", "events", "visitors"])
      : { series: live.perMinute, hidden: false };
    const fold = (count: string) => ({ count, sum: [count] });
    return withVisitorData(
      {
        at: live.at,
        activeVisitors: live.activeVisitors,
        perMinute: minutes.series,
        ...(locale && { smallValuesHidden: minutes.hidden }),
        countries: foldSmallRows(live.countries, {
          dims: ["country"],
          ...fold("views"),
        }),
        channels: foldSmallRows(live.channels, {
          dims: ["channel"],
          ...fold("visits"),
        }),
        devices: foldSmallRows(live.devices, {
          dims: ["device"],
          ...fold("views"),
        }),
      },
      {
        pages: foldSmallRows(live.pages, { dims: ["path"], ...fold("views") }),
        referrers: foldSmallRows(live.referrers, {
          dims: ["referrer"],
          ...fold("visits"),
        }),
        events: foldSmallRows(live.events, {
          dims: ["name"],
          ...fold("count"),
        }),
      },
    );
  },
});
