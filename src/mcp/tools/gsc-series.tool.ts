import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SearchConsoleService } from "../../search-console/search-console.service";
import { GSC_INTERVALS } from "../../search-console/search-console.constants";
import { round3 } from "./site-analytics.inputs";
import {
  GSC_CAVEATS,
  gscCountryFilter,
  gscDeviceFilter,
  gscPageFilter,
  gscQuery,
  gscQueryFilter,
  gscWindowInput,
  readable,
} from "./gsc-data";

export const gscSeriesTool = defineMcpTool({
  name: "gsc_series",
  title: "Search Console over time",
  description:
    "Clicks, impressions, ctr and average position of park.fan in Google per day, week or " +
    "month, from the window's first day to the last day with data; a period the window or the " +
    "data cuts is partial: true. Reach for it to see a trend or a drop after a change; filter " +
    "by page or query to follow one of them. The figures include queries Google anonymises; under a country or device filter they stay complete, but with both filters, a page or a language as well they are visible queries only and partialBreakdown: true says so. " +
    GSC_CAVEATS,
  input: {
    ...gscWindowInput,
    interval: z
      .enum(GSC_INTERVALS)
      .optional()
      .describe(
        "Series step: day, week (ISO, Monday) or month. Default: day up to 92 days, week up to 183, month beyond",
      ),
    page: gscPageFilter,
    query: gscQueryFilter,
    country: gscCountryFilter,
    device: gscDeviceFilter,
  },
  async handler({ interval, ...input }, ctx) {
    const report = await readable(
      ctx
        .get(SearchConsoleService)
        .series({ ...gscQuery(input), interval: interval ?? null }),
    );
    return {
      from: report.from,
      to: report.to,
      days: report.days,
      locale: report.locale,
      ...(report.partialBreakdown && { partialBreakdown: true }),
      interval: report.interval,
      series: report.series.map((point) => ({
        ...point,
        ctr: round3(point.ctr),
      })),
    };
  },
});
