import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SearchConsoleService } from "../../search-console/search-console.service";
import { round3 } from "./site-analytics.inputs";
import {
  GSC_CAVEATS,
  GSC_DATA_DESCRIPTION,
  gscCountryFilter,
  gscDeviceFilter,
  gscQuery,
  gscQueryFilter,
  gscWindowInput,
  withGscData,
} from "./gsc-data";

export const gscPagesTool = defineMcpTool({
  name: "gsc_pages",
  title: "Pages found in Google",
  description:
    "The pages of park.fan found in Google, most clicks first: path, clicks, impressions, ctr, " +
    "average position, the change against the window before, and `site`: park.fan's own " +
    "numbers for the visits from search that entered on that page in the same window (visits, " +
    "bounceRate, avgVisitSeconds; null without such a visit). Together they say which pages " +
    "Google shows often but rarely gets clicked (low ctr, decent position: a title or " +
    "description to rewrite) and which ones people leave at once. Pass query to see the pages " +
    "one search query brings. The totals include queries Google anonymises, unlike gsc_queries', and are the property's, so the items (by page) add up to a little more. With a country or device filter the totals are complete but the pages come from the visible queries only and partialBreakdown: true says so. " +
    GSC_DATA_DESCRIPTION +
    " " +
    GSC_CAVEATS,
  input: {
    ...gscWindowInput,
    query: gscQueryFilter,
    country: gscCountryFilter,
    device: gscDeviceFilter,
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(25)
      .describe("Pages to list"),
  },
  async handler(input, ctx) {
    const report = await ctx.get(SearchConsoleService).pages(gscQuery(input));
    const rate = <T extends { ctr: number }>(row: T) => ({
      ...row,
      ctr: round3(row.ctr),
    });
    return withGscData(
      {
        from: report.from,
        to: report.to,
        days: report.days,
        locale: report.locale,
        ...(report.partialBreakdown && { partialBreakdown: true }),
        totals: rate(report.totals),
        previous: report.previous ? rate(report.previous) : null,
      },
      {
        items: report.items.map((item) => ({
          ...rate(item),
          site: item.site
            ? { ...item.site, bounceRate: round3(item.site.bounceRate) }
            : null,
        })),
      },
    );
  },
});
