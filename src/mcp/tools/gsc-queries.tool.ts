import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SearchConsoleService } from "../../search-console/search-console.service";
import { round3 } from "./site-analytics.inputs";
import {
  GSC_CAVEATS,
  GSC_DATA_DESCRIPTION,
  gscCountryFilter,
  gscDeviceFilter,
  gscPageFilter,
  gscQuery,
  gscWindowInput,
  withGscData,
} from "./gsc-data";

export const gscQueriesTool = defineMcpTool({
  name: "gsc_queries",
  title: "Search queries from Google",
  description:
    "The search queries park.fan was found with in Google, most clicks first: clicks, " +
    "impressions, ctr, average position and the change against the window before, plus the " +
    "totals over every listed query. Reach for it to see what people look for before they " +
    "arrive, which queries rank low with many impressions (room to improve a page) and which " +
    "moved. Pass page to see the queries one page is found with. Google leaves out queries it " +
    "anonymises (rare ones), so these totals are below gsc_pages' and gsc_series'. " +
    GSC_DATA_DESCRIPTION +
    " " +
    GSC_CAVEATS,
  input: {
    ...gscWindowInput,
    page: gscPageFilter,
    country: gscCountryFilter,
    device: gscDeviceFilter,
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(25)
      .describe("Queries to list"),
  },
  async handler(input, ctx) {
    const report = await ctx.get(SearchConsoleService).queries(gscQuery(input));
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
        totals: rate(report.totals),
        previous: report.previous ? rate(report.previous) : null,
      },
      { items: report.items.map(rate) },
    );
  },
});
