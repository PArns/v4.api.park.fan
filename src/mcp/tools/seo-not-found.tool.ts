import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsSeoService } from "../../site-analytics/site-analytics-seo.service";
import {
  HOURLY_FROM_DESCRIPTION,
  siteQuery,
  pageWindowInput,
  windowOf,
} from "./site-analytics.inputs";
import { SMALL_CELLS_DESCRIPTION, foldSmallRows } from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

export const seoNotFoundTool = defineMcpTool({
  name: "seo_not_found",
  title: "Not-found paths",
  description:
    "Paths that showed park.fan's not-found page, most viewed first, with the sites of the visits " +
    "that entered on them (visits per referrer): a referrer such as google means a dead URL still " +
    "indexed or linked from outside; views without any such visit usually mean a broken link " +
    "inside the site. Candidates for a redirect or a fixed link. Takes no visit-dimension filter. " +
    "UTC days; up to six minutes behind the site; Do Not Track, GPC and opted-out readers are " +
    "missing. " +
    VISITOR_DATA_DESCRIPTION +
    " " +
    SMALL_CELLS_DESCRIPTION +
    " Under (other) the referrers are null. " +
    HOURLY_FROM_DESCRIPTION,
  input: {
    ...pageWindowInput,
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(25)
      .describe("Paths to list"),
  },
  async handler({ limit, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsSeoService)
      .notFound({ ...siteQuery(input), limit });
    return withVisitorData(windowOf(report), {
      items: foldSmallRows(
        report.items.map((item) => ({
          path: item.path,
          views: item.views,
          referrers: foldSmallRows(item.referrers.slice(0, 5), {
            dims: ["value"],
            count: "visits",
            sum: ["visits"],
          }),
        })),
        { dims: ["path"], count: "views", sum: ["views"] },
      ),
    });
  },
});
