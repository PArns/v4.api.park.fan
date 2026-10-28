import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsSeoService } from "../../site-analytics/site-analytics-seo.service";
import { siteQuery, siteWindowInput, windowOf } from "./site-analytics.inputs";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

export const seoNotFoundTool = defineMcpTool({
  name: "seo_not_found",
  title: "Not-found paths",
  description:
    "Paths that showed park.fan's not-found page, most viewed first, with the sites their visits " +
    "came from: a referrer such as google means a dead URL still indexed or linked from outside; " +
    "views without any referrer usually mean a broken link inside the site. Candidates for a " +
    "redirect or a fixed link. UTC days; up to six minutes behind the site; Do Not Track, GPC and " +
    "opted-out readers are missing. " +
    VISITOR_DATA_DESCRIPTION,
  input: {
    ...siteWindowInput,
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
      items: report.items.map((item) => ({
        path: item.path,
        views: item.views,
        referrers: item.referrers.slice(0, 5),
      })),
    });
  },
});
