import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsSeoService } from "../../site-analytics/site-analytics-seo.service";
import { siteQuery, siteWindowInput, windowOf } from "./site-analytics.inputs";

export const seoUncoveredTool = defineMcpTool({
  name: "seo_uncovered",
  title: "Pages search never brings",
  description:
    "Parks or rides whose page no search-engine visit entered on in the window, in any language, " +
    "with name, park and park.fan path: the pages search does not send anyone to yet, and the " +
    "count of all such pages against all pages of that kind. A ride renamed since its old path was " +
    "found is listed too. UTC days; up to six minutes behind the site; Do Not Track, GPC and " +
    "opted-out readers are missing, so a page with very little traffic can be listed although it " +
    "had a visit.",
  input: {
    kind: z
      .enum(["park", "attraction"])
      .default("park")
      .describe("Parks or rides"),
    ...siteWindowInput,
    limit: z
      .number()
      .int()
      .min(1)
      .max(200)
      .default(50)
      .describe("Pages to list"),
  },
  async handler({ kind, limit, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsSeoService)
      .uncovered({ ...siteQuery(input), kind, limit });
    return {
      ...windowOf(report),
      kind: report.kind,
      total: report.total,
      uncovered: report.uncovered,
      items: report.items,
    };
  },
});
