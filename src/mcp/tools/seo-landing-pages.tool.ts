import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsSeoService } from "../../site-analytics/site-analytics-seo.service";
import { CHANNELS } from "../../site-analytics/site-analytics.constants";
import {
  SITE_ANALYTICS_CAVEATS,
  round3,
  siteQuery,
  siteWindowInput,
  windowOf,
} from "./site-analytics.inputs";
import { SMALL_CELLS_DESCRIPTION, foldSmallRows } from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

export const seoLandingPagesTool = defineMcpTool({
  name: "seo_landing_pages",
  title: "SEO landing pages",
  description:
    "The pages that visits from one channel (search engines by default; also ai, social, " +
    "referral, direct) entered park.fan on, most visits first: path, page type, visits, " +
    "the previous window's visits and the change in visits (absolute; both null beyond 180 days), " +
    "the bounce rate, the average visit in seconds (avgVisitSeconds) and the top " +
    "sources (google, bing, chatgpt …). The closest thing to organic search performance this site " +
    "has; it knows nothing of rankings, impressions or queries. " +
    VISITOR_DATA_DESCRIPTION +
    " " +
    SMALL_CELLS_DESCRIPTION +
    " Under (other) the page type, bounce rate, average visit and sources are null. " +
    SITE_ANALYTICS_CAVEATS,
  input: {
    channel: z
      .enum(CHANNELS)
      .default("search")
      .describe("Where the visits came from"),
    ...siteWindowInput,
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(25)
      .describe("Pages to list"),
  },
  async handler({ channel, limit, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsSeoService)
      .landingPages({ ...siteQuery(input), channel, limit });
    return withVisitorData(
      { ...windowOf(report), channel: report.channel },
      {
        items: foldSmallRows(
          report.items.map((page) => ({
            ...page,
            sources: foldSmallRows(page.sources, {
              dims: ["value"],
              count: "visits",
              sum: ["visits"],
            }),
          })),
          {
            dims: ["path"],
            count: "visits",
            sum: ["visits", "previous", "change"],
          },
        ).map((page) => ({
          ...page,
          bounceRate: round3(page.bounceRate),
        })),
      },
    );
  },
});
