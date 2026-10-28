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

export const seoLandingPagesTool = defineMcpTool({
  name: "seo_landing_pages",
  title: "SEO landing pages",
  description:
    "The pages that visits from one channel (search engines by default; also ai, social, " +
    "referral, campaign, direct) entered park.fan on, most visits first: path, page type, visits, " +
    "the previous window's visits, the change in visits (absolute), the bounce rate and the top " +
    "sources (google, bing, chatgpt …). The closest thing to organic search performance this site " +
    "has; it knows nothing of rankings, impressions or queries. " +
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
    return {
      ...windowOf(report),
      channel: report.channel,
      items: report.items.map((page) => ({
        ...page,
        bounceRate: round3(page.bounceRate),
      })),
    };
  },
});
