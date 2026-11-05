import { z } from "zod";
import { defineMcpTool, McpToolError } from "../mcp-tool";
import {
  InspectionInputError,
  SearchConsoleInspectionService,
} from "../../search-console/search-console-inspection.service";
import { MAX_INSPECTION_URLS } from "../../search-console/search-console.constants";
import { GSC_DATA_DESCRIPTION, withGscData } from "./gsc-data";

export const gscInspectUrlTool = defineMcpTool({
  name: "gsc_inspect_url",
  title: "Is this page indexed by Google?",
  description:
    "Google's own verdict on one or more park.fan pages (Search Console URL Inspection): whether " +
    "the page is indexed and why not, the coverage and indexing state, whether robots.txt and the " +
    "fetch allowed it, when Google last crawled it and as which crawler, the canonical Google " +
    "chose against the one the page declares (canonicalMatches), the sitemap and pages that link " +
    "to it, and the mobile-usability and rich-results verdicts when Google reports them. Reach for " +
    "it when gsc_pages shows a page with no impressions, or a new page should be in Google. " +
    "Pass url (a path like /de/parks/europe/germany/bruehl/phantasialand or a park.fan URL) or " +
    `urls (up to ${MAX_INSPECTION_URLS}). Each URL costs one of Google's 2,000 inspections a day, ` +
    "so answers are kept six hours (cached: true) and the tool stops at 1,800 a day; quota says " +
    "how many are used. A URL Google refuses comes back with error. " +
    GSC_DATA_DESCRIPTION,
  input: {
    url: z
      .string()
      .max(900)
      .optional()
      .describe("One page: its path (/de/parks/…) or park.fan URL"),
    urls: z
      .array(z.string().max(900))
      .max(MAX_INSPECTION_URLS)
      .optional()
      .describe(
        `Several pages, up to ${MAX_INSPECTION_URLS}; with or instead of url`,
      ),
  },
  async handler({ url, urls }, ctx) {
    const inputs = [...(url ? [url] : []), ...(urls ?? [])];
    try {
      const report = await ctx
        .get(SearchConsoleInspectionService)
        .inspect(inputs);
      return withGscData({ quota: report.quota }, { results: report.results });
    } catch (error) {
      if (error instanceof InspectionInputError) {
        throw new McpToolError(error.message);
      }
      throw error;
    }
  },
});
