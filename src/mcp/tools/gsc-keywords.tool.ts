import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SearchConsoleService } from "../../search-console/search-console.service";
import { DEFAULT_KEYWORD_LIMIT } from "../../search-console/search-console.constants";
import {
  GSC_CAVEATS,
  GSC_DATA_DESCRIPTION,
  gscCountryFilter,
  gscDeviceFilter,
  readable,
  withGscData,
} from "./gsc-data";

export const gscKeywordsTool = defineMcpTool({
  name: "gsc_keywords",
  title: "Weekly keyword report",
  description:
    "The weekly keyword report, like a rank tracker's: the last complete week (Monday to " +
    "Sunday) against the week before, for the queries with the most impressions over both " +
    "weeks. The summary has the visibility (share of the clicks the keywords would get at " +
    "position 1, weighted by impressions, in percent), how many rank in the top 3 and top 10, " +
    "risers and fallers with the places moved, and new and lost keywords; each keyword has " +
    "both weeks' position, clicks and impressions, the places moved (positive is up), its " +
    "trend and the page that ranks; movers are the ten biggest moves each way. Reach for it " +
    "for 'how did we rank this week' or to find the keywords that dropped. Pass week (any day " +
    "in it) for an earlier week. Positions are Search Console's averages; competitors are not " +
    "in it. " +
    GSC_DATA_DESCRIPTION +
    " " +
    GSC_CAVEATS,
  input: {
    week: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/)
      .optional()
      .describe(
        "Any day of the week to report; default the last complete week",
      ),
    country: gscCountryFilter,
    device: gscDeviceFilter,
    limit: z
      .number()
      .int()
      .min(1)
      .max(200)
      .default(DEFAULT_KEYWORD_LIMIT)
      .describe("Keywords to monitor"),
  },
  async handler(input, ctx) {
    const report = await readable(
      ctx.get(SearchConsoleService).keywords(input),
    );
    return withGscData(
      {
        current: report.current,
        previous: report.previous,
        complete: report.complete,
        summary: report.summary,
      },
      { movers: report.movers, items: report.items },
    );
  },
});
