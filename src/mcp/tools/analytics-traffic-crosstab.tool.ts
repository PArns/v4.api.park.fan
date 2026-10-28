import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsService } from "../../site-analytics/site-analytics.service";
import { TRAFFIC_CROSSTAB_SIDES } from "../../site-analytics/dto/site-analytics.dto";
import {
  FILTER_DIM_HELP,
  SITE_ANALYTICS_CAVEATS,
  siteQuery,
  siteWindowInput,
  windowOf,
} from "./site-analytics.inputs";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

const SIDE_HELP =
  "metric=pageviews: path, page_type, country, device, browser, os, locale, " +
  "browser_language or day. metric=visits also entry, referrer, channel and exit " +
  "(the page the visit ended on); there the page dimensions are the visit's first page, or with " +
  "exit on a side its last page. day steps by the interval";

export const analyticsTrafficCrosstabTool = defineMcpTool({
  name: "analytics_traffic_crosstab",
  title: "Traffic by two dimensions",
  description:
    "Page views or visits counted by two dimensions at once, as a matrix: counts[i][j] is the " +
    'count for rows[i] and cols[j]. Answers "which OS on which device" (row=os, col=device), ' +
    '"which channel from which country" (metric=visits, row=channel, col=country) or "where do ' +
    'search visits end" (metric=visits, row=channel, col=exit). Each side keeps its 30 largest ' +
    'values and folds the rest into "__rest__"; "(none)" is an empty value; day keeps every ' +
    "period. A filter on a visit dimension needs metric=visits. " +
    VISITOR_DATA_DESCRIPTION +
    " " +
    SITE_ANALYTICS_CAVEATS,
  input: {
    metric: z
      .enum(["pageviews", "visits"])
      .default("pageviews")
      .describe("Count page views, or visits started (ended, with exit)"),
    row: z.enum(TRAFFIC_CROSSTAB_SIDES).describe(`Row side: ${SIDE_HELP}`),
    col: z
      .enum(TRAFFIC_CROSSTAB_SIDES)
      .describe(`Column side: ${SIDE_HELP}. Filters: ${FILTER_DIM_HELP}`),
    ...siteWindowInput,
  },
  async handler({ metric, row, col, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsService)
      .trafficCrosstab({ ...siteQuery(input), metric, row, col });
    const rowIndex = new Map(report.rows.map((value, i) => [value, i]));
    const colIndex = new Map(report.cols.map((value, i) => [value, i]));
    const counts = report.rows.map(() => report.cols.map(() => 0));
    for (const cell of report.cells) {
      const i = rowIndex.get(cell.row);
      const j = colIndex.get(cell.col);
      if (i !== undefined && j !== undefined) counts[i][j] += cell.count;
    }
    return withVisitorData(
      {
        ...windowOf(report),
        metric: report.metric,
        row: report.row,
        col: report.col,
        total: report.total,
      },
      { rows: report.rows, cols: report.cols, counts },
    );
  },
});
