import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsService } from "../../site-analytics/site-analytics.service";
import { EVENT_NAME_PATTERN } from "../../site-analytics/site-analytics.hits";
import { CROSSTAB_AXIS_PATTERN } from "../../site-analytics/site-analytics.query";
import { siteQuery, pageWindowInput, windowOf } from "./site-analytics.inputs";

const SIDE_HELP =
  "page_type, viewport, locale, day, path, country, browser, os, or " +
  '"prop:<key>" for one of the event\'s properties (e.g. prop:surface); events carry no visit ' +
  "dimension";

export const analyticsCrosstabTool = defineMcpTool({
  name: "analytics_crosstab",
  title: "One event by two sides",
  description:
    "One event counted by two sides at once, as a matrix: counts[i][j] is the count for rows[i] " +
    'and cols[j]. Answers questions like "which search, from which page" (search_opened, ' +
    'row=prop:surface, col=page_type) or "which tab on phones" (tab_changed, row=prop:tab, ' +
    'col=viewport). Each side keeps its 30 largest values and folds the rest into "__rest__" ' +
    '(a page type may itself be "other"); "(none)" is an empty value; day ' +
    "keeps every day. UTC days; up to six minutes behind the site; Do Not Track, GPC and opted-out " +
    "readers are missing.",
  input: {
    name: z
      .string()
      .regex(EVENT_NAME_PATTERN)
      .describe("Event name from analytics_events"),
    row: z
      .string()
      .regex(CROSSTAB_AXIS_PATTERN)
      .describe(`Row side: ${SIDE_HELP}`),
    col: z
      .string()
      .regex(CROSSTAB_AXIS_PATTERN)
      .describe(`Column side: ${SIDE_HELP}`),
    ...pageWindowInput,
  },
  async handler({ name, row, col, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsService)
      .crosstab(name, { ...siteQuery(input), row, col });
    const rowIndex = new Map(report.rows.map((value, i) => [value, i]));
    const colIndex = new Map(report.cols.map((value, i) => [value, i]));
    const counts = report.rows.map(() => report.cols.map(() => 0));
    for (const cell of report.cells) {
      const i = rowIndex.get(cell.row);
      const j = colIndex.get(cell.col);
      if (i !== undefined && j !== undefined) counts[i][j] += cell.count;
    }
    return {
      ...windowOf(report),
      name: report.name,
      row: report.row,
      col: report.col,
      total: report.total,
      rows: report.rows,
      cols: report.cols,
      counts,
    };
  },
});
