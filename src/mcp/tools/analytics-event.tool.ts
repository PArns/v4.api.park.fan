import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsService } from "../../site-analytics/site-analytics.service";
import { EVENT_NAME_PATTERN } from "../../site-analytics/site-analytics.hits";
import { siteQuery, eventWindowInput, windowOf } from "./site-analytics.inputs";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

export const analyticsEventTool = defineMcpTool({
  name: "analytics_event",
  title: "One site event",
  description:
    "One event in detail: its total, every property it carries with the most frequent values (the " +
    "rest summed in otherCount), the most frequent property combinations, the page types and " +
    "viewports it came from, the parks, rides, shows or restaurants it was about (name and park.fan " +
    "path; kind null once the entity is gone) and optionally its count per period of the interval. Event names come " +
    "from analytics_events. Properties are choices the site defines (a surface, a tab, a position " +
    "bucket), never free text. Entities are resolved from park.fan's own tables, unresolvable ones " +
    "left out. UTC days; up to six minutes behind the site; Do Not Track, GPC and opted-out readers " +
    "are missing. " +
    VISITOR_DATA_DESCRIPTION,
  input: {
    name: z
      .string()
      .regex(EVENT_NAME_PATTERN)
      .describe('Event name, e.g. "search_result_clicked"'),
    ...eventWindowInput,
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .default(10)
      .describe(
        "Entries per list (values per property, combinations, entities)",
      ),
    series: z
      .boolean()
      .default(false)
      .describe("Include the count per period of the interval"),
  },
  async handler({ name, limit, series, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsService)
      .event(name, siteQuery(input));
    // Names and paths of entities come from park.fan's own tables; one that
    // no longer resolves is left out rather than shown by its raw id.
    const resolved = report.entities.filter((entity) => entity.kind !== null);
    return withVisitorData(
      {
        ...windowOf(report),
        name: report.name,
        total: report.total,
        ...(report.blocked && { blocked: true }),
        entities: resolved.slice(0, limit),
        unresolvedEntities: report.entities.length - resolved.length,
      },
      {
        props: report.props.map((prop) => {
          const kept = prop.values.slice(0, limit);
          const cut = prop.values
            .slice(limit)
            .reduce((sum, entry) => sum + entry.count, 0);
          return {
            prop: prop.prop,
            values: kept,
            otherCount: prop.otherCount + cut,
          };
        }),
        combinations: report.combinations.slice(0, limit),
        byPageType: report.byPageType.slice(0, limit),
        byViewport: report.byViewport,
        ...(series && { series: report.series }),
      },
    );
  },
});
