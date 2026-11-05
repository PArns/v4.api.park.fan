import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsService } from "../../site-analytics/site-analytics.service";
import { EVENT_NAME_PATTERN } from "../../site-analytics/site-analytics.hits";
import {
  HOURLY_FROM_DESCRIPTION,
  siteQuery,
  eventWindowInput,
  windowOf,
} from "./site-analytics.inputs";
import {
  SMALL_CELLS_DESCRIPTION,
  SMALL_SERIES_DESCRIPTION,
  foldSmallRows,
  hideSmallInSeries,
  hideSmallTotal,
  isNarrowed,
} from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

export const analyticsEventTool = defineMcpTool({
  name: "analytics_event",
  title: "One site event",
  description:
    "One event in detail: its total, every property it carries with the most frequent values (the " +
    "rest summed in otherCount), the most frequent property combinations, the page types and " +
    "devices it came from, the parks, rides, shows or restaurants it was about (name and park.fan " +
    "path; kind null once the entity is gone) and optionally its count per period of the interval. Event names come " +
    "from analytics_events. Properties are choices the site defines (a surface, a tab, a position " +
    "bucket), never free text. Entities are resolved from park.fan's own tables, unresolvable ones " +
    "left out. UTC days; up to six minutes behind the site; Do Not Track, GPC and opted-out readers " +
    "are missing. " +
    VISITOR_DATA_DESCRIPTION +
    " " +
    SMALL_CELLS_DESCRIPTION +
    ' Under (other) an entity has no id, kind or path and a combination reads "(other)" for ' +
    "its properties. " +
    SMALL_SERIES_DESCRIPTION +
    " " +
    HOURLY_FROM_DESCRIPTION,
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
    const values = { dims: ["value"], count: "count", sum: ["count"] };
    const hide = hideSmallInSeries(report.series, ["count"]);
    const total = hideSmallTotal(report.total, input);
    return withVisitorData(
      {
        ...windowOf(report),
        name: report.name,
        total: total.total,
        ...(report.blocked && { blocked: true }),
        entities: foldSmallRows(resolved.slice(0, limit), {
          dims: ["name"],
          count: "count",
          sum: ["count"],
        }),
        unresolvedEntities: report.entities.length - resolved.length,
        ...((series || isNarrowed(input)) && {
          smallValuesHidden: (series && hide.hidden) || total.hidden,
        }),
      },
      {
        props: report.props.map((prop) => {
          const kept = prop.values.slice(0, limit);
          const cut = prop.values
            .slice(limit)
            .reduce((sum, entry) => sum + entry.count, 0);
          return {
            prop: prop.prop,
            values: foldSmallRows(kept, values),
            otherCount: prop.otherCount + cut,
          };
        }),
        combinations: foldSmallRows(report.combinations.slice(0, limit), {
          dims: ["props"],
          count: "count",
          sum: ["count"],
        }),
        byPageType: foldSmallRows(report.byPageType.slice(0, limit), values),
        byDevice: foldSmallRows(report.byDevice, values),
        ...(series && { series: hide.series }),
      },
    );
  },
});
