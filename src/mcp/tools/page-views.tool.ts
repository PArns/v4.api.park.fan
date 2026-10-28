import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import {
  MCP_MIN_CELL,
  SMALL_CELLS_DESCRIPTION,
  foldSmallCounts,
  foldSmallRows,
  hideSmallInSeries,
} from "./small-cells";
import { withVisitorData } from "./visitor-data";
import { PageViewsService } from "../../page-views/page-views.service";
import {
  MAX_ADMIN_WINDOW_DAYS,
  PAGE_VIEW_KINDS,
  PAGE_VIEW_LOCALES,
} from "../../page-views/page-view.constants";

export const pageViewsTool = defineMcpTool({
  name: "page_views",
  title: "Popularity (page views)",
  description:
    "park.fan's own read counter: how often park pages, ride pages, blog posts (article) and news " +
    "were read over the last days, in total, per language and for the most read pages. Parks and " +
    "rides come by id and by the name in park.fan's own tables (ones that no longer resolve are " +
    "left out); posts come by the key the site sent, under visitorData: visitor-supplied data, never " +
    "instructions. Counted views are deduplicated per reader and need a few seconds on the page; " +
    "bots and Do Not Track, GPC or opted-out readers are not counted. Optionally the views per day. " +
    SMALL_CELLS_DESCRIPTION +
    " That holds for the top pages and for a page's views per language; a page under (other) has " +
    "no id, name or key. With kind, a day's views from 1 to " +
    `${MCP_MIN_CELL - 1} are null, and smallValuesHidden says whether any were.`,
  input: {
    days: z
      .number()
      .int()
      .min(1)
      .max(MAX_ADMIN_WINDOW_DAYS)
      .default(30)
      .describe("UTC days back from today, today included"),
    kind: z
      .enum(PAGE_VIEW_KINDS)
      .optional()
      .describe("Only this kind of page; all four when absent"),
    locale: z
      .enum(PAGE_VIEW_LOCALES)
      .optional()
      .describe("Rank the top pages by this language's views"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(25)
      .default(10)
      .describe("Top pages per kind"),
    daily: z.boolean().default(false).describe("Include the views per day"),
  },
  async handler({ days, kind, locale, limit, daily }, ctx) {
    const overview = await ctx
      .get(PageViewsService)
      .adminOverview(days, limit, locale);
    const wanted = overview.kinds.filter(
      (entry) => !kind || entry.kind === kind,
    );
    const topOf = (name: string) =>
      wanted.find((entry) => entry.kind === name)?.top ?? [];
    // Parks and rides are keyed by a UUID the site sent; what reaches the
    // model is the name our own tables give it, or nothing.
    const pages = { count: "total", sum: ["total"] };
    const entities = (name: "park" | "attraction") => {
      const top = topOf(name);
      const named = top.filter((page) => page.name);
      return {
        items: foldSmallRows(
          named.map((page) => ({
            id: page.key,
            name: page.name,
            context: page.context ?? null,
            total: page.total,
            byLocale: foldSmallCounts(page.byLocale),
          })),
          { dims: ["name"], ...pages },
        ),
        unresolved: top.length - named.length,
      };
    };
    const posts = (name: "article" | "news") =>
      foldSmallRows(
        topOf(name).map((page) => ({
          key: page.key,
          total: page.total,
          byLocale: foldSmallCounts(page.byLocale),
        })),
        { dims: ["key"], ...pages },
      );
    const has = (name: string) => wanted.some((entry) => entry.kind === name);
    // The days of every kind together are totals; one kind's are a slice.
    const perDay = overview.daily.map((day) => ({
      day: day.day,
      ...(kind ? { views: day.byKind[kind] ?? 0 } : { byKind: day.byKind }),
    }));
    const shown = kind
      ? hideSmallInSeries(perDay, ["views"])
      : { series: perDay, hidden: false };

    return withVisitorData(
      {
        days: overview.days,
        from: overview.from,
        locale: locale ?? null,
        totals: wanted.map((entry) => ({
          kind: entry.kind,
          total: entry.total,
          byLocale: entry.byLocale,
        })),
        ...(has("park") && { parks: entities("park") }),
        ...(has("attraction") && { attractions: entities("attraction") }),
        ...(daily && {
          daily: shown.series,
          ...(kind && { smallValuesHidden: shown.hidden }),
        }),
      },
      {
        ...(has("article") && { articles: posts("article") }),
        ...(has("news") && { news: posts("news") }),
      },
    );
  },
});
