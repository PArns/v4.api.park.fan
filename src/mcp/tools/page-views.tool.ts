import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
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
    "were read over the last days, in total, per language and for the most read pages (parks and " +
    "rides with name and link, posts by their key). Counted views are deduplicated per reader and " +
    "need a few seconds on the page; bots are not counted. Optionally the views per day.",
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
    return {
      days: overview.days,
      from: overview.from,
      locale: locale ?? null,
      kinds: overview.kinds
        .filter((entry) => !kind || entry.kind === kind)
        .map((entry) => ({
          kind: entry.kind,
          total: entry.total,
          byLocale: entry.byLocale,
          top: entry.top.map((page) => ({
            key: page.key,
            name: page.name ?? null,
            context: page.context ?? null,
            total: page.total,
            byLocale: page.byLocale,
          })),
        })),
      ...(daily && {
        daily: overview.daily.map((day) => ({
          day: day.day,
          ...(kind ? { views: day.byKind[kind] ?? 0 } : { byKind: day.byKind }),
        })),
      }),
    };
  },
});
