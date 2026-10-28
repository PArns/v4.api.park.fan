import { z } from "zod";
import {
  FILTER_DIMS,
  MAX_ADMIN_WINDOW_DAYS,
  SITE_LOCALES,
} from "../../site-analytics/site-analytics.constants";
import type { SiteAnalyticsQuery } from "../../site-analytics/site-analytics.query";

/**
 * What the site-statistic tools share: the window, the language and the
 * filters every report takes, and the caveats a model has to know before it
 * reads a number from them.
 */

/** Appended to the descriptions of the tools that report traffic. */
export const SITE_ANALYTICS_CAVEATS =
  "Numbers: park.fan's own cookieless statistic, UTC days, today included. A visit ends after " +
  "30 minutes without a hit. Visitors are unique per UTC day and summed over the window (a reader " +
  "back on another day counts again), and null under any filter but the language. Bounce rate = " +
  "1 − engaged / visits, engaged meaning a visit with a second page view. `previous` is the window " +
  "of the same length right before. Reports lag the site by up to about six minutes. Readers with " +
  "Do Not Track, Global Privacy Control or the privacy page's opt-out, and bots, are not counted.";

export const FILTER_DIM_HELP =
  "path (e.g. /de/parks/europe/germany/bruehl/phantasialand), page_type (home, park, attraction, " +
  "guide, news, search, not_found …), entry (the visit's first path), referrer (google, chatgpt, " +
  "a site name), channel (search, ai, social, referral, campaign, direct), campaign (utm_campaign), " +
  "country (ISO alpha-2, zz unknown), viewport (mobile, tablet, desktop), browser, os, locale";

export const siteWindowInput = {
  days: z
    .number()
    .int()
    .min(1)
    .max(MAX_ADMIN_WINDOW_DAYS)
    .default(30)
    .describe(
      "UTC days back from today, today included; 1 gives hourly points",
    ),
  locale: z
    .enum(SITE_LOCALES)
    .optional()
    .describe("Only this language of the site; all six when absent"),
  filters: z
    .array(
      z.object({
        dim: z.enum(FILTER_DIMS).describe(FILTER_DIM_HELP),
        value: z.string().min(1).max(820).describe("The exact value"),
      }),
    )
    .max(8)
    .optional()
    .describe(
      "Rows must match all of these, e.g. [{dim:'channel',value:'search'}]. Visit dimensions " +
        "(entry, referrer, channel, campaign) select by how the visit began",
    ),
};

export type SiteWindowInput = {
  days: number;
  locale?: (typeof SITE_LOCALES)[number];
  filters?: Array<{ dim: (typeof FILTER_DIMS)[number]; value: string }>;
};

/** The service's query: `filter` as the `<dim>:<value>` strings the HTTP route takes. */
export function siteQuery(input: SiteWindowInput): SiteAnalyticsQuery {
  return {
    days: input.days,
    locale: input.locale ?? null,
    filter: (input.filters ?? []).map(({ dim, value }) => `${dim}:${value}`),
  };
}

/** The window a report echoes, without repeating it per tool. */
export function windowOf(report: {
  from: string;
  to: string;
  days: number;
  locale: string | null;
  filters: string[];
}) {
  return {
    from: report.from,
    to: report.to,
    days: report.days,
    locale: report.locale,
    filters: report.filters,
  };
}

/** A rate to three places: enough to compare, short to read. */
export function round3(value: number | null | undefined): number | null {
  return typeof value === "number" ? Math.round(value * 1000) / 1000 : null;
}

/** `count` against `previous` as a ratio (0.25 = +25 %), null without a base. */
export function changeRatio(count: number, previous: number): number | null {
  return previous > 0 ? round3((count - previous) / previous) : null;
}
