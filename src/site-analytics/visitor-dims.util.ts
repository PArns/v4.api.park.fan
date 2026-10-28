/**
 * Visitors under a filter: which filter sets have a visitor count, and which
 * day HyperLogLogs a reader's page views are counted in. A HyperLogLog cannot
 * be intersected with another, so a count exists only for what was counted
 * into its own key: one value of one dimension, and the language. See
 * docs/frontend/site-analytics.md.
 */
import {
  ALL_LOCALES_SCOPE,
  OVERFLOW_VALUE,
  VISITOR_DIMS,
  type BreakdownDim,
  type VisitorDim,
} from "./site-analytics.constants";
import type { SiteFilter } from "./site-analytics.query";

/** Whether a dimension has visitors of its own ({@link VISITOR_DIMS}). */
export function isVisitorDim(dim: string): dim is VisitorDim {
  return (VISITOR_DIMS as readonly string[]).includes(dim);
}

/** One value of one dimension, which a day's visitors were counted under. */
export interface VisitorFilter {
  dim: VisitorDim;
  value: string;
}

/**
 * What a report's visitors are counted under: `filter` is the one value, or
 * null for every reader of the window's language; `by` is the breakdown's
 * dimension, when it was asked for.
 */
export interface VisitorBasis {
  filter: VisitorFilter | null;
  by: VisitorDim | null;
}

/**
 * Whether a filter set has a visitor count, and under what. The one place
 * that decides: no filter, or exactly one filter on a {@link VISITOR_DIMS}
 * dimension, with or without the language (`locale` is the report's own
 * field, not a filter). Null for two filters and for any filter on another
 * dimension (a page, the entry, the referrer, a `locale` filter). For a
 * breakdown, pass its dimension as `by`: the count per value exists for a
 * visitor dimension, under no filter or under a filter on that same
 * dimension, and is null otherwise.
 */
export function visitorBasisOf(
  filters: readonly SiteFilter[],
  by?: BreakdownDim,
): VisitorBasis | null {
  if (by !== undefined && !isVisitorDim(by)) return null;
  const dim = by ?? null;
  if (filters.length === 0) return { filter: null, by: dim };
  const [only] = filters;
  if (filters.length > 1 || !isVisitorDim(only.dim)) return null;
  if (dim !== null && only.dim !== dim) return null;
  return { filter: { dim: only.dim, value: only.value }, by: dim };
}

/** A day HyperLogLog beside the language ones: a scope, a dimension, a value. */
export interface VisitorDimKey {
  scope: string;
  dim: VisitorDim;
  value: string;
}

/** The index set's member for a key: `scope|dim|value`. */
export function visitorDimMember({ scope, dim, value }: VisitorDimKey): string {
  return `${scope}|${dim}|${value}`;
}

/** The key a member stands for; null for anything else. */
export function parseVisitorDimMember(member: string): VisitorDimKey | null {
  const first = member.indexOf("|");
  const second = member.indexOf("|", first + 1);
  if (first < 1 || second < 0) return null;
  const dim = member.slice(first + 1, second);
  const value = member.slice(second + 1);
  if (!isVisitorDim(dim) || !value) return null;
  return { scope: member.slice(0, first), dim, value };
}

/**
 * The HyperLogLogs one request's reader is counted in, besides the day's
 * all-languages and per-language ones. Their page views' languages each get
 * the reader's country, device, browser and OS, as the language keys count
 * them. A visit that opened in this request also counts the reader under its
 * channel, in its own language only: a visit is of the language of its first
 * page, so under `locale=de` and `channel:search` a reader is a visitor of
 * the search visits that began in German. Empty without a page view. A value
 * the guards stored as an overflow value, or that is empty, is not counted.
 */
export function visitorDimKeys(
  reader: Pick<
    Record<VisitorDim, string>,
    "country" | "device" | "browser" | "os"
  >,
  pageviewLocales: Iterable<string>,
  opened: { locale: string; channel: string } | null,
): VisitorDimKey[] {
  const locales = new Set(pageviewLocales);
  if (locales.size === 0) return [];
  const keys = new Map<string, VisitorDimKey>();
  const add = (scope: string, dim: VisitorDim, value: string) => {
    if (!value || value === OVERFLOW_VALUE) return;
    const key = { scope, dim, value };
    keys.set(visitorDimMember(key), key);
  };
  for (const scope of [ALL_LOCALES_SCOPE, ...locales]) {
    for (const dim of ["country", "device", "browser", "os"] as const) {
      add(scope, dim, reader[dim]);
    }
  }
  if (opened) {
    for (const scope of [ALL_LOCALES_SCOPE, opened.locale]) {
      add(scope, "channel", opened.channel);
    }
  }
  return [...keys.values()];
}
