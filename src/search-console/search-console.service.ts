import { Injectable } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { addIsoDays } from "../common/utils/date.util";
import { withoutTrailingSlashes } from "../common/utils/url.util";
import {
  SqlParams,
  averageSeconds,
  bounceRate,
  daysBetween,
  defaultInterval,
  num,
  periodStartOf,
  rate,
  siteAnalyticsWindow,
  whereOf,
  type Count,
  type SiteAnalyticsWindow,
} from "../site-analytics/site-analytics.query";
import { getSearchConsoleSetup } from "../config/search-console.config";
import {
  BATCH_FORMAT,
  DEFAULT_REPORT_LIMIT,
  type GscInterval,
} from "./search-console.constants";
import { currentBatches } from "./search-console-batches";
import { pagePath } from "./search-console-url.util";
import type {
  SearchConsolePagesDto,
  SearchConsoleQueriesDto,
  SearchConsoleSeriesDto,
  SearchConsoleStatusDto,
  SearchConsoleTotalsDto,
} from "./dto/search-console.dto";

/** What every report takes: the window, a language and the filters. */
export interface SearchConsoleQuery {
  days?: number | null;
  from?: string | null;
  to?: string | null;
  locale?: string | null;
  interval?: GscInterval | null;
  /** A page's path or URL. */
  page?: string | null;
  query?: string | null;
  /** ISO alpha-3. */
  country?: string | null;
  device?: string | null;
  limit?: number | null;
}

/** Sums over rows; the figures of a report are derived from these. */
interface Sums {
  clicks: number;
  impressions: number;
  positionSum: number;
}

/**
 * Which stored rows answer. `property` (one row a day), `page`, `country` and
 * `device` are the complete rows without a query; `query` are the rows with
 * one (visible queries only).
 */
export type RowLevel = "property" | "page" | "country" | "device" | "query";

/** What answers a request: the list's rows, the totals' rows, and whether the list misses traffic. */
export interface LevelPlan {
  list: RowLevel;
  totals: RowLevel;
  /** The items sum to less than the totals, or the totals miss anonymised queries. */
  partialBreakdown: boolean;
}

/**
 * Google aggregates by property without a page dimension and by page with
 * one, and drops anonymised queries as soon as a page is combined with a
 * country or device. So the property row gives the totals the Search Console
 * chart shows, the page rows the per-page figures (their sum is a little
 * above), and only the single-dimension country and device rows are complete
 * under one of those filters. A page or language filter needs page rows. A
 * list under a country or device filter, both filters together, or a query
 * filter can only come from the query rows.
 */
export function levelPlan(
  query: Pick<
    SearchConsoleQuery,
    "query" | "country" | "device" | "page" | "locale"
  >,
  report: "pages" | "queries" | "series",
): LevelPlan {
  if (query.query) {
    return { list: "query", totals: "query", partialBreakdown: false };
  }
  const narrowed = Boolean(query.page || query.locale);
  const slice = query.country || query.device;
  if (!slice) {
    return {
      list: report === "queries" ? "query" : "page",
      totals: narrowed ? "page" : "property",
      partialBreakdown: false,
    };
  }
  const complete = !(query.country && query.device) && !narrowed;
  return {
    list: "query",
    totals: complete ? (query.country ? "country" : "device") : "query",
    partialBreakdown: report !== "queries",
  };
}

type Row = Record<string, Count | string>;

/** The window of a report once it knows which days have data. */
interface Resolved {
  window: SiteAnalyticsWindow;
  from: string;
  /** The last day of the window that has data; null when none does. */
  dataTo: string | null;
  /** The days to compare with: as many as `from`–`dataTo`, at the start of the window before. */
  previous: { from: string; to: string } | null;
  /** The batches to read, those of days with rows. */
  batchIds: number[];
  /** The first and last day the batches cover. */
  min: string;
}

const round1 = (value: number): number => Math.round(value * 10) / 10;

const positionOf = (sums: Sums): number | null =>
  sums.impressions > 0 ? round1(sums.positionSum / sums.impressions) : null;

function totalsOf(sums: Sums): SearchConsoleTotalsDto {
  return {
    clicks: sums.clicks,
    impressions: sums.impressions,
    ctr: rate(sums.clicks, sums.impressions),
    position: positionOf(sums),
  };
}

/** `clicks`, `impressions` and `position_sum` of rows matching `condition`, aliased with a prefix. */
function sumsSelect(prefix: string, condition: string): string {
  return (
    `COALESCE(SUM(r.clicks) FILTER (WHERE ${condition}), 0) AS ${prefix}_clicks, ` +
    `COALESCE(SUM(r.impressions) FILTER (WHERE ${condition}), 0) AS ${prefix}_impressions, ` +
    `COALESCE(SUM(r.position_sum) FILTER (WHERE ${condition}), 0) AS ${prefix}_position_sum`
  );
}

function sumsOf(row: Row | undefined, prefix: string): Sums {
  return {
    clicks: num(row?.[`${prefix}_clicks`] as Count),
    impressions: num(row?.[`${prefix}_impressions`] as Count),
    positionSum: Number(row?.[`${prefix}_position_sum`] ?? 0) || 0,
  };
}

/** The change of an item against the window before; null when there is no window before. */
function changeOf(
  current: Sums,
  previous: Sums,
  hasPrevious: boolean,
): { clicks: number; position: number | null } | null {
  if (!hasPrevious) return null;
  const was = positionOf(previous);
  const now = positionOf(current);
  return {
    clicks: current.clicks - previous.clicks,
    position:
      was === null || now === null
        ? null
        : round1(
            current.positionSum / current.impressions -
              previous.positionSum / previous.impressions,
          ),
  };
}

/** A page filter as stored: the path, whatever the caller wrote, a URL included. */
export function pageFilterOf(value: string): string {
  const trimmed = value.trim();
  if (/^https?:\/\//i.test(trimmed)) {
    try {
      const url = new URL(trimmed);
      return pagePath(trimmed, [url.host.toLowerCase()]) ?? "/";
    } catch {
      return trimmed.toLowerCase();
    }
  }
  const path = withoutTrailingSlashes(trimmed.split(/[?#]/)[0].toLowerCase());
  return path.startsWith("/") ? path || "/" : `/${path}`;
}

/**
 * The reports over Search Console's numbers: the queries people found the
 * site with, the pages they landed on (joined with the site statistic's own
 * numbers for visits from search), the series over time, and the status of
 * the import. Public so the admin's MCP tools offer the same answers.
 * docs/frontend/search-console.md.
 */
@Injectable()
export class SearchConsoleService {
  constructor(@InjectDataSource() private readonly db: DataSource) {}

  async status(): Promise<SearchConsoleStatusDto> {
    const setup = getSearchConsoleSetup();
    const [coverage]: Array<{
      first: string | null;
      last: string | null;
      rows: Count;
    }> = await this.db.query(
      `SELECT MIN(day)::text AS first, MAX(day)::text AS last, COALESCE(SUM(rows), 0) AS rows
         FROM (SELECT DISTINCT ON (day) day, rows
                 FROM search_console_batches ORDER BY day, id DESC) b
        WHERE rows > 0`,
    );
    const [success]: Array<{ at: Date | null }> = await this.db.query(
      `SELECT MAX(finished_at) AS at FROM search_console_runs WHERE ok`,
    );
    const [latest]: Array<{ ok: boolean; error: string | null }> =
      await this.db.query(
        `SELECT ok, error FROM search_console_runs
          WHERE finished_at IS NOT NULL ORDER BY id DESC LIMIT 1`,
      );
    return {
      configured: setup.configured,
      siteUrl: setup.siteUrl,
      lastImportAt: success?.at ? new Date(success.at).toISOString() : null,
      lastError: !setup.configured
        ? setup.reason
        : latest && !latest.ok
          ? latest.error
          : null,
      coverage:
        coverage?.first && coverage.last
          ? { from: coverage.first, to: coverage.last }
          : null,
      rows: num(coverage?.rows),
    };
  }

  /** The queries the site was found with, most clicks first. */
  async queries(
    query: SearchConsoleQuery,
    now = new Date(),
  ): Promise<SearchConsoleQueriesDto> {
    const resolved = await this.resolve(query, now);
    const plan = levelPlan(query, "queries");
    const echo = this.echo(resolved, plan);
    if (!resolved.dataTo) {
      return { ...echo, totals: totalsOf(ZERO), previous: null, items: [] };
    }
    const filters = this.filters(query);
    const { totals, previous, rows } = await this.grouped(
      resolved,
      "r.query",
      plan,
      filters,
      query.limit ?? DEFAULT_REPORT_LIMIT,
    );
    return {
      ...echo,
      totals,
      previous,
      items: rows.map((row) => {
        const current = sumsOf(row, "c");
        return {
          query: String(row.item_key),
          ...totalsOf(current),
          change: changeOf(
            current,
            sumsOf(row, "p"),
            resolved.previous !== null,
          ),
        };
      }),
    };
  }

  /**
   * The pages the site was found on, most clicks first, with the site
   * statistic's own numbers for the visits that came from search and entered
   * on them. Under a `query` filter, the pages that one query brought.
   */
  async pages(
    query: SearchConsoleQuery,
    now = new Date(),
  ): Promise<SearchConsolePagesDto> {
    const resolved = await this.resolve(query, now);
    const plan = levelPlan(query, "pages");
    const echo = this.echo(resolved, plan);
    if (!resolved.dataTo) {
      return { ...echo, totals: totalsOf(ZERO), previous: null, items: [] };
    }
    const filters = this.filters(query);
    const { totals, previous, rows } = await this.grouped(
      resolved,
      "r.page",
      plan,
      filters,
      query.limit ?? DEFAULT_REPORT_LIMIT,
    );
    const site = await this.siteNumbers(
      resolved.window,
      rows.map((row) => String(row.item_key)),
    );
    return {
      ...echo,
      totals,
      previous,
      items: rows.map((row) => {
        const current = sumsOf(row, "c");
        return {
          path: String(row.item_key),
          ...totalsOf(current),
          change: changeOf(
            current,
            sumsOf(row, "p"),
            resolved.previous !== null,
          ),
          site: site.get(String(row.item_key)) ?? null,
        };
      }),
    };
  }

  /**
   * Clicks, impressions, click-through rate and position per day, week or
   * month, from the first day to the last with data; a day inside that
   * without rows is a zero, and a period the window or the data cuts is
   * `partial`.
   */
  async series(
    query: SearchConsoleQuery,
    now = new Date(),
  ): Promise<SearchConsoleSeriesDto> {
    const resolved = await this.resolve(query, now);
    const { window } = resolved;
    const fallback = defaultInterval(window.days);
    const interval: GscInterval =
      query.interval ?? (fallback === "hour" ? "day" : fallback);
    const plan = levelPlan(query, "series");
    const echo = { ...this.echo(resolved, plan), interval };
    if (!resolved.dataTo) return { ...echo, series: [] };

    const params = new SqlParams();
    const where = this.rowsWhere(
      params,
      resolved,
      plan.totals,
      this.filters(query),
      false,
    );
    const rows: Array<Row> = await this.db.query(
      `SELECT r.day::text AS day, SUM(r.clicks) AS clicks, SUM(r.impressions) AS impressions,
              SUM(r.position_sum) AS position_sum
         FROM search_console_rows r
        WHERE ${where}
        GROUP BY r.day`,
      params.values,
    );
    const byDay = new Map(rows.map((row) => [String(row.day), row]));
    const periods = new Map<string, Sums>();
    for (const day of daysBetween(resolved.from, resolved.dataTo)) {
      const start = periodStartOf(day, interval);
      const sums = periods.get(start) ?? {
        clicks: 0,
        impressions: 0,
        positionSum: 0,
      };
      const row = byDay.get(day);
      sums.clicks += num(row?.clicks as Count);
      sums.impressions += num(row?.impressions as Count);
      sums.positionSum += Number(row?.position_sum ?? 0) || 0;
      periods.set(start, sums);
    }
    const dataTo = resolved.dataTo;
    return {
      ...echo,
      series: [...periods.entries()].map(([start, sums]) => {
        const next = nextPeriod(start, interval);
        const partial = start < resolved.from || addIsoDays(next, -1) > dataTo;
        return {
          day: start,
          ...totalsOf(sums),
          ...(partial && { partial: true as const }),
        };
      }),
    };
  }

  private echo(resolved: Resolved, plan: LevelPlan) {
    const { window } = resolved;
    return {
      ...(plan.partialBreakdown && { partialBreakdown: true as const }),
      from: window.from,
      to: window.to,
      days: window.days,
      locale: window.locale,
    };
  }

  /**
   * The window, and which days of it Search Console has: its newest rows lag
   * by days, so the window before is cut to the same number of days from its
   * start, and a change compares like with like.
   */
  private async resolve(
    query: SearchConsoleQuery,
    now: Date,
  ): Promise<Resolved> {
    const window = siteAnalyticsWindow(
      {
        days: query.days ?? null,
        from: query.from ?? null,
        to: query.to ?? null,
        locale: (query.locale ?? null) as SiteAnalyticsWindow["locale"],
      },
      now,
    );
    const lookFrom = window.previous?.from ?? window.from;
    const batches = await currentBatches(this.db, lookFrom, window.to);
    // A batch of an older layout has no complete rows; its day waits for the re-import.
    const withRows = [...batches.entries()].filter(
      ([, batch]) => batch.rows > 0 && batch.format >= BATCH_FORMAT,
    );
    const inWindow = withRows
      .map(([day]) => day)
      .filter((day) => day >= window.from && day <= window.to)
      .sort();
    const dataTo = inWindow.length > 0 ? inWindow[inWindow.length - 1] : null;

    let previous: Resolved["previous"] = null;
    if (dataTo && window.previous) {
      const to = addIsoDays(
        window.previous.from,
        daysBetween(window.from, dataTo).length - 1,
      );
      const hasData = withRows.some(
        ([day]) => day >= window.previous!.from && day <= to,
      );
      if (hasData) previous = { from: window.previous.from, to };
    }
    return {
      window,
      from: window.from,
      dataTo,
      previous,
      batchIds: withRows.map(([, batch]) => batch.id),
      min: previous?.from ?? window.from,
    };
  }

  /** The filters a query carries, as SQL conditions on `r`. */
  private filters(query: SearchConsoleQuery): Array<[string, string]> {
    const filters: Array<[string, string]> = [];
    if (query.page) filters.push(["r.page", pageFilterOf(query.page)]);
    if (query.query)
      filters.push(["r.query", query.query.trim().toLowerCase()]);
    if (query.country) filters.push(["r.country", query.country.toLowerCase()]);
    if (query.device) filters.push(["r.device", query.device.toLowerCase()]);
    return filters;
  }

  /**
   * The WHERE over `search_console_rows`: the current batches of the days of
   * the window (and the window before), the kind of row, the language and the
   * filters. `level` is the kind of row, see `RowLevel`.
   */
  private rowsWhere(
    params: SqlParams,
    resolved: Resolved,
    level: RowLevel,
    filters: Array<[string, string]>,
    withPrevious = true,
  ): string {
    const conditions = [
      `r.batch_id = ANY(${params.add(resolved.batchIds)}::int[])`,
      `r.day >= ${params.add(withPrevious ? resolved.min : resolved.from)}::date`,
      `r.day <= ${params.add(resolved.dataTo)}::date`,
      LEVEL_CONDITION[level],
    ];
    if (resolved.window.locale) {
      conditions.push(`r.locale = ${params.add(resolved.window.locale)}`);
    }
    for (const [column, value] of filters) {
      conditions.push(`${column} = ${params.add(value)}`);
    }
    return conditions.join(" AND ");
  }

  /**
   * The rows of a report grouped by a column, most clicks first, and the
   * sums over everything the filters select (not only the listed rows) for
   * the window and the one before.
   */
  private async grouped(
    resolved: Resolved,
    column: "r.query" | "r.page",
    plan: LevelPlan,
    filters: Array<[string, string]>,
    limit: number,
  ): Promise<{
    totals: SearchConsoleTotalsDto;
    previous: SearchConsoleTotalsDto | null;
    rows: Row[];
  }> {
    const params = new SqlParams();
    const where = this.rowsWhere(params, resolved, plan.list, filters);
    const current = `r.day >= ${params.add(resolved.from)}::date`;
    const currentTo = `r.day <= ${params.add(resolved.dataTo)}::date`;
    const cur = `${current} AND ${currentTo}`;
    const prev = resolved.previous
      ? `r.day >= ${params.add(resolved.previous.from)}::date AND r.day <= ${params.add(resolved.previous.to)}::date`
      : "false";
    const rows: Row[] = await this.db.query(
      `SELECT ${column} AS item_key, ${sumsSelect("c", cur)}, ${sumsSelect("p", prev)}
         FROM search_console_rows r
        WHERE ${where}
        GROUP BY ${column}
       HAVING COALESCE(SUM(r.impressions) FILTER (WHERE ${cur}), 0) > 0
        ORDER BY c_clicks DESC, c_impressions DESC, item_key ASC
        LIMIT ${params.add(limit)}`,
      params.values,
    );
    const sums = new SqlParams();
    const totalsWhere = this.rowsWhere(sums, resolved, plan.totals, filters);
    const totalsCur = `r.day >= ${sums.add(resolved.from)}::date AND r.day <= ${sums.add(resolved.dataTo)}::date`;
    const totalsPrev = resolved.previous
      ? `r.day >= ${sums.add(resolved.previous.from)}::date AND r.day <= ${sums.add(resolved.previous.to)}::date`
      : "false";
    const [total]: Row[] = await this.db.query(
      `SELECT ${sumsSelect("c", totalsCur)}, ${sumsSelect("p", totalsPrev)}
         FROM search_console_rows r
        WHERE ${totalsWhere}`,
      sums.values,
    );
    return {
      totals: totalsOf(sumsOf(total, "c")),
      previous: resolved.previous ? totalsOf(sumsOf(total, "p")) : null,
      rows,
    };
  }

  /**
   * The site statistic's numbers for visits from the search channel that
   * entered on each path in the window: visits, bounce rate and average visit.
   * Only a path with at least one such visit has an entry.
   */
  private async siteNumbers(
    window: SiteAnalyticsWindow,
    paths: string[],
  ): Promise<
    Map<
      string,
      { visits: number; bounceRate: number; avgVisitSeconds: number | null }
    >
  > {
    const found = new Map<
      string,
      { visits: number; bounceRate: number; avgVisitSeconds: number | null }
    >();
    if (paths.length === 0) return found;
    const params = new SqlParams();
    const where = whereOf(
      params,
      { ...window, filters: [], previous: null },
      "current",
    );
    const rows: Array<{
      path: string;
      visits: Count;
      engaged: Count;
      visit_seconds: Count;
    }> = await this.db.query(
      `SELECT entry AS path, SUM(visits) AS visits, SUM(engaged) AS engaged,
              SUM(visit_seconds) AS visit_seconds
         FROM site_pageviews
        WHERE ${where} AND channel = 'search' AND visits > 0
          AND entry = ANY(${params.add(paths)}::text[])
        GROUP BY entry`,
      params.values,
    );
    for (const row of rows) {
      const visits = num(row.visits);
      if (visits === 0) continue;
      found.set(row.path, {
        visits,
        bounceRate: bounceRate(visits, num(row.engaged)),
        avgVisitSeconds: averageSeconds(num(row.visit_seconds), visits),
      });
    }
    return found;
  }
}

const LEVEL_CONDITION: Record<RowLevel, string> = {
  property:
    "r.query IS NULL AND r.page = '' AND r.country = '' AND r.device = ''",
  page: "r.query IS NULL AND r.page <> '' AND r.country = '' AND r.device = ''",
  country: "r.query IS NULL AND r.page = '' AND r.country <> ''",
  device: "r.query IS NULL AND r.page = '' AND r.device <> ''",
  query: "r.query IS NOT NULL",
};

const ZERO: Sums = { clicks: 0, impressions: 0, positionSum: 0 };

function nextPeriod(start: string, interval: GscInterval): string {
  if (interval === "day") return addIsoDays(start, 1);
  if (interval === "week") return addIsoDays(start, 7);
  const [year, month] = start.split("-").map(Number);
  return month === 12
    ? `${year + 1}-01-01`
    : `${year}-${String(month + 1).padStart(2, "0")}-01`;
}
