import { Injectable, Logger } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { addIsoDays } from "../common/utils/date.util";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { SearchConsoleReindexPage } from "./entities/search-console-reindex-page.entity";
import { SearchConsoleReindexRequest } from "./entities/search-console-reindex-request.entity";
import {
  INSPECTION_ORIGIN,
  MAX_INSPECTION_URLS,
} from "./search-console.constants";
import { SearchConsoleClient } from "./search-console.client";
import { SearchConsoleInspectionService } from "./search-console-inspection.service";
import {
  parseSitemap,
  reindexState,
  type ReindexState,
  type SitemapEntry,
} from "./search-console-reindex";
import { SearchConsoleService } from "./search-console.service";
import { pagePath } from "./search-console-url.util";

/** The sitemaps whose `<lastmod>` the scan reads; an index is followed one level. */
const SITEMAPS = ["/sitemap.xml", "/sitemap-attractions.xml"];
const SITE_HOSTS = ["park.fan", "www.park.fan"];
const SITEMAP_TIMEOUT_MS = 60_000;

/** A change older than this is out of the queue: Google has recrawled or will not soon. */
export const REINDEX_RECENT_DAYS = 30;
/** Inspections the daily scan spends at most, of the 1,800 shared with the MCP tool. */
export const REINDEX_SCAN_BUDGET = 300;
/** A page inspected more recently than this is not inspected again by the scan. */
const REINSPECT_AFTER_MS = 20 * 60 * 60 * 1000;
/** Google takes about ten "request indexing" per property and day; the admin warns past it. */
export const REINDEX_DAILY_LIMIT = 10;
const FIGURE_DAYS = 28;
const LIST_LIMIT = 100;
const HISTORY_LIMIT = 50;

export interface ReindexItem {
  path: string;
  url: string;
  /** The sitemap `<lastmod>`: the day park.fan saw the content change. */
  changedOn: string;
  /** Google's figures over the last 28 days with data. */
  clicks: number;
  impressions: number;
  ctr: number | null;
  position: number | null;
  /** Why it is listed: `unknown` (no crawl), `stale` (crawled before the change), `uncertain`. */
  state: Exclude<ReindexState, "fresh">;
  lastCrawlAt: string | null;
  coverageState: string | null;
  inspectedAt: string;
  /** The latest request since the change, which takes the page off the list to click; else null. */
  requestedAt: string | null;
  requestedBy: string | null;
}

export interface ReindexQueue {
  /** Not requested since the change: certain cases first, then most impressions. */
  open: ReindexItem[];
  /** Requested since the change and not yet crawled after it: waiting for Google. */
  requested: ReindexItem[];
  /** Requests in the last 24 hours, against {@link REINDEX_DAILY_LIMIT}. */
  requestsLast24h: number;
  dailyLimit: number;
  history: Array<{ path: string; requestedAt: string; actor: string }>;
  /** Candidates in the queue and how many of them were ever inspected. */
  candidates: number;
  inspected: number;
  lastInspectedAt: string | null;
}

export interface ReindexRecordResult {
  /** False when the page was already requested since its change; nothing was written. */
  recorded: boolean;
  requestedAt: string;
  requestsLast24h: number;
  dailyLimit: number;
}

export interface ReindexScanSummary {
  skipped?: "not-configured";
  candidates: number;
  inspected: number;
  errors: number;
}

const startOf = (day: string): number => Date.parse(`${day}T00:00:00Z`);

/**
 * The reindex queue: which recently changed pages Google has not crawled
 * since, and the record of the "request indexing" clicks the admin sends for
 * them, so a page is requested once per change even while Search Console
 * still reports the old crawl. docs/frontend/search-console.md.
 */
@Injectable()
export class SearchConsoleReindexService {
  private readonly logger = new Logger(SearchConsoleReindexService.name);

  constructor(
    @InjectDataSource() private readonly db: DataSource,
    private readonly client: SearchConsoleClient,
    private readonly inspection: SearchConsoleInspectionService,
    private readonly reports: SearchConsoleService,
  ) {}

  /**
   * Rewrites the candidates from the sitemaps and inspects, most impressions
   * first, the ones Google may not have seen since their change, up to
   * {@link REINDEX_SCAN_BUDGET}.
   */
  async scan(now = new Date()): Promise<ReindexScanSummary> {
    if (!this.client.setup().configured) {
      return {
        skipped: "not-configured",
        candidates: 0,
        inspected: 0,
        errors: 0,
      };
    }
    const since = addIsoDays(
      now.toISOString().slice(0, 10),
      -REINDEX_RECENT_DAYS,
    );
    const changed = new Map<string, string>();
    for (const entry of await this.sitemapEntries()) {
      const path = pagePath(entry.loc, SITE_HOSTS);
      if (!path || !entry.lastmod || entry.lastmod < since) continue;
      changed.set(path, entry.lastmod);
    }
    // A sitemap that answered empty is an outage, not a site without changes.
    if (changed.size === 0) {
      throw new Error("No sitemap page changed within the window");
    }
    await this.store(changed, now);

    const pages = await this.db
      .getRepository(SearchConsoleReindexPage)
      .find({ order: { impressions: "DESC", path: "ASC" } });
    const due = pages
      .filter(
        (page) =>
          !page.inspectedAt ||
          (reindexState(page.changedOn, page.lastCrawlAt) !== "fresh" &&
            now.getTime() - page.inspectedAt.getTime() > REINSPECT_AFTER_MS),
      )
      // Among pages Google shows nowhere, a hub or a post before a ride.
      .sort(
        (a, b) =>
          b.impressions - a.impressions ||
          a.path.split("/").length - b.path.split("/").length,
      )
      .slice(0, REINDEX_SCAN_BUDGET);

    let inspected = 0;
    let errors = 0;
    for (let i = 0; i < due.length; i += MAX_INSPECTION_URLS) {
      const chunk = due.slice(i, i + MAX_INSPECTION_URLS);
      const report = await this.inspection.inspect(
        chunk.map((page) => page.path),
      );
      for (const outcome of report.results) {
        if ("error" in outcome) {
          errors++;
          continue;
        }
        const path = pagePath(outcome.url, SITE_HOSTS);
        if (!path) continue;
        await this.db.getRepository(SearchConsoleReindexPage).update(
          { path },
          {
            inspectedAt: now,
            lastCrawlAt: outcome.lastCrawlTime
              ? new Date(outcome.lastCrawlTime)
              : null,
            coverageState: outcome.coverageState,
          },
        );
        inspected++;
      }
      if (
        report.quota.usedToday !== null &&
        report.quota.usedToday >= report.quota.dailyLimit
      ) {
        this.logger.warn("Reindex scan stopped at the daily inspection limit");
        break;
      }
    }
    return { candidates: changed.size, inspected, errors };
  }

  /** The pages Google has not crawled since their change, split by whether they were requested. */
  async queue(now = new Date()): Promise<ReindexQueue> {
    const pages = await this.db
      .getRepository(SearchConsoleReindexPage)
      .find({ order: { impressions: "DESC", path: "ASC" } });
    const latest = await this.latestRequests();

    const open: ReindexItem[] = [];
    const requested: ReindexItem[] = [];
    for (const page of pages) {
      if (!page.inspectedAt) continue;
      const state = reindexState(page.changedOn, page.lastCrawlAt);
      if (state === "fresh") continue;
      const request = latest.get(page.path);
      const sinceChange =
        request && request.at.getTime() >= startOf(page.changedOn)
          ? request
          : null;
      const item: ReindexItem = {
        path: page.path,
        url: INSPECTION_ORIGIN + page.path,
        changedOn: page.changedOn,
        clicks: page.clicks,
        impressions: page.impressions,
        ctr: page.impressions > 0 ? page.clicks / page.impressions : null,
        position: page.position,
        state,
        lastCrawlAt: page.lastCrawlAt?.toISOString() ?? null,
        coverageState: page.coverageState,
        inspectedAt: page.inspectedAt.toISOString(),
        requestedAt: sinceChange?.at.toISOString() ?? null,
        requestedBy: sinceChange?.actor ?? null,
      };
      (sinceChange ? requested : open).push(item);
    }
    const certainFirst = (a: ReindexItem, b: ReindexItem) =>
      Number(a.state === "uncertain") - Number(b.state === "uncertain") ||
      b.impressions - a.impressions;
    open.sort(certainFirst);
    requested.sort((a, b) => b.requestedAt!.localeCompare(a.requestedAt!));

    const history = await this.db
      .getRepository(SearchConsoleReindexRequest)
      .find({ order: { requestedAt: "DESC" }, take: HISTORY_LIMIT });
    const inspectedPages = pages.filter((page) => page.inspectedAt);
    return {
      open: open.slice(0, LIST_LIMIT),
      requested: requested.slice(0, LIST_LIMIT),
      requestsLast24h: await this.requestsLast24h(now),
      dailyLimit: REINDEX_DAILY_LIMIT,
      history: history.map((row) => ({
        path: row.path,
        requestedAt: row.requestedAt.toISOString(),
        actor: row.actor,
      })),
      candidates: pages.length,
      inspected: inspectedPages.length,
      lastInspectedAt:
        inspectedPages
          .map((page) => page.inspectedAt!.toISOString())
          .sort()
          .pop() ?? null,
    };
  }

  /**
   * Records a request for `path`, once per change: a page already requested
   * since its change (or within the last 24 hours, for a page outside the
   * queue) is not recorded again, and the answer says when it was.
   */
  async record(
    path: string,
    admin: AdminPrincipal,
    now = new Date(),
  ): Promise<ReindexRecordResult> {
    const page = await this.db
      .getRepository(SearchConsoleReindexPage)
      .findOneBy({ path });
    const previous = (await this.latestRequests(path)).get(path);
    const since = page
      ? startOf(page.changedOn)
      : now.getTime() - 24 * 60 * 60 * 1000;
    if (previous && previous.at.getTime() >= since) {
      return {
        recorded: false,
        requestedAt: previous.at.toISOString(),
        requestsLast24h: await this.requestsLast24h(now),
        dailyLimit: REINDEX_DAILY_LIMIT,
      };
    }
    const row = await this.db.getRepository(SearchConsoleReindexRequest).save({
      path,
      actor: admin.apiToken?.clientName ?? admin.displayName ?? admin.email,
    });
    return {
      recorded: true,
      requestedAt: row.requestedAt.toISOString(),
      requestsLast24h: await this.requestsLast24h(now),
      dailyLimit: REINDEX_DAILY_LIMIT,
    };
  }

  /** The newest request per path within the queue's window, one path or all. */
  private async latestRequests(
    path?: string,
  ): Promise<Map<string, { at: Date; actor: string }>> {
    const rows: Array<{ path: string; at: Date; actor: string }> =
      await this.db.query(
        `SELECT DISTINCT ON (path) path, requested_at AS at, actor
           FROM search_console_reindex_requests
          WHERE requested_at > now() - ($1::int * interval '1 day')
            AND ($2::varchar IS NULL OR path = $2::varchar)
          ORDER BY path, requested_at DESC`,
        [REINDEX_RECENT_DAYS + 1, path ?? null],
      );
    return new Map(
      rows.map((row) => [row.path, { at: new Date(row.at), actor: row.actor }]),
    );
  }

  private async requestsLast24h(now: Date): Promise<number> {
    const [row]: Array<{ count: string }> = await this.db.query(
      `SELECT COUNT(*) AS count FROM search_console_reindex_requests
        WHERE requested_at > $1::timestamptz - interval '24 hours'`,
      [now.toISOString()],
    );
    return Number(row?.count ?? 0);
  }

  /** Replaces the candidates and their figures, keeping what an earlier inspection learned. */
  private async store(
    changed: ReadonlyMap<string, string>,
    now: Date,
  ): Promise<void> {
    const figures = await this.reports.pageFigures(FIGURE_DAYS, now);
    const paths = [...changed.keys()];
    await this.db.transaction(async (tx) => {
      await tx.query(
        `DELETE FROM search_console_reindex_pages WHERE NOT (path = ANY($1::varchar[]))`,
        [paths],
      );
      await tx.query(
        `INSERT INTO search_console_reindex_pages (path, changed_on, impressions, clicks, position)
         SELECT * FROM unnest($1::varchar[], $2::date[], $3::int[], $4::int[], $5::float8[])
         ON CONFLICT (path) DO UPDATE
            SET changed_on = EXCLUDED.changed_on, impressions = EXCLUDED.impressions,
                clicks = EXCLUDED.clicks, position = EXCLUDED.position`,
        [
          paths,
          paths.map((path) => changed.get(path)),
          paths.map((path) => figures.get(path)?.impressions ?? 0),
          paths.map((path) => figures.get(path)?.clicks ?? 0),
          paths.map((path) => figures.get(path)?.position ?? null),
        ],
      );
    });
  }

  /** Every `<url>` of the sitemaps, the children of an index included. */
  private async sitemapEntries(): Promise<SitemapEntry[]> {
    const entries: SitemapEntry[] = [];
    for (const sitemap of SITEMAPS) {
      const top = parseSitemap(
        await this.fetchText(INSPECTION_ORIGIN + sitemap),
      );
      if (!top.index) {
        entries.push(...top.entries);
        continue;
      }
      for (const child of top.entries) {
        entries.push(...parseSitemap(await this.fetchText(child.loc)).entries);
      }
    }
    return entries;
  }

  private async fetchText(url: string): Promise<string> {
    const response = await fetch(url, {
      signal: AbortSignal.timeout(SITEMAP_TIMEOUT_MS),
      headers: { "user-agent": "park.fan-api (reindex queue)" },
    });
    if (!response.ok) throw new Error(`${url} answered ${response.status}`);
    return response.text();
  }
}
