import { Inject, Injectable, Logger } from "@nestjs/common";
import type Redis from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  SearchConsoleApiError,
  SearchConsoleClient,
  type GscInspectionApiResult,
} from "./search-console.client";
import {
  INSPECTION_CACHE_PREFIX,
  INSPECTION_CACHE_SECONDS,
  INSPECTION_COUNT_PREFIX,
  INSPECTION_DAILY_LIMIT,
  INSPECTION_ORIGIN,
  MAX_INSPECTION_URLS,
} from "./search-console.constants";

/** What Google says of one URL, reduced to what decides whether and why it is indexed. */
export interface InspectionResult {
  url: string;
  verdict: string | null;
  coverageState: string | null;
  indexingState: string | null;
  robotsTxtState: string | null;
  pageFetchState: string | null;
  lastCrawlTime: string | null;
  crawledAs: string | null;
  googleCanonical: string | null;
  userCanonical: string | null;
  /** Whether Google chose the canonical the page declares; null when either is unknown. */
  canonicalMatches: boolean | null;
  sitemap: string[];
  referringUrls: string[];
  mobileUsability?: { verdict: string | null; issues: string[] };
  richResults?: { verdict: string | null; types: string[] };
}

/** One URL of a call: its result, or why there is none. */
export type InspectionOutcome =
  (InspectionResult & { cached: boolean }) | { url: string; error: string };

export interface InspectionReport {
  results: InspectionOutcome[];
  quota: { usedToday: number | null; dailyLimit: number };
}

/** A URL the tool will not send to Google, with the reason. */
export class InspectionInputError extends Error {}

const HOSTS = new Set(["park.fan", "www.park.fan"]);

/**
 * A path (`/de/parks/…`) or a park.fan URL as the absolute URL to inspect:
 * `https://park.fan` plus the path, `www` and `http` folded in, query and
 * fragment dropped. Anything on another host is refused: the property would
 * answer it with a 400 that costs a request of the quota.
 */
export function normalizeInspectionUrl(input: string): string {
  const text = input.trim();
  if (!text) throw new InspectionInputError("An empty URL");
  let url: URL;
  try {
    url = text.startsWith("/")
      ? new URL(text, INSPECTION_ORIGIN)
      : new URL(/^[a-z][a-z0-9+.-]*:/i.test(text) ? text : `https://${text}`);
  } catch {
    throw new InspectionInputError(`"${text.slice(0, 100)}" is not a URL`);
  }
  if (!HOSTS.has(url.hostname.toLowerCase())) {
    throw new InspectionInputError(
      `"${url.hostname}" is not park.fan: only the property's own pages can be inspected`,
    );
  }
  return `${INSPECTION_ORIGIN}${url.pathname}`;
}

const text = (value: string | undefined): string | null => value ?? null;

/** The compact result for Google's answer. */
export function mapInspection(
  url: string,
  api: GscInspectionApiResult,
): InspectionResult {
  const result = api.inspectionResult ?? {};
  const index = result.indexStatusResult ?? {};
  const mobile = result.mobileUsabilityResult;
  const rich = result.richResultsResult;
  return {
    url,
    verdict: text(index.verdict),
    coverageState: text(index.coverageState),
    indexingState: text(index.indexingState),
    robotsTxtState: text(index.robotsTxtState),
    pageFetchState: text(index.pageFetchState),
    lastCrawlTime: text(index.lastCrawlTime),
    crawledAs: text(index.crawledAs),
    googleCanonical: text(index.googleCanonical),
    userCanonical: text(index.userCanonical),
    canonicalMatches:
      index.googleCanonical && index.userCanonical
        ? index.googleCanonical === index.userCanonical
        : null,
    sitemap: index.sitemap ?? [],
    referringUrls: (index.referringUrls ?? []).slice(0, 10),
    ...(mobile && {
      mobileUsability: {
        verdict: text(mobile.verdict),
        issues: (mobile.issues ?? []).flatMap((issue) =>
          issue.issueType ? [issue.issueType] : [],
        ),
      },
    }),
    ...(rich && {
      richResults: {
        verdict: text(rich.verdict),
        types: (rich.detectedItems ?? []).flatMap((item) =>
          item.richResultType ? [item.richResultType] : [],
        ),
      },
    }),
  };
}

/** Google's quota day is Pacific time. */
export function quotaDay(now: Date): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles",
  }).format(now);
}

/**
 * URL inspection through the Search Console API: one request per URL, spared
 * by a six hour cache in Redis and a daily counter that stops short of
 * Google's 2,000 a day. docs/frontend/search-console.md.
 */
@Injectable()
export class SearchConsoleInspectionService {
  private readonly logger = new Logger(SearchConsoleInspectionService.name);

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly client: SearchConsoleClient,
  ) {}

  /**
   * Inspects up to `MAX_INSPECTION_URLS` paths or URLs, one after the other.
   * A URL Google refuses, or one past the daily limit, has an `error` in
   * place of a result; a refusal of the account stops the call.
   */
  async inspect(
    inputs: readonly string[],
    now = new Date(),
  ): Promise<InspectionReport> {
    if (inputs.length === 0 || inputs.length > MAX_INSPECTION_URLS) {
      throw new InspectionInputError(
        `Give between 1 and ${MAX_INSPECTION_URLS} URLs`,
      );
    }
    const setup = this.client.setup();
    if (!setup.configured) {
      throw new InspectionInputError(
        `Search Console is not configured${setup.reason ? `: ${setup.reason}` : ""}`,
      );
    }
    const urls = [...new Set(inputs.map(normalizeInspectionUrl))];
    const day = quotaDay(now);
    const results: InspectionOutcome[] = [];
    let usedToday: number | null = null;
    for (const url of urls) {
      const cached = await this.cached(url);
      if (cached) {
        results.push({ ...cached, cached: true });
        continue;
      }
      const used = await this.reserve(day);
      if (used !== null && used > INSPECTION_DAILY_LIMIT) {
        usedToday = INSPECTION_DAILY_LIMIT;
        results.push({
          url,
          error: `Daily inspection limit reached (${INSPECTION_DAILY_LIMIT} of Google's 2,000 a day, which resets at midnight Pacific time); cached answers still work`,
        });
        continue;
      }
      usedToday = used;
      try {
        const result = mapInspection(url, await this.client.inspectUrl(url));
        await this.store(url, result);
        results.push({ ...result, cached: false });
      } catch (error) {
        if (error instanceof SearchConsoleApiError && !error.fatal) {
          results.push({ url, error: error.message });
          continue;
        }
        throw error;
      }
    }
    return {
      results,
      quota: {
        usedToday: usedToday ?? (await this.used(day)),
        dailyLimit: INSPECTION_DAILY_LIMIT,
      },
    };
  }

  private async cached(url: string): Promise<InspectionResult | null> {
    try {
      const value = await this.redis.get(INSPECTION_CACHE_PREFIX + url);
      return value ? (JSON.parse(value) as InspectionResult) : null;
    } catch (error) {
      this.logger.warn(
        `Inspection cache unavailable: ${(error as Error).message}`,
      );
      return null;
    }
  }

  private async store(url: string, result: InspectionResult): Promise<void> {
    try {
      await this.redis.set(
        INSPECTION_CACHE_PREFIX + url,
        JSON.stringify(result),
        "EX",
        INSPECTION_CACHE_SECONDS,
      );
    } catch {
      // Not cached; the next call asks Google again.
    }
  }

  /**
   * Counts one inspection against today and returns the count; null when
   * Redis is away, in which case Google's own limit is the only one. A count
   * past the limit is given back.
   */
  private async reserve(day: string): Promise<number | null> {
    const key = INSPECTION_COUNT_PREFIX + day;
    try {
      const count = await this.redis.incr(key);
      if (count === 1) await this.redis.expire(key, 2 * 24 * 60 * 60);
      if (count > INSPECTION_DAILY_LIMIT) await this.redis.decr(key);
      return count;
    } catch (error) {
      this.logger.warn(
        `Inspection counter unavailable: ${(error as Error).message}`,
      );
      return null;
    }
  }

  private async used(day: string): Promise<number | null> {
    try {
      return Number(await this.redis.get(INSPECTION_COUNT_PREFIX + day)) || 0;
    } catch {
      return null;
    }
  }
}
