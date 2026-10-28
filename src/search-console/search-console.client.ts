import { Injectable, Logger } from "@nestjs/common";
import {
  getSearchConsoleSetup,
  type SearchConsoleSetup,
} from "../config/search-console.config";
import { GoogleAuthError, ServiceAccountTokens } from "./google-auth";
import {
  GSC_API_BASE,
  GSC_MAX_ATTEMPTS,
  GSC_REQUEST_TIMEOUT_MS,
  GSC_ROW_LIMIT,
  GSC_SCOPE,
} from "./search-console.constants";

/** One row of `searchAnalytics.query`, its `keys` in the order of the dimensions asked for. */
export interface GscApiRow {
  keys: string[];
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
}

export type GscDimension = "page" | "query" | "country" | "device";

/**
 * A refusal or failure from the Search Console API. `fatal` means the next
 * request would fail the same way (no access, a wrong property, a bad key), so
 * a run stops there instead of trying every day.
 */
export class SearchConsoleApiError extends Error {
  constructor(
    message: string,
    readonly status: number | null,
    readonly fatal: boolean,
  ) {
    super(message);
  }
}

/** Waits, replaceable in a test. */
export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A small client of Google's Search Console REST API: the service account's
 * access token (`google-auth.ts`) and `searchAnalytics.query`, paged.
 *
 * Written against `fetch` and Node's `crypto` rather than the `googleapis`
 * package (about 190 MB installed, every Google API in it) or
 * `google-auth-library` (a dozen transitive packages in the path of a
 * credential): the import uses one endpoint and one grant type.
 */
@Injectable()
export class SearchConsoleClient {
  private readonly logger = new Logger(SearchConsoleClient.name);
  private tokens: ServiceAccountTokens | null = null;

  /** Pause between tries; a test sets it to nothing. */
  wait: (ms: number) => Promise<void> = sleep;

  setup(): SearchConsoleSetup {
    return getSearchConsoleSetup();
  }

  /** Every row of one day for the dimensions, page after page. */
  async fetchDay(
    day: string,
    dimensions: readonly GscDimension[],
  ): Promise<GscApiRow[]> {
    const setup = this.setup();
    if (!setup.configured) {
      throw new SearchConsoleApiError(
        "Search Console is not configured",
        null,
        true,
      );
    }
    const rows: GscApiRow[] = [];
    for (let startRow = 0; ; startRow += GSC_ROW_LIMIT) {
      const page = await this.query(setup, {
        startDate: day,
        endDate: day,
        dimensions,
        rowLimit: GSC_ROW_LIMIT,
        startRow,
        // Final numbers only: a day Google has not finished comes back empty
        // and is asked again by the next runs.
        dataState: "final",
      });
      rows.push(...page);
      if (page.length < GSC_ROW_LIMIT) return rows;
    }
  }

  private tokensFor(
    setup: Extract<SearchConsoleSetup, { configured: true }>,
  ): ServiceAccountTokens {
    if (!this.tokens || !this.tokens.isFor(setup.account)) {
      this.tokens = new ServiceAccountTokens(setup.account, GSC_SCOPE);
    }
    return this.tokens;
  }

  private async query(
    setup: Extract<SearchConsoleSetup, { configured: true }>,
    body: Record<string, unknown>,
  ): Promise<GscApiRow[]> {
    const url = `${GSC_API_BASE}/sites/${encodeURIComponent(setup.siteUrl)}/searchAnalytics/query`;
    const tokens = this.tokensFor(setup);
    let refreshed = false;
    for (let attempt = 1; ; attempt++) {
      let token: string;
      try {
        token = await tokens.token();
      } catch (error) {
        if (error instanceof GoogleAuthError) {
          throw new SearchConsoleApiError(error.message, null, true);
        }
        throw error;
      }
      let response: Response | null = null;
      let failure = "";
      try {
        response = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(GSC_REQUEST_TIMEOUT_MS),
        });
      } catch (error) {
        failure = (error as Error).message;
      }

      if (response?.ok) {
        const parsed = (await response.json()) as { rows?: GscApiRow[] };
        return parsed.rows ?? [];
      }
      const status = response?.status ?? null;
      if (status === 401 && !refreshed) {
        refreshed = true;
        tokens.invalidate();
        continue;
      }
      const retryable =
        status === null || status === 429 || (status >= 500 && status < 600);
      if (retryable && attempt < GSC_MAX_ATTEMPTS) {
        const retryAfter = Number(response?.headers.get("retry-after"));
        await this.wait(
          Number.isFinite(retryAfter) && retryAfter > 0
            ? Math.min(retryAfter, 60) * 1000
            : 1000 * 2 ** (attempt - 1),
        );
        continue;
      }
      throw await this.refusal(setup, response, failure);
    }
  }

  /** The error for a response that will not become a success. */
  private async refusal(
    setup: Extract<SearchConsoleSetup, { configured: true }>,
    response: Response | null,
    failure: string,
  ): Promise<SearchConsoleApiError> {
    if (!response) {
      return new SearchConsoleApiError(
        `Search Console could not be reached: ${failure}`,
        null,
        false,
      );
    }
    const body = (await response.json().catch(() => ({}))) as {
      error?: { message?: string; status?: string };
    };
    const detail = (body.error?.message ?? "").slice(0, 300);
    const status = response.status;
    if (status === 401 || status === 403) {
      return new SearchConsoleApiError(
        `Search Console refused ${setup.account.clientEmail} for ${setup.siteUrl} (${status}): add the account as a user of the property and enable the Search Console API in its Google Cloud project${detail ? `. Google says: ${detail}` : ""}`,
        status,
        true,
      );
    }
    if (status === 404) {
      return new SearchConsoleApiError(
        `Search Console has no property ${setup.siteUrl} for ${setup.account.clientEmail} (404): check GSC_SITE_URL against the property list`,
        status,
        true,
      );
    }
    this.logger.warn(`Search Console answered ${status}: ${detail}`);
    return new SearchConsoleApiError(
      `Search Console answered ${status}${detail ? `: ${detail}` : ""}`,
      status,
      false,
    );
  }
}
