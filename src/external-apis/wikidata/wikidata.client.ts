import { Injectable, Logger } from "@nestjs/common";
import axios, { AxiosInstance } from "axios";
import { logExternalApiError } from "../../common/utils/file-logger.util";
import {
  buildStatsQuery,
  parseStatsResults,
  type SparqlResults,
  type WikidataRideStats,
} from "./wikidata.parser";

const TIMEOUT_MS = 60_000;

/**
 * Wikidata asks for a descriptive User-Agent naming the tool and a way to reach
 * its operator, and enforces it — the generic browser header this project sends
 * to scrape-resistant sites is the wrong thing here.
 *
 * @see https://foundation.wikimedia.org/wiki/Policy:User-Agent_policy
 */
const USER_AGENT = "park.fan-ride-stats/1.0 (https://park.fan; info@park.fan)";

/**
 * Reads ride measurements from the Wikidata Query Service.
 *
 * One SPARQL request answers for hundreds of rides at once, which is why this
 * has no rate limiting to speak of: the whole catalogue is a handful of
 * queries, not one per ride.
 */
@Injectable()
export class WikidataClient {
  private readonly logger = new Logger(WikidataClient.name);
  private readonly http: AxiosInstance;

  constructor() {
    this.http = axios.create({
      baseURL: "https://query.wikidata.org",
      timeout: TIMEOUT_MS,
      headers: {
        "User-Agent": USER_AGENT,
        Accept: "application/sparql-results+json",
      },
    });
  }

  /**
   * Measurements for a batch of RCDB ids, keyed by id.
   *
   * Returns an empty map when the query fails: a batch we could not fetch is
   * indistinguishable from a batch Wikidata knows nothing about, and both mean
   * "nothing to write this run".
   */
  async fetchRideStats(
    rcdbIds: readonly number[],
  ): Promise<Map<string, WikidataRideStats>> {
    if (rcdbIds.length === 0) return new Map();

    try {
      const response = await this.http.post<SparqlResults>(
        "/sparql",
        // POST, not a query string: a few hundred ids make for a URL that
        // proxies and servers are entitled to reject.
        new URLSearchParams({ query: buildStatsQuery(rcdbIds) }).toString(),
        { headers: { "Content-Type": "application/x-www-form-urlencoded" } },
      );
      return parseStatsResults(response.data);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Wikidata query failed for ${rcdbIds.length} id(s): ${message}`,
      );
      logExternalApiError("wikidata", "/sparql", message);
      return new Map();
    }
  }
}
