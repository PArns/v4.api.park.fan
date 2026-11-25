import { BadGatewayException, Injectable, Logger } from "@nestjs/common";
import axios from "axios";
import type { OverpassResponse } from "./overpass.parser";

/** Names the tool and where its operator is found; no person, no address. */
const USER_AGENT = "park.fan-facilities/1.0 (+https://park.fan)";

/**
 * The waits before a refused query is sent again. The public instance's
 * policy: a request the rate limit denies gets 429 and "the client framework
 * shall resubmit the requests after the 15 seconds"; a full queue answers 502
 * or 504 the same way. Four more tries, each wait twice the last.
 */
export const OVERPASS_BACKOFF_MS = [15_000, 30_000, 60_000, 120_000];

const BUSY = new Set([429, 502, 504]);

/**
 * One query at a time against the public Overpass API. Its usage policy asks
 * for "a maximum of about 10000 requests per day" and less than 1 GB; a full
 * run over every park is two small queries per park, about 400 in all.
 */
@Injectable()
export class OverpassClient {
  private readonly logger = new Logger(OverpassClient.name);

  /** Overridden in specs, which do not wait. */
  protected sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async query(ql: string): Promise<OverpassResponse> {
    for (let attempt = 0; ; attempt++) {
      try {
        const { data } = await axios.post<OverpassResponse>(
          process.env.OVERPASS_URL ?? "https://overpass-api.de/api/interpreter",
          new URLSearchParams({ data: ql }).toString(),
          {
            timeout: 90_000,
            headers: {
              "User-Agent": USER_AGENT,
              "Content-Type": "application/x-www-form-urlencoded",
            },
          },
        );
        // A timeout or memory limit arrives as HTTP 200 with a `remark` and
        // part of the elements. Importing that would delete what it left out.
        if (data?.remark || !Array.isArray(data?.elements)) {
          throw new BadGatewayException(
            `Overpass answered incompletely: ${data?.remark ?? "no elements"}`,
          );
        }
        return data;
      } catch (error) {
        if (error instanceof BadGatewayException) throw error;
        const status = axios.isAxiosError(error)
          ? error.response?.status
          : undefined;
        const wait = OVERPASS_BACKOFF_MS[attempt];
        if (status !== undefined && BUSY.has(status) && wait !== undefined) {
          this.logger.warn(`Overpass ${status}, again in ${wait / 1000} s`);
          await this.sleep(wait);
          continue;
        }
        const message = error instanceof Error ? error.message : String(error);
        throw new BadGatewayException(`Overpass did not answer: ${message}`);
      }
    }
  }
}
