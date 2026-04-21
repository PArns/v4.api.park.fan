import { Injectable, Logger } from "@nestjs/common";
import axios, { AxiosInstance } from "axios";
import {
  QueueTimesParksResponse,
  QueueTimesParkQueueData,
} from "./queue-times.types";
import { logExternalApiError } from "../../common/utils/file-logger.util";

const TIMEOUT_MS = 15_000;
const MAX_RETRIES = 2;
const RETRY_BASE_MS = 1_000;

function isRetryableError(error: unknown): boolean {
  if (error instanceof AggregateError) return true;
  if (!(error instanceof Error)) return false;
  const msg = error.message;
  return (
    msg.includes("ETIMEDOUT") ||
    msg.includes("EAI_AGAIN") ||
    msg.includes("ENOTFOUND") ||
    msg.includes("ECONNRESET") ||
    msg.includes("ECONNREFUSED")
  );
}

/**
 * Queue-Times.com API Client
 *
 * Wrapper for Queue-Times.com REST API
 * API Docs: https://queue-times.com/pages/api
 *
 * Rate Limiting: Unknown (not documented)
 * Attribution Required: "Powered by Queue-Times.com" in README
 */
@Injectable()
export class QueueTimesClient {
  private readonly logger = new Logger(QueueTimesClient.name);
  private readonly client: AxiosInstance;
  private readonly baseUrl = "https://queue-times.com";

  constructor() {
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: TIMEOUT_MS,
    });
  }

  /**
   * GET /parks.json
   *
   * Fetches all parks grouped by park group
   */
  async getParks(): Promise<QueueTimesParksResponse[]> {
    const url = "/parks.json";

    try {
      const response = await this.client.get<QueueTimesParksResponse[]>(url);
      return response.data;
    } catch (error) {
      this.logger.error(`Error fetching parks from Queue-Times: ${error}`);
      logExternalApiError("QueueTimesClient", "getParks", error, { url });
      throw error;
    }
  }

  /**
   * GET /parks/{parkId}/queue_times.json
   *
   * Fetches queue times for a specific park with exponential-backoff retry
   * for transient network errors (ETIMEDOUT, EAI_AGAIN, ECONNRESET).
   *
   * @param parkId - Queue-Times park ID (numeric)
   */
  async getParkQueueTimes(parkId: number): Promise<QueueTimesParkQueueData> {
    if (!Number.isInteger(parkId) || parkId <= 0) {
      throw new Error(`Invalid parkId: ${parkId}. Must be a positive integer.`);
    }

    const url = `/parks/${parkId}/queue_times.json`;
    let lastError: unknown;

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      if (attempt > 0) {
        const delay = RETRY_BASE_MS * 2 ** (attempt - 1); // 1s, 2s
        this.logger.debug(
          `Retrying park ${parkId} (attempt ${attempt}/${MAX_RETRIES}) after ${delay}ms`,
        );
        await new Promise((r) => setTimeout(r, delay));
      }

      try {
        const response = await this.client.get<QueueTimesParkQueueData>(url);
        return response.data;
      } catch (error) {
        lastError = error;

        if (!isRetryableError(error) || attempt === MAX_RETRIES) {
          break;
        }
      }
    }

    const error = lastError;
    const errorMessage = error instanceof Error ? error.message : String(error);
    const isNetwork = isRetryableError(error);

    if (isNetwork) {
      const causes =
        error instanceof AggregateError && error.errors?.length
          ? error.errors.map((e: unknown) => String(e)).join(", ")
          : errorMessage;
      this.logger.warn(
        `Network error fetching queue times for park ${parkId} (after ${MAX_RETRIES} retries): ${causes}`,
      );
    } else {
      this.logger.error(
        `Error fetching queue times for park ${parkId}: ${errorMessage}`,
      );
    }

    logExternalApiError("QueueTimesClient", "getParkQueueTimes", error, {
      parkId,
      url,
    });

    throw error;
  }
}
