import { Injectable, Logger } from "@nestjs/common";
import axios, { AxiosInstance } from "axios";
import {
  QueueTimesParksResponse,
  QueueTimesParkQueueData,
} from "./queue-times.types";
import { logExternalApiError } from "../../common/utils/file-logger.util";

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
      timeout: 30000, // 30 seconds (increased from 10s to reduce timeout errors)
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

      // Log to dedicated file for later analysis
      logExternalApiError("QueueTimesClient", "getParks", error, { url });

      throw error;
    }
  }

  /**
   * GET /parks/{parkId}/queue_times.json
   *
   * Fetches queue times for a specific park (rides grouped by lands)
   * Timestamps are in UTC
   *
   * @param parkId - Queue-Times park ID (numeric)
   */
  async getParkQueueTimes(parkId: number): Promise<QueueTimesParkQueueData> {
    // SECURITY: Validate parkId to prevent injection
    if (!Number.isInteger(parkId) || parkId <= 0) {
      throw new Error(`Invalid parkId: ${parkId}. Must be a positive integer.`);
    }

    const url = `/parks/${parkId}/queue_times.json`;

    try {
      const response = await this.client.get<QueueTimesParkQueueData>(url);
      return response.data;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      const isNetworkError =
        error instanceof AggregateError ||
        errorMessage.includes("ENOTFOUND") ||
        errorMessage.includes("ECONNREFUSED") ||
        errorMessage.includes("ETIMEDOUT");

      if (isNetworkError) {
        // AggregateError wraps multiple causes — log each inner error for diagnosis
        const causes =
          error instanceof AggregateError && error.errors?.length
            ? error.errors.map((e: unknown) => String(e)).join(", ")
            : errorMessage;
        this.logger.warn(
          `Network error fetching queue times for park ${parkId}: ${causes}`,
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
}
