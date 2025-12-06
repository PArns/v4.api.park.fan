import { Injectable, Logger } from "@nestjs/common";
import {
  QueueTimesParksResponse,
  QueueTimesParkQueueData,
} from "./queue-times.types";

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
  private readonly baseUrl = "https://queue-times.com";

  /**
   * GET /parks.json
   *
   * Fetches all parks grouped by park group
   */
  async getParks(): Promise<QueueTimesParksResponse[]> {
    const url = `${this.baseUrl}/parks.json`;

    try {
      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(
          `Failed to fetch parks: ${response.status} ${response.statusText}`,
        );
      }

      return response.json();
    } catch (error) {
      this.logger.error(`Error fetching parks from Queue-Times: ${error}`);
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
    const url = `${this.baseUrl}/parks/${parkId}/queue_times.json`;

    try {
      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(
          `Failed to fetch queue times for park ${parkId}: ${response.status} ${response.statusText}`,
        );
      }

      return response.json();
    } catch (error) {
      this.logger.error(
        `Error fetching queue times for park ${parkId}: ${error}`,
      );
      throw error;
    }
  }
}
