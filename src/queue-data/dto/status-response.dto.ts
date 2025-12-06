import {
  LiveStatus,
  QueueType,
} from "../../external-apis/themeparks/themeparks.types";

/**
 * Response DTO for GET /v1/attractions/:slug/status
 * Returns current real-time status only (no historical data)
 */
export class StatusResponseDto {
  attraction: {
    id: string;
    name: string;
    slug: string;
  };

  park: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
  };

  status: LiveStatus;

  queues: {
    queueType: QueueType;
    status: LiveStatus;
    waitTime?: number;
    state?: string;
    returnStart?: string;
    returnEnd?: string;
    price?: {
      amount: number;
      currency: string;
      formatted?: string;
    };
    allocationStatus?: string;
    currentGroupStart?: number;
    currentGroupEnd?: number;
    estimatedWait?: number;
  }[];

  lastUpdated: string; // ISO 8601
}
