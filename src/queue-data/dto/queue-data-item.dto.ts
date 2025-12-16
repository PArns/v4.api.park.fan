import {
  QueueType,
  LiveStatus,
} from "../../external-apis/themeparks/themeparks.types";

/**
 * Represents a single queue data entry
 * Polymorphic design: fields are nullable based on queue type
 */
export class QueueDataItemDto {
  queueType: QueueType;
  status: LiveStatus;

  // STANDBY, SINGLE_RIDER, PAID_STANDBY
  waitTime?: number | null;

  // RETURN_TIME, PAID_RETURN_TIME
  state?: string | null;
  returnStart?: string | null; // ISO 8601
  returnEnd?: string | null; // ISO 8601

  // PAID_RETURN_TIME, PAID_STANDBY
  price?: {
    amount: number;
    currency: string;
    formatted?: string;
  } | null;

  // BOARDING_GROUP
  allocationStatus?: string | null;
  currentGroupStart?: number | null;
  currentGroupEnd?: number | null;
  estimatedWait?: number | null;

  // Timestamps
  lastUpdated: string; // ISO 8601 - from API (most relevant for users)

  // Phase 5.5: Wait time trends (last 2-3 hours)
  trend?: {
    direction: "increasing" | "stable" | "decreasing";
    changeRate: number; // Minutes per hour (positive = increasing, negative = decreasing)
    recentAverage: number | null; // Last hour average wait time
    previousAverage: number | null; // 2-3 hours ago average wait time
  };
}
