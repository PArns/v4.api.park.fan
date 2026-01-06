import { ApiProperty } from "@nestjs/swagger";
import {
  QueueType,
  LiveStatus,
} from "../../external-apis/themeparks/themeparks.types";

/**
 * Represents a single queue data entry
 * Polymorphic design: fields are nullable based on queue type
 */
export class QueueDataItemDto {
  @ApiProperty({
    description: "Type of the queue",
    enum: [
      "STANDBY",
      "SINGLE_RIDER",
      "RETURN_TIME",
      "BOARDING_GROUP",
      "PAID_RETURN_TIME",
      "PAID_STANDBY",
    ],
  })
  queueType: QueueType;

  @ApiProperty({
    description: "Current operating status",
    enum: ["OPERATING", "DOWN", "CLOSED", "REFURBISHMENT"],
  })
  status: LiveStatus;

  // STANDBY, SINGLE_RIDER, PAID_STANDBY
  @ApiProperty({
    description: "Current wait time in minutes",
    required: false,
    nullable: true,
  })
  waitTime?: number | null;

  // RETURN_TIME, PAID_RETURN_TIME
  @ApiProperty({
    description: "Current state/message",
    required: false,
    nullable: true,
  })
  state?: string | null;

  @ApiProperty({
    description: "Return window start time",
    required: false,
    nullable: true,
  })
  returnStart?: string | null; // ISO 8601

  @ApiProperty({
    description: "Return window end time",
    required: false,
    nullable: true,
  })
  returnEnd?: string | null; // ISO 8601

  // PAID_RETURN_TIME, PAID_STANDBY
  @ApiProperty({
    description: "Price information",
    required: false,
    nullable: true,
  })
  price?: {
    amount: number;
    currency: string;
    formatted?: string;
  } | null;

  // BOARDING_GROUP
  @ApiProperty({
    description: "Allocation status",
    required: false,
    nullable: true,
  })
  allocationStatus?: string | null;

  @ApiProperty({
    description: "Current boarding group start",
    required: false,
    nullable: true,
  })
  currentGroupStart?: number | null;

  @ApiProperty({
    description: "Current boarding group end",
    required: false,
    nullable: true,
  })
  currentGroupEnd?: number | null;

  @ApiProperty({
    description: "Estimated wait time",
    required: false,
    nullable: true,
  })
  estimatedWait?: number | null;

  // Timestamps
  @ApiProperty({ description: "Last updated timestamp (ISO 8601)" })
  lastUpdated: string; // ISO 8601 - from API (most relevant for users)

  // Phase 5.5: Wait time trends (last 2-3 hours)
  @ApiProperty({ description: "Wait time trend direction", required: false })
  trend?: {
    direction: "increasing" | "stable" | "decreasing";
    changeRate: number; // Minutes per hour (positive = increasing, negative = decreasing)
    recentAverage: number | null; // Last hour average wait time
    previousAverage: number | null; // 2-3 hours ago average wait time
  };

  /**
   * Create QueueDataItemDto from QueueData entity
   * Centralizes DTO transformation logic to eliminate code duplication
   *
   * @param queueData - QueueData entity from database
   * @returns Transformed DTO
   */
  static fromEntity(queueData: any): QueueDataItemDto {
    return {
      queueType: queueData.queueType,
      status: queueData.status,
      waitTime: queueData.waitTime ?? null,
      state: queueData.state ?? null,
      returnStart: queueData.returnStart
        ? queueData.returnStart.toISOString()
        : null,
      returnEnd: queueData.returnEnd ? queueData.returnEnd.toISOString() : null,
      price: queueData.price ?? null,
      allocationStatus: queueData.allocationStatus ?? null,
      currentGroupStart: queueData.currentGroupStart ?? null,
      currentGroupEnd: queueData.currentGroupEnd ?? null,
      estimatedWait: queueData.estimatedWait ?? null,
      lastUpdated: (queueData.lastUpdated || queueData.timestamp).toISOString(),
    };
  }
}
