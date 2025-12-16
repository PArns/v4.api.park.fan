import {
  LiveStatus,
  QueueType,
} from "../../external-apis/themeparks/themeparks.types";
import { ApiProperty } from "@nestjs/swagger";

export class QueuePriceDto {
  @ApiProperty({ description: "Price amount" })
  amount: number;
  @ApiProperty({ description: "Currency code (e.g., USD)" })
  currency: string;
  @ApiProperty({ description: "Formatted price string", required: false })
  formatted?: string;
}

/**
 * DTO for a single queue item within StatusResponseDto
 */
export class QueueDataItemDto {
  @ApiProperty({
    enum: QueueType,
    description: "Type of queue (e.g., STANDBY, SINGLE_RIDER)",
  })
  queueType: QueueType;

  @ApiProperty({
    enum: LiveStatus,
    description: "Status of this specific queue",
  })
  status: LiveStatus;

  @ApiProperty({
    description: "Current wait time in minutes, if applicable",
    type: Number,
    required: false,
  })
  waitTime?: number;

  @ApiProperty({
    description: "Current state of the queue (e.g., 'OPEN', 'CLOSED')",
    type: String,
    required: false,
  })
  state?: string;

  @ApiProperty({
    description: "Start time for return entry (ISO 8601), if applicable",
    type: String,
    format: "date-time",
    required: false,
  })
  returnStart?: string;

  @ApiProperty({
    description: "End time for return entry (ISO 8601), if applicable",
    type: String,
    format: "date-time",
    required: false,
  })
  returnEnd?: string;

  @ApiProperty({
    description: "Price details for paid queues (e.g., Lightning Lane)",
    type: QueuePriceDto,
    required: false,
  })
  price?: QueuePriceDto;

  @ApiProperty({
    description:
      "Allocation status for virtual queues (e.g., 'AVAILABLE', 'FULL')",
    type: String,
    required: false,
  })
  allocationStatus?: string;

  @ApiProperty({
    description: "Current boarding group start number, if applicable",
    type: Number,
    required: false,
  })
  currentGroupStart?: number;

  @ApiProperty({
    description: "Current boarding group end number, if applicable",
    type: Number,
    required: false,
  })
  currentGroupEnd?: number;

  @ApiProperty({
    description:
      "Estimated wait time in minutes for virtual queues, if applicable",
    type: Number,
    required: false,
  })
  estimatedWait?: number;
}

/**
 * Response DTO for GET /v1/attractions/:slug/status
 * Returns current real-time status only (no historical data)
 */
export class StatusResponseDto {
  @ApiProperty({ description: "Attraction details" })
  attraction: {
    id: string;
    name: string;
    slug: string;
  };

  @ApiProperty({ description: "Park details" })
  park: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
  };

  @ApiProperty({
    description: "Current overall status (e.g., OPERATING, CLOSED)",
    enum: LiveStatus,
  })
  status: LiveStatus;

  @ApiProperty({
    description: "List of queue-specific status data",
    type: [QueueDataItemDto],
  })
  queues: QueueDataItemDto[];

  @ApiProperty({
    description: "Timestamp of the last status update",
    type: String,
    format: "date-time",
  })
  lastUpdated: string; // ISO 8601
}
