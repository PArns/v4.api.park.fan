import { LiveStatus } from "../../external-apis/themeparks/themeparks.types";
import { ApiProperty } from "@nestjs/swagger";
import { QueueDataItemDto } from "./queue-data-item.dto";

export class QueuePriceDto {
  @ApiProperty({ description: "Price amount" })
  amount: number;
  @ApiProperty({ description: "Currency code (e.g., USD)" })
  currency: string;
  @ApiProperty({ description: "Formatted price string", required: false })
  formatted?: string;
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
