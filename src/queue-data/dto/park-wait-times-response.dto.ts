import { ApiProperty } from "@nestjs/swagger";
import { QueueDataItemDto } from "./queue-data-item.dto";
import { ParkStatus } from "../../common/types/status.type";

/**
 * Attraction with wait times
 */
export class AttractionWaitTimesDto {
  @ApiProperty({ description: "Attraction details" })
  attraction: {
    id: string;
    name: string;
    slug: string;
  };

  @ApiProperty({
    description: "List of queue data entries for this attraction",
    type: [QueueDataItemDto],
  })
  queues: QueueDataItemDto[];
}

/**
 * Response DTO for GET /v1/parks/:slug/wait-times
 * Returns current wait times for all attractions in a park
 */
export class ParkWaitTimesResponseDto {
  @ApiProperty({
    description: "Park details",
    example: {
      id: "uuid",
      name: "Magic Kingdom",
      slug: "magic-kingdom",
      timezone: "America/New_York",
      status: "OPERATING",
    },
  })
  park: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
    status?: ParkStatus;
  };

  @ApiProperty({
    description: "List of attractions with their wait times",
    type: [AttractionWaitTimesDto],
  })
  attractions: AttractionWaitTimesDto[];
}
