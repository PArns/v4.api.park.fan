import { ApiProperty } from "@nestjs/swagger";
import { QueueDataItemDto } from "./queue-data-item.dto";
import { PaginationDto } from "../../common/dto/pagination.dto";

/**
 * Response DTO for GET /v1/attractions/:slug/wait-times
 * Returns historical and current wait time data
 */
export class WaitTimesResponseDto {
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
    description: "List of historical wait time entries",
    type: [QueueDataItemDto],
  })
  waitTimes: QueueDataItemDto[];

  @ApiProperty({ description: "Pagination metadata" })
  pagination: PaginationDto;
}
