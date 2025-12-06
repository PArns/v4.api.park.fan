import { QueueDataItemDto } from "./queue-data-item.dto";
import { PaginationDto } from "../../common/dto/pagination.dto";

/**
 * Response DTO for GET /v1/attractions/:slug/wait-times
 * Returns historical and current wait time data
 */
export class WaitTimesResponseDto {
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

  waitTimes: QueueDataItemDto[];
  pagination?: PaginationDto;
}
