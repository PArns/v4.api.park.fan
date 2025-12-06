import { ScheduleItemDto } from "./schedule-item.dto";

/**
 * Schedule Response DTO
 *
 * Returns schedule data for a park
 */
export class ScheduleResponseDto {
  park: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
  };
  schedule: ScheduleItemDto[];
}
