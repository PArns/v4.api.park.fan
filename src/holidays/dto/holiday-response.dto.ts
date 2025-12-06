import { HolidayItemDto } from "./holiday-item.dto";

/**
 * Holiday Response DTO
 *
 * Returns holidays for a country or park
 */
export class HolidayResponseDto {
  holidays: HolidayItemDto[];
}
