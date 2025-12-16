import { ApiProperty } from "@nestjs/swagger";
import { HolidayItemDto } from "./holiday-item.dto";

/**
 * Holiday Response DTO
 *
 * Returns holidays for a country or park
 */
export class HolidayResponseDto {
  @ApiProperty({
    description: "List of holidays",
    type: [HolidayItemDto],
  })
  holidays: HolidayItemDto[];
}
