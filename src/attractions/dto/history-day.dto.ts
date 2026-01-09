import { ApiProperty } from "@nestjs/swagger";
import { CrowdLevel } from "../../common/types/crowd-level.type";

/**
 * History Day DTO
 *
 * Represents historical data for a single day, including:
 * - Daily utilization (crowd level)
 * - Hourly P90 wait times
 * - Down count (number of downtime events)
 */
export class HistoryDayDto {
  @ApiProperty({
    description: "Date in YYYY-MM-DD format (park timezone)",
    example: "2025-01-15",
  })
  date: string;

  @ApiProperty({
    description: "Daily utilization (crowd level) for this day",
    enum: [
      "very_low",
      "low",
      "moderate",
      "high",
      "very_high",
      "extreme",
      "closed",
    ],
    example: "moderate",
  })
  utilization: CrowdLevel | "closed";

  @ApiProperty({
    description:
      "Hourly P90 wait times (one value per hour during operating hours)",
    type: Array,
    isArray: true,
    example: [
      { hour: "09:00", value: 15 },
      { hour: "10:00", value: 25 },
      { hour: "11:00", value: 35 },
    ],
  })
  hourlyP90: Array<{ hour: string; value: number }>;

  @ApiProperty({
    description: "Number of times the attraction was DOWN on this day",
    example: 2,
  })
  downCount: number;
}
