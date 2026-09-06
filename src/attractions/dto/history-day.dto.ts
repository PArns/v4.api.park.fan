import { ApiProperty } from "@nestjs/swagger";
import {
  CROWD_LEVEL_WITH_CLOSED_VALUES,
  CrowdLevel,
} from "../../common/types/crowd-level.type";

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
    enum: CROWD_LEVEL_WITH_CLOSED_VALUES,
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
    description:
      "DEPRECATED — this is not an event count. It is the number of distinct " +
      "clock hours in which a DOWN reading was recorded, across the whole " +
      "park-local calendar day and WITHOUT an opening-hours bound. Two halts " +
      "in one hour read as 1; a four-day outage reads as about 40; and an " +
      "outage that starts before closing keeps accruing hours overnight " +
      "(Universal Studios Singapore's Revenge of the Mummy scored 16 on a park " +
      "day running 10:00 to 20:00). Use `outageCount` and `outageMinutes`, " +
      "which are derived from reconstructed intervals and bounded by the " +
      "park's hours.",
    deprecated: true,
    example: 2,
  })
  downCount: number;

  @ApiProperty({
    description:
      "Outages that STARTED on this day, from the reconstructed intervals. A " +
      "multi-day outage increments exactly one day — the one it began on. " +
      "Zero is a real answer; absence of the field is not.",
    example: 1,
    required: false,
  })
  outageCount?: number;

  @ApiProperty({
    description:
      "Minutes this attraction was reported DOWN inside the park's operating " +
      "hours on this day, from the reconstructed intervals. A multi-day outage " +
      "contributes its share to each day it touches, which is why this and " +
      "`outageCount` answer different questions and must not be divided by one " +
      "another.",
    example: 45,
    required: false,
  })
  outageMinutes?: number;
}
