import { ApiProperty } from "@nestjs/swagger";
import { ParkStatus } from "../../common/types/status.type";
import { CrowdLevel } from "../../common/types/crowd-level.type";
import { InfluencingHoliday } from "./schedule-item.dto";

export { InfluencingHoliday };

/**
 * Calendar Metadata
 */
export class CalendarMeta {
  @ApiProperty({ description: "Park slug" })
  slug: string;

  @ApiProperty({ description: "Park timezone (IANA format)" })
  timezone: string;

  @ApiProperty({
    description: "Whether the park provides official operating hours",
  })
  hasOperatingSchedule: boolean;
}

/**
 * Operating Hours
 */
export class OperatingHours {
  @ApiProperty()
  openingTime: string;

  @ApiProperty()
  closingTime: string;

  @ApiProperty({ enum: ["OPERATING", "CLOSED"] })
  type: string;

  @ApiProperty({
    description: "Whether these hours are reconstructed from activity data",
  })
  isInferred: boolean;
}

/**
 * Weather Summary
 */
export class WeatherSummary {
  @ApiProperty()
  condition: string;

  @ApiProperty()
  icon: number;

  @ApiProperty()
  tempMin: number;

  @ApiProperty()
  tempMax: number;

  @ApiProperty()
  rainChance: number;
}

/**
 * Calendar Event
 *
 * Currently only supports holidays from Nager.Date API
 */
export class CalendarEvent {
  @ApiProperty({ description: "Event name", example: "Christmas Day" })
  name: string;

  @ApiProperty({
    description: "Event type",
    example: "holiday",
    enum: ["holiday", "school-holiday"],
  })
  type: "holiday" | "school-holiday";

  @ApiProperty({
    description: "Whether this is a nationwide holiday",
    required: false,
  })
  isNationwide?: boolean;
}

/**
 * Show Time
 */
export class ShowTime {
  @ApiProperty()
  name: string;

  @ApiProperty()
  time: string;

  @ApiProperty({ required: false })
  endTime?: string;
}

/**
 * Hourly Prediction
 */
export class HourlyPrediction {
  @ApiProperty()
  hour: number;

  @ApiProperty()
  crowdLevel: CrowdLevel;

  @ApiProperty()
  predictedWaitTime: number;

  @ApiProperty({
    required: false,
    description: "Probability/Confidence of prediction (0-1)",
  })
  probability?: number;
}

/**
 * Calendar Day - unified structure
 */
export class CalendarDay {
  @ApiProperty()
  date: string;

  @ApiProperty({
    description:
      "OPERATING (open), CLOSED (confirmed closed), or UNKNOWN (no schedule data yet – not published or placeholder)",
    enum: ["OPERATING", "CLOSED", "UNKNOWN"],
  })
  status: ParkStatus;

  @ApiProperty()
  isToday: boolean;

  @ApiProperty({
    description:
      "Whether this day's status or hours are estimated/reconstructed",
    required: false,
  })
  isEstimated?: boolean;

  @ApiProperty()
  isHoliday: boolean;

  @ApiProperty()
  isBridgeDay: boolean;

  @ApiProperty()
  isSchoolVacation: boolean;

  @ApiProperty({ type: () => OperatingHours, required: false })
  hours?: OperatingHours;

  @ApiProperty()
  crowdLevel: CrowdLevel | "closed";

  @ApiProperty({
    description:
      "Peak crowd level (P90) for this day (historical or predicted)",
    enum: [
      "very_low",
      "low",
      "moderate",
      "high",
      "very_high",
      "extreme",
      "closed",
    ],
    required: false,
  })
  peakLoad?: CrowdLevel | "closed";

  @ApiProperty({ type: () => WeatherSummary, required: false })
  weather?: WeatherSummary;

  @ApiProperty({ type: () => [CalendarEvent], required: false })
  events?: CalendarEvent[];

  @ApiProperty({ type: () => [InfluencingHoliday], required: false })
  influencingHolidays?: InfluencingHoliday[];

  @ApiProperty({
    description:
      "Recommendation for visiting (score-based labels). Omitted for past days.",
    enum: [
      "highly_recommended",
      "recommended",
      "neutral",
      "avoid",
      "strongly_avoid",
      "closed",
    ],
    required: false,
  })
  recommendation?:
    | "highly_recommended"
    | "recommended"
    | "neutral"
    | "avoid"
    | "strongly_avoid"
    | "closed";

  @ApiProperty({
    description: "Hourly predictions (0-23)",
    type: () => [HourlyPrediction],
    required: false,
  })
  hourly?: HourlyPrediction[];
}

/**
 * Metadata for a month response
 */
export class CalendarMonthResponse {
  @ApiProperty({ description: "Month (YYYY-MM)" })
  month: string;

  @ApiProperty({ type: () => [CalendarDay] })
  days: CalendarDay[];
}

/**
 * Full integrated response structure
 */
export class IntegratedCalendarResponse {
  @ApiProperty({ description: "Metadata about the calendar response" })
  meta: CalendarMeta;

  @ApiProperty({
    description: "Array of calendar days with unified structure",
    type: () => [CalendarDay],
  })
  days: CalendarDay[];
}
