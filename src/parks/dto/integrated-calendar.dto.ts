import { ApiProperty } from "@nestjs/swagger";
import { ParkStatus } from "../../common/types/status.type";
import { CrowdLevel } from "../../common/types/crowd-level.type";
import { ScheduleType } from "../entities/schedule-entry.entity";
import { InfluencingHoliday } from "./schedule-item.dto";

/**
 * Calendar Metadata
 */
export class CalendarMeta {
  @ApiProperty({ description: "Park slug" })
  slug: string;

  @ApiProperty({ description: "Park timezone (IANA format)" })
  timezone: string;
}

/**
 * Operating Hours
 */
export class OperatingHours {
  @ApiProperty()
  openingTime: string;

  @ApiProperty()
  closingTime: string;

  @ApiProperty()
  type: ScheduleType;

  @ApiProperty()
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

  @ApiProperty({ type: () => OperatingHours, required: false })
  hours?: OperatingHours;

  @ApiProperty()
  crowdLevel: CrowdLevel | "closed";

  @ApiProperty({ type: () => WeatherSummary, required: false })
  weather?: WeatherSummary;

  @ApiProperty({ type: () => [CalendarEvent], required: false })
  events?: CalendarEvent[];

  @ApiProperty()
  isHoliday: boolean;

  @ApiProperty()
  isBridgeDay: boolean;

  @ApiProperty()
  isSchoolVacation: boolean;

  @ApiProperty({
    description: "Holidays from neighbor regions that might influence crowds",
    type: [InfluencingHoliday],
    required: false,
  })
  influencingHolidays?: InfluencingHoliday[];

  @ApiProperty({
    description:
      "Visit recommendation based on crowd level, weather, and holiday context",
    required: false,
    enum: [
      "highly_recommended",
      "recommended",
      "neutral",
      "avoid",
      "strongly_avoid",
      "closed",
    ],
  })
  recommendation?:
    | "highly_recommended"
    | "recommended"
    | "neutral"
    | "avoid"
    | "strongly_avoid"
    | "closed";

  @ApiProperty({ type: () => [HourlyPrediction], required: false })
  hourly?: HourlyPrediction[];
}

/**
 * Integrated Calendar Response
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
