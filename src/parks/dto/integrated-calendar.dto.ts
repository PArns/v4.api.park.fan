import { ApiProperty } from "@nestjs/swagger";
import { ParkStatus } from "../../common/types/status.type";
import { CrowdLevel } from "../../common/types/crowd-level.type";
import { ScheduleType } from "../entities/schedule-entry.entity";

/**
 * Calendar Metadata
 */
export class CalendarMeta {
  @ApiProperty({ description: "Park ID (UUID)" })
  parkId: string;

  @ApiProperty({ description: "Park slug" })
  slug: string;

  @ApiProperty({ description: "Park timezone (IANA format)" })
  timezone: string;

  @ApiProperty({
    description: "Timestamp when response was generated (ISO 8601)",
  })
  generatedAt: string;

  @ApiProperty({ description: "Requested date range" })
  requestRange: { from: string; to: string };
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
 * Supports holidays and special park events like parades, fireworks, seasonal events
 */
export class CalendarEvent {
  @ApiProperty({ description: "Event name", example: "Christmas Parade" })
  name: string;

  @ApiProperty({
    description: "Event type",
    enum: [
      "holiday",
      "parade",
      "firework",
      "character_meet",
      "seasonal_event",
      "special_show",
      "festival",
    ],
    example: "parade",
  })
  type:
    | "holiday"
    | "parade"
    | "firework"
    | "character_meet"
    | "seasonal_event"
    | "special_show"
    | "festival";

  @ApiProperty({
    description: "Event start time (HH:mm format), optional for all-day events",
    required: false,
    example: "15:00",
  })
  startTime?: string;

  @ApiProperty({
    description: "Event end time (HH:mm format)",
    required: false,
    example: "15:30",
  })
  endTime?: string;

  @ApiProperty({
    description: "Event location within the park",
    required: false,
    example: "Main Street",
  })
  location?: string;

  @ApiProperty({
    description: "Whether this is a nationwide holiday (only for type=holiday)",
    required: false,
  })
  isNationwide?: boolean;

  @ApiProperty({
    description: "Additional description or details",
    required: false,
    example: "Spectacular Christmas parade with Disney characters",
  })
  description?: string;
}

/**
 * Show Time
 */
export class ShowTime {
  @ApiProperty()
  name: string;

  @ApiProperty()
  time: string;
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
}

/**
 * Calendar Day - unified structure
 */
export class CalendarDay {
  @ApiProperty()
  date: string;

  @ApiProperty()
  status: ParkStatus;

  @ApiProperty()
  isToday: boolean;

  @ApiProperty()
  isTomorrow: boolean;

  @ApiProperty({ type: () => OperatingHours, required: false })
  hours?: OperatingHours;

  @ApiProperty()
  crowdLevel: CrowdLevel;

  @ApiProperty({ required: false })
  crowdScore?: number;

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

  @ApiProperty({ type: () => [HourlyPrediction], required: false })
  hourly?: HourlyPrediction[];

  @ApiProperty({ type: [String], required: false })
  refurbishments?: string[];

  @ApiProperty({ required: false })
  recommendation?: string;

  @ApiProperty({ type: () => [ShowTime], required: false })
  showTimes?: ShowTime[];
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
