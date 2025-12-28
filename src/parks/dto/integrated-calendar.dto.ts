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
 * Currently only supports holidays from Nager.Date API
 */
export class CalendarEvent {
  @ApiProperty({ description: "Event name", example: "Christmas Day" })
  name: string;

  @ApiProperty({
    description: "Event type - currently only 'holiday' is supported",
    example: "holiday",
  })
  type: "holiday";

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
