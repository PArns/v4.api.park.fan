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

  @ApiProperty({
    description:
      "Total precipitation for the day, in mm (legacy name; NOT a percentage).",
  })
  rainChance: number;

  @ApiProperty({
    required: false,
    description: "Total precipitation for the day, in mm.",
  })
  precipitationMm?: number;

  @ApiProperty({
    required: false,
    description: "Total snowfall for the day, in cm.",
  })
  snowMm?: number;

  @ApiProperty({
    required: false,
    description: "Maximum wind speed for the day, in km/h.",
  })
  windMax?: number;

  @ApiProperty({
    required: false,
    description: "Relative humidity (%), when available (live/today).",
  })
  humidity?: number;

  @ApiProperty({
    required: false,
    description: "Apparent ('feels like') temperature, when available (today).",
  })
  apparentTemp?: number;
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
 * A single headliner ride's expected wait for a calendar day.
 */
export class HeadlinerWaitForecast {
  @ApiProperty({ description: "Attraction UUID" })
  attractionId: string;

  @ApiProperty({ description: "Attraction display name", example: "Taron" })
  name: string;

  @ApiProperty({
    description: "Expected (predicted) standby wait for this day, in minutes",
    example: 45,
  })
  waitTime: number;
}

/**
 * Headliner forecast for a calendar day.
 *
 * Grounds the abstract crowd level ("high", "extreme", …) in concrete numbers:
 * the top headliner rides and the wait a visitor should expect for THIS day at
 * THIS park. Present only on days that carry per-attraction ML predictions
 * (today + future); absent on completed/closed days.
 */
export class HeadlinerForecast {
  @ApiProperty({
    description:
      "Average wait across the park's headliners (minutes, rounded to 5).",
    example: 40,
  })
  avgWait: number;

  @ApiProperty({
    description:
      "Top headliner rides for this day, sorted by wait desc (minutes, rounded to 5).",
    type: () => [HeadlinerWaitForecast],
  })
  rides: HeadlinerWaitForecast[];

  @ApiProperty({
    required: false,
    description:
      "true = actual recorded averages for a PAST day; " +
      "false/absent = ML forecast for today/future.",
  })
  actual?: boolean;
}

/**
 * A neighbouring-region holiday that meets the calendar priority threshold.
 *
 * Distinct from the local `events`/holiday flags: these are holidays in a
 * NEIGHBOURING region whose day-trippers drive up local crowds. Pre-ranked and
 * capped so the default payload stays lean (the full, unranked list is still
 * available via `?include=influencingHolidays`).
 */
export class NeighborHoliday {
  @ApiProperty({ description: "English holiday name" })
  name: string;

  @ApiProperty({ description: "The neighbouring region this holiday is in" })
  source: {
    countryCode: string;
    regionCode?: string | null;
  };

  @ApiProperty({
    description: "Type of holiday",
    enum: ["public", "school", "bank"],
  })
  holidayType: string;

  @ApiProperty({
    description:
      "Influence rank: 1 = the nearest/most important influencing region. " +
      "Lower = stronger crowd impact. Only the top-ranked regions are surfaced.",
    example: 1,
  })
  priority: number;
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
      "The ML FORWARD prediction for this day (predicted headliner peak ÷ typical-day-peak). " +
      "Equals crowdLevel on future days; on TODAY it differs, because crowdLevel is overridden " +
      "with the live occupancy — so surfaces can show a true 'forecast today' vs the live 'now'.",
    required: false,
  })
  predictedCrowdLevel?: CrowdLevel;

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

  @ApiProperty({
    type: () => [InfluencingHoliday],
    required: false,
    description:
      "Neighbouring-region holidays influencing this day's crowds. OMITTED by " +
      "default (it was ~98% of the payload and no consumer reads it here) — " +
      "request it with `?include=influencingHolidays`.",
  })
  influencingHolidays?: InfluencingHoliday[];

  @ApiProperty({
    type: () => HeadlinerForecast,
    required: false,
    description:
      "Expected headliner waits for this day (avg + top rides). Grounds the " +
      "crowd level in concrete numbers. Present on days with ML predictions " +
      "(today + future), absent on completed/closed days.",
  })
  headlinerForecast?: HeadlinerForecast;

  @ApiProperty({
    type: () => [NeighborHoliday],
    required: false,
    description:
      "Priority-ranked, capped holidays in NEIGHBOURING regions (top influencing " +
      "regions only) whose day-trippers raise local crowds. Default-on and lean; " +
      "distinct from the local holiday flags. Sorted by priority (1 = strongest).",
  })
  neighborHolidays?: NeighborHoliday[];

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
