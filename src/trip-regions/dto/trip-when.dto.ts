import { ApiProperty, ApiPropertyOptional, OmitType } from "@nestjs/swagger";
import { IsIn, IsInt, IsOptional, Matches, Max, Min } from "class-validator";
import { TripPlanQueryDto } from "./trip-region-query.dto";
import {
  WHEN_DEFAULT_DAYS,
  WHEN_DEFAULT_GAP,
  WHEN_DEFAULT_LIMIT,
  WHEN_MAX_DAYS,
  WHEN_MAX_LIMIT,
  WHEN_MIN_DAYS,
} from "../trip-when";

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const DATE_MESSAGE = "must be a YYYY-MM-DD date";
/** A holiday-region code as the holiday table stores it: `DE-NW`, `AT-KÄ`, `GB-ENG`. */
const REGION_CODE = /^[A-Za-z]{2}(-[\p{L}\p{N}]{1,12}){1,2}$/u;

/**
 * `GET /v1/trip-regions/:region/when`: the plan's trip options (minus the
 * dates and pins, which differ per window), the search range and the window
 * length. Contract: docs/frontend/trip-regions-when.md.
 */
export class TripWhenQueryDto extends OmitType(TripPlanQueryDto, [
  "from",
  "to",
  "lock",
] as const) {
  @ApiPropertyOptional({
    description: "First day of the search range. Defaults to today.",
    example: "2027-01-01",
  })
  @IsOptional()
  @Matches(DATE, { message: `from ${DATE_MESSAGE}` })
  from?: string;

  @ApiPropertyOptional({
    description:
      "Last day of the search range, included. Defaults to from + 365, at most a year from today.",
    example: "2027-12-31",
  })
  @IsOptional()
  @Matches(DATE, { message: `to ${DATE_MESSAGE}` })
  to?: string;

  @ApiPropertyOptional({
    description: "Trip days, arrival and departure day included.",
    default: WHEN_DEFAULT_DAYS,
  })
  @IsOptional()
  @IsInt()
  @Min(WHEN_MIN_DAYS)
  @Max(WHEN_MAX_DAYS)
  days?: number;

  @ApiPropertyOptional({
    description:
      "A school-holiday region code from `/school-breaks` (`DE-NW`): only windows inside its breaks count.",
    example: "DE-NW",
  })
  @IsOptional()
  @Matches(REGION_CODE, { message: "within must be a code like DE-NW" })
  within?: string;

  @ApiPropertyOptional({
    description: "Windows returned.",
    default: WHEN_DEFAULT_LIMIT,
  })
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(WHEN_MAX_LIMIT)
  limit?: number;

  @ApiPropertyOptional({
    description: "Minimum days between two returned windows' start dates.",
    default: WHEN_DEFAULT_GAP,
  })
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(60)
  gap?: number;

  @ApiPropertyOptional({
    enum: ["0", "1"],
    default: "0",
    description:
      "Plan first: give each window a suggested ticket per resort (`ticketSuggestion`). " +
      "Off by default to keep the search fast.",
  })
  @IsOptional()
  @IsIn(["0", "1", "true", "false"])
  suggest?: string;

  @ApiPropertyOptional({
    enum: ["avoid", "allow", "prefer"],
    deprecated: true,
    description:
      "Old name of `fireworks`: `avoid` reads as `fireworks=want`, `allow` and `prefer` as `any`. `fireworks` wins when both are sent.",
  })
  @IsOptional()
  @IsIn(["avoid", "allow", "prefer"])
  parties?: string;
}

/** `GET /v1/trip-regions/:region/school-breaks` */
export class TripSchoolBreaksQueryDto {
  @ApiPropertyOptional({
    description: "First day. Defaults to today.",
    example: "2027-01-01",
  })
  @IsOptional()
  @Matches(DATE, { message: `from ${DATE_MESSAGE}` })
  from?: string;

  @ApiPropertyOptional({
    description:
      "Last day, included. Defaults to from + 365; at most 366 days.",
    example: "2027-12-31",
  })
  @IsOptional()
  @Matches(DATE, { message: `to ${DATE_MESSAGE}` })
  to?: string;

  @ApiPropertyOptional({
    description:
      "ISO country codes, comma-separated. Defaults to DE, NL, GB, BE, FR, AT, each when its stored school data reaches 180 days ahead.",
    example: "DE,NL,GB",
  })
  @IsOptional()
  @Matches(/^[A-Za-z]{2}(,[A-Za-z]{2}){0,19}$/, {
    message: "countries must be ISO codes, comma-separated",
  })
  countries?: string;
}

/** The `/when` answer, for Swagger; every field: docs/frontend/trip-regions-when.md. */
export class TripWhenDto {
  @ApiProperty({
    description:
      "region, from, to, days, computedAt, forecastUntil, within, search, climate",
    type: "object",
    additionalProperties: true,
  })
  meta: Record<string, unknown>;
  @ApiProperty({
    description:
      "The best windows: from, to, headlinerWait, rangeAverageWait, basis, reasons, warnings, seasons, closesEarlyDays, plan, tickets, ticketSuggestion",
    type: "array",
    items: { type: "object" },
  })
  windows: Array<Record<string, unknown>>;
  @ApiProperty({
    description: "Per ISO week: start, headlinerWait, basis",
    type: "array",
    items: { type: "object" },
  })
  weeks: Array<Record<string, unknown>>;
  @ApiProperty({
    description:
      "Per calendar month: month, headlinerWait, basis, climate, usSchoolBreakDays, seasons, partyNights, dayGuestHours, hurricaneSeason",
    type: "array",
    items: { type: "object" },
  })
  months: Array<Record<string, unknown>>;
  @ApiProperty({
    description: "Every season a window or a month names, as in /calendar",
    type: "array",
    items: { type: "object" },
  })
  seasons: Array<Record<string, unknown>>;
}

export class SchoolBreakPeriodDto {
  @ApiProperty({ example: "2027-07-17" }) from: string;
  @ApiProperty({ example: "2027-08-31" }) to: string;
  @ApiProperty({ example: "Summer Holidays" }) name: string;
}

export class SchoolBreakRegionDto {
  @ApiProperty({
    example: "DE-NW",
    description: "As the holiday table stores it; what `within` takes.",
  })
  code: string;
  @ApiProperty({ example: "Nordrhein-Westfalen" }) name: string;
  @ApiProperty({ example: "DE" }) country: string;
  @ApiProperty({
    nullable: true,
    description:
      "The last day the stored data reaches; later breaks are unknown.",
  })
  through: string | null;
  @ApiProperty({ type: () => [SchoolBreakPeriodDto] })
  periods: SchoolBreakPeriodDto[];
}

export class TripSchoolBreaksDto {
  @ApiProperty({ type: () => [SchoolBreakRegionDto] })
  regions: SchoolBreakRegionDto[];
}
