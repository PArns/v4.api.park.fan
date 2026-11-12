import { ApiPropertyOptional, ApiProperty } from "@nestjs/swagger";
import {
  IsIn,
  IsOptional,
  IsString,
  Matches,
  MaxLength,
} from "class-validator";
import { HOTEL_KINDS } from "../regions";

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const DATE_MESSAGE = "must be a YYYY-MM-DD date";

/** `GET /v1/trip-regions/:region/calendar` */
export class TripCalendarQueryDto {
  @ApiPropertyOptional({
    description: "First day (region-local). Defaults to the region's today.",
    example: "2026-11-28",
  })
  @IsOptional()
  @Matches(DATE, { message: `from ${DATE_MESSAGE}` })
  from?: string;

  @ApiPropertyOptional({
    description:
      "Last day, included. Defaults to from + 30. At most 92 days, and no later than a year from today.",
    example: "2026-12-06",
  })
  @IsOptional()
  @Matches(DATE, { message: `to ${DATE_MESSAGE}` })
  to?: string;
}

/** `GET /v1/trip-regions/:region/plan` */
export class TripPlanQueryDto {
  @ApiProperty({
    description: "Arrival day (region-local).",
    example: "2026-11-28",
  })
  @Matches(DATE, { message: `from ${DATE_MESSAGE}` })
  from: string;

  @ApiProperty({
    description: "Departure day, included. The trip runs at most 21 days.",
    example: "2026-12-06",
  })
  @Matches(DATE, { message: `to ${DATE_MESSAGE}` })
  to: string;

  @ApiPropertyOptional({
    description:
      "Comma-separated park slugs, each with `:n` visits (1–3, default 1). Defaults to the region's headliner parks.",
    example: "magic-kingdom-park:2,epcot,universal-epic-universe",
  })
  @IsOptional()
  @IsString()
  @MaxLength(1000)
  @Matches(/^[a-z0-9-]+(:[1-9])?(,[a-z0-9-]+(:[1-9])?)*$/, {
    message: "parks must be slug or slug:n, comma-separated",
  })
  parks?: string;

  @ApiPropertyOptional({
    enum: HOTEL_KINDS,
    description:
      "Which hotel perks count. Defaults to the hotel the `area` usually means, else `offsite`.",
  })
  @IsOptional()
  @IsIn(HOTEL_KINDS)
  hotel?: string;

  @ApiPropertyOptional({
    description:
      "A `hotelAreas` key: travel to each day's park is read from there.",
    example: "disney",
  })
  @IsOptional()
  @Matches(/^[a-z_]+$/, { message: "area must be a hotel area key" })
  area?: string;

  @ApiPropertyOptional({ enum: ["free", "park"], default: "free" })
  @IsOptional()
  @IsIn(["free", "park"])
  arrival?: string;

  @ApiPropertyOptional({ enum: ["free", "park"], default: "free" })
  @IsOptional()
  @IsIn(["free", "park"])
  departure?: string;

  @ApiPropertyOptional({
    enum: ["0", "1"],
    default: "0",
    description: "Keep each resort's days in one block.",
  })
  @IsOptional()
  @IsIn(["0", "1", "true", "false"])
  blocks?: string;

  @ApiPropertyOptional({ enum: ["avoid", "allow", "prefer"], default: "avoid" })
  @IsOptional()
  @IsIn(["avoid", "allow", "prefer"])
  parties?: string;

  @ApiPropertyOptional({
    description:
      "`auto` (default), `none`, or `max:n` for at most n park days in a row (1–9).",
    example: "auto",
  })
  @IsOptional()
  @Matches(/^(auto|none|max:[1-9])$/, {
    message: "rest must be auto, none or max:n",
  })
  rest?: string;

  @ApiPropertyOptional({
    enum: ["0", "1"],
    default: "0",
    description:
      "Allow two parks of one resort on a day when the trip is shorter than its parks.",
  })
  @IsOptional()
  @IsIn(["0", "1", "true", "false"])
  hop?: string;

  @ApiPropertyOptional({
    description:
      "Comma-separated pins: `slug@date` for a park, `rest@date` for a rest day.",
    example: "epcot@2026-12-01,rest@2026-12-02",
  })
  @IsOptional()
  @IsString()
  @MaxLength(1000)
  @Matches(/^[a-z0-9-]+@\d{4}-\d{2}-\d{2}(,[a-z0-9-]+@\d{4}-\d{2}-\d{2})*$/, {
    message: "lock must be slug@YYYY-MM-DD, comma-separated",
  })
  lock?: string;
}
