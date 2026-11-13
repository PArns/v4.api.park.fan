import { ApiPropertyOptional, ApiProperty } from "@nestjs/swagger";
import {
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Matches,
  Max,
  MaxLength,
  Min,
} from "class-validator";
import { HOTEL_KINDS } from "../regions";

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const DATE_MESSAGE = "must be a YYYY-MM-DD date";
/** One owned ticket: `product[.days][.h][@YYYY-MM-DD]`. */
const TICKET_ITEM =
  "[a-z0-9-]+(\\.[0-9]{1,2})?(\\.h\\??)?(@\\d{4}-\\d{2}-\\d{2})?";
/** `tickets=`: one to six of them, comma-separated. */
export const TICKETS_PATTERN = new RegExp(
  `^${TICKET_ITEM}(,${TICKET_ITEM}){0,5}$`,
);

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

/** `GET /v1/trip-regions/:region/best-days` */
export class TripBestDaysQueryDto extends TripCalendarQueryDto {
  @ApiPropertyOptional({
    description: "Quietest days per park (1–31).",
    default: 5,
  })
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(31)
  limit?: number;
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
    default: "1",
    description: "Keep each resort's days in one block.",
  })
  @IsOptional()
  @IsIn(["0", "1", "true", "false"])
  blocks?: string;

  @ApiPropertyOptional({
    enum: ["want", "any"],
    default: "any",
    description:
      "`want`: a night the day guests lose the evening show to a party is used only when " +
      "nothing else fits. `any`: a day is priced by the hours a day guest gets.",
  })
  @IsOptional()
  @IsIn(["want", "any"])
  fireworks?: string;

  @ApiPropertyOptional({
    enum: ["auto", "fixed"],
    default: "auto",
    description:
      "`auto`: parks listed without a count may get a second visit where the trip has " +
      "park days to spare after its rest days. Explicit `slug:n` counts never change.",
  })
  @IsOptional()
  @IsIn(["auto", "fixed"])
  split?: string;

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
    description:
      "The tickets the visitor already has, comma-separated, at most 6: " +
      "`<product>[.<days>][.h|.h?][@<YYYY-MM-DD start>]`, the product an id of the region's " +
      "`tickets`; `.h?`: the hopper is not known. " +
      "The parks none of them covers are planned plan first, with a suggested ticket.",
    example: "wdw-uk-14-day-magic@2027-02-01,uor-date-based.3.h",
  })
  @IsOptional()
  @IsString()
  @MaxLength(400)
  @Matches(TICKETS_PATTERN, {
    message:
      "tickets must be product[.days][.h|.h?][@YYYY-MM-DD], comma-separated, at most 6",
  })
  tickets?: string;

  @ApiPropertyOptional({
    description:
      "Plan first: the resorts (group keys) planned with the hopper option. `ticket=hopper` means every resort.",
    example: "walt-disney-world,universal-orlando",
  })
  @IsOptional()
  @IsString()
  @MaxLength(200)
  @Matches(/^[a-z0-9-]+(,[a-z0-9-]+){0,7}$/, {
    message: "hopper must be resort keys, comma-separated",
  })
  hopper?: string;

  @ApiPropertyOptional({
    enum: ["base", "hopper"],
    default: "base",
    description:
      "Plan first: the ticket for the parks no owned ticket covers. `hopper`: Disney's Park " +
      "Hopper or Universal's park-to-park (Universal Studios and Islands of Adventure, never " +
      "Epic Universe); it allows `hop=1` and a second park in the evening.",
  })
  @IsOptional()
  @IsIn(["base", "hopper"])
  ticket?: string;

  @ApiPropertyOptional({
    enum: ["0", "1"],
    default: "0",
    description:
      "Allow two parks of one resort on a day when the trip or a ticket has fewer days than " +
      "visits. The day's ticket must hop between them (`ticket=hopper`, or an owned ticket with " +
      "hopping); otherwise ignored with the warning `hop_needs_hopper`.",
  })
  @IsOptional()
  @IsIn(["0", "1", "true", "false"])
  hop?: string;

  @ApiPropertyOptional({
    enum: ["0", "1"],
    default: "1",
    description:
      "Suggest an evening on park days that end early: a hop park with `ticket=hopper`, " +
      "else one of the resort's free venues.",
  })
  @IsOptional()
  @IsIn(["0", "1", "true", "false"])
  evenings?: string;

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
