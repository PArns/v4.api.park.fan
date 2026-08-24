import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import { Type } from "class-transformer";
import {
  IsArray,
  IsBoolean,
  IsIn,
  IsInt,
  IsObject,
  IsOptional,
  IsString,
  Matches,
  Max,
  MaxLength,
  Min,
} from "class-validator";
import {
  PARK_SEASON_KINDS,
  PARK_SEASON_STATUSES,
  type ParkSeasonKind,
  type ParkSeasonStatus,
} from "../../../parks/entities/park-season.entity";

export class CurationPatchDto {
  @ApiProperty({
    description:
      "Curated fields to write, keyed by the descriptor's `key`. `null` clears " +
      "a correction and accepts upstream again; omitting a key leaves it alone.",
    example: { curatedName: "TARON", curatedSeasonMonths: [4, 5, 6, 7, 8, 9] },
    type: "object",
    additionalProperties: true,
  })
  @IsObject()
  fields: Record<string, unknown>;

  @ApiPropertyOptional({
    description:
      "Why. Goes into the audit row and is what makes the change reviewable.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(2000)
  reason?: string;

  @ApiPropertyOptional({
    description:
      "The page this was established from. A correction without a source is a rumour.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(2000)
  sourceUrl?: string;
}

export class SeasonWriteDto {
  @ApiProperty({ enum: PARK_SEASON_KINDS })
  @IsIn(PARK_SEASON_KINDS as unknown as string[])
  kind: ParkSeasonKind;

  @ApiPropertyOptional({
    description:
      "The event's own name, untranslated — 'Halloween Horror Nights', " +
      "'Traumatica'. Null falls back to the frontend's translated label for the kind.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(200)
  name?: string | null;

  @ApiProperty({ example: "2026-10-03" })
  @IsString()
  @Matches(/^\d{4}-\d{2}-\d{2}$/, { message: "startDate must be YYYY-MM-DD" })
  startDate: string;

  @ApiProperty({ example: "2026-11-01" })
  @IsString()
  @Matches(/^\d{4}-\d{2}-\d{2}$/, { message: "endDate must be YYYY-MM-DD" })
  endDate: string;

  @ApiPropertyOptional({
    description:
      "The individual days inside the range the season actually runs. Omit for " +
      "'every day between start and end'. Walibi Holland's Fright Nights are " +
      "the case this exists for: weekends plus three single dates.",
    example: ["2026-10-03", "2026-10-04", "2026-10-10"],
    type: [String],
  })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  dates?: string[] | null;

  @ApiPropertyOptional({ enum: PARK_SEASON_STATUSES, default: "announced" })
  @IsOptional()
  @IsIn(PARK_SEASON_STATUSES as unknown as string[])
  status?: ParkSeasonStatus;

  @ApiPropertyOptional({ description: "Needs more than a normal day ticket." })
  @IsOptional()
  @IsBoolean()
  separateTicket?: boolean;

  @ApiPropertyOptional({ example: 53 })
  @IsOptional()
  @Type(() => Number)
  @Min(0)
  priceFrom?: number | null;

  @ApiPropertyOptional({ example: "EUR" })
  @IsOptional()
  @IsString()
  @MaxLength(3)
  priceCurrency?: string | null;

  @ApiPropertyOptional({ example: "19:00", description: "Park-local HH:MM." })
  @IsOptional()
  @IsString()
  @MaxLength(5)
  opensAt?: string | null;

  @ApiPropertyOptional({ example: "01:00" })
  @IsOptional()
  @IsString()
  @MaxLength(5)
  closesAt?: string | null;

  @ApiPropertyOptional({
    description:
      "Attractions this season is about — a maintenance window, a re-themed ride.",
    type: [String],
  })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  attractionIds?: string[] | null;

  @ApiPropertyOptional({ description: "The park's own page for the event." })
  @IsOptional()
  @IsString()
  @MaxLength(2000)
  url?: string | null;

  @ApiPropertyOptional({ description: "Where the dates were read." })
  @IsOptional()
  @IsString()
  @MaxLength(2000)
  sourceUrl?: string | null;

  @ApiPropertyOptional({
    description:
      "When this was last checked against the source. Not the same as " +
      "updatedAt — a typo fix does not mean anybody re-read the announcement.",
  })
  @IsOptional()
  @IsString()
  confirmedAt?: string | null;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MaxLength(4000)
  note?: string | null;
}

export class SeasonPatchDto {
  @ApiPropertyOptional({ enum: PARK_SEASON_KINDS })
  @IsOptional()
  @IsIn(PARK_SEASON_KINDS as unknown as string[])
  kind?: ParkSeasonKind;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MaxLength(200)
  name?: string | null;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @Matches(/^\d{4}-\d{2}-\d{2}$/, { message: "startDate must be YYYY-MM-DD" })
  startDate?: string;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @Matches(/^\d{4}-\d{2}-\d{2}$/, { message: "endDate must be YYYY-MM-DD" })
  endDate?: string;

  @ApiPropertyOptional({ type: [String] })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  dates?: string[] | null;

  @ApiPropertyOptional({ enum: PARK_SEASON_STATUSES })
  @IsOptional()
  @IsIn(PARK_SEASON_STATUSES as unknown as string[])
  status?: ParkSeasonStatus;

  @ApiPropertyOptional()
  @IsOptional()
  @IsBoolean()
  separateTicket?: boolean;

  @ApiPropertyOptional()
  @IsOptional()
  @Type(() => Number)
  @Min(0)
  priceFrom?: number | null;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MaxLength(3)
  priceCurrency?: string | null;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MaxLength(5)
  opensAt?: string | null;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MaxLength(5)
  closesAt?: string | null;

  @ApiPropertyOptional({ type: [String] })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  attractionIds?: string[] | null;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MaxLength(2000)
  url?: string | null;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MaxLength(2000)
  sourceUrl?: string | null;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  confirmedAt?: string | null;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MaxLength(4000)
  note?: string | null;
}

export class RideProfileWriteDto {
  @ApiPropertyOptional({
    description:
      "Glossary term ids of the track elements, IN RIDE ORDER. Order is " +
      "meaningful and repeats are intentional — this is the layout walkthrough.",
    type: [String],
  })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  elements?: string[];

  @ApiPropertyOptional({
    description:
      "Glossary term ids describing what kind of ride this is. Unordered set.",
    type: [String],
  })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  types?: string[];

  @ApiPropertyOptional({
    description: 'Display name, e.g. "Bolliger & Mabillard".',
  })
  @IsOptional()
  @IsString()
  @MaxLength(200)
  manufacturerName?: string | null;

  @ApiPropertyOptional({
    description:
      "Glossary term id of the manufacturer; null renders the name as plain text.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(200)
  manufacturerTermId?: string | null;

  @ApiPropertyOptional({ example: "Blitz Coaster" })
  @IsOptional()
  @IsString()
  @MaxLength(200)
  model?: string | null;

  @ApiPropertyOptional({ example: 2016 })
  @IsOptional()
  @IsInt()
  @Min(1800)
  @Max(2100)
  openedYear?: number | null;

  @ApiPropertyOptional({
    description:
      "As the manufacturer publishes it. Kept separate from `elements` on " +
      "purpose — the two legitimately disagree.",
  })
  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(20)
  inversions?: number | null;

  @ApiPropertyOptional({
    description: "Hand-written measurements. Each wins over Wikidata's.",
  })
  @IsOptional()
  @IsObject()
  curatedStats?: {
    topSpeedKmh?: number | null;
    heightM?: number | null;
    lengthM?: number | null;
    durationSeconds?: number | null;
  } | null;
}
