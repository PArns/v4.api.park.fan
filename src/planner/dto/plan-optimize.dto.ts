import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import { Type } from "class-transformer";
import {
  ArrayMaxSize,
  ArrayUnique,
  IsArray,
  IsBoolean,
  IsIn,
  IsInt,
  IsNotEmpty,
  IsNumber,
  IsOptional,
  IsString,
  Matches,
  Max,
  MaxLength,
  Min,
  ValidateNested,
} from "class-validator";
import { PLANNER_BLOCK_ICONS } from "../day-optimizer/types";

/**
 * The body and answer of `POST …/plan/optimize`. The planner's own shapes, as park.fan stores a day
 * in the browser; the contract is docs/frontend/plan-day/optimize.md.
 */

/** Entries in one request: a planned day of quarter hours from opening to past midnight. */
export const MAX_OPTIMIZE_ENTRIES = 80;

/** Rides a `fit` request may add on top of the entries. */
export const MAX_OPTIMIZE_ADDITIONS = 40;

/** Keys or ids in one list of a fit choice. */
const MAX_CHOICE_ITEMS = 120;

/** The latest minute an entry may carry: the end of the next day. */
const MAX_ENTRY_MINUTE = 48 * 60;

export const OPTIMIZE_MODES = ["day", "headliners", "fit"] as const;

export class PlannerCustomBlockDto {
  @ApiProperty({ example: "Lunch", maxLength: 200 })
  @IsString()
  @MaxLength(200)
  label: string;

  @ApiProperty({ enum: PLANNER_BLOCK_ICONS, example: "food" })
  @IsIn(PLANNER_BLOCK_ICONS)
  icon: (typeof PLANNER_BLOCK_ICONS)[number];

  @ApiProperty({ example: 60, minimum: 0, maximum: 1440 })
  @IsInt()
  @Min(0)
  @Max(1440)
  durationMinutes: number;
}

export class PlannerEntryDto {
  @ApiProperty({
    example: "taron-1",
    description: "Unique within the request; the answer names entries by it.",
  })
  @IsString()
  @IsNotEmpty()
  @MaxLength(100)
  id: string;

  @ApiPropertyOptional({
    example: "taron",
    description: "The ride; absent on a free block and on a show.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(200)
  attractionSlug?: string;

  @ApiPropertyOptional({ example: "Taron" })
  @IsOptional()
  @IsString()
  @MaxLength(300)
  attractionName?: string;

  @ApiPropertyOptional({
    type: () => PlannerCustomBlockDto,
    description:
      "A free block (or, with `showSlug`, a show): a fixed duration the optimizer plans around.",
  })
  @IsOptional()
  @ValidateNested()
  @Type(() => PlannerCustomBlockDto)
  custom?: PlannerCustomBlockDto;

  @ApiPropertyOptional({
    example: "wintertraum",
    description:
      "One performance of a show; always with `custom`. Never moved.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(200)
  showSlug?: string;

  @ApiProperty({
    example: 630,
    description:
      "Park-local minutes since midnight; past 1440 on a day that runs past midnight.",
  })
  @IsInt()
  @Min(0)
  @Max(MAX_ENTRY_MINUTE)
  startMinute: number;

  @ApiPropertyOptional({
    deprecated: true,
    description:
      "The browser's write-only mirror of `startMinute / 60`. Accepted and ignored.",
  })
  @IsOptional()
  @IsInt()
  hour?: number;

  @ApiPropertyOptional({
    description: "Ticked off: a record, never moved.",
  })
  @IsOptional()
  @IsBoolean()
  done?: boolean;

  @ApiPropertyOptional({
    description: "Minutes actually queued, recorded when ticked off.",
  })
  @IsOptional()
  @IsNumber({ allowNaN: false, allowInfinity: false })
  @Min(0)
  @Max(1440)
  actualWait?: number;
}

export class PlannerDayPrefsDto {
  @ApiPropertyOptional({
    example: 110,
    description:
      "The shortest rider in cm. Only `headliners` reads it: a headliner this party cannot ride is not added.",
  })
  @IsOptional()
  @IsNumber({ allowNaN: false, allowInfinity: false })
  @Min(0)
  @Max(300)
  riderHeightCm?: number;

  @ApiPropertyOptional({
    description:
      "The party would rather stay dry; `headliners` leaves water rides out.",
  })
  @IsOptional()
  @IsBoolean()
  avoidWet?: boolean;

  @ApiPropertyOptional({
    description:
      "The visitor holds early entry on this day; headliners may then be planned before opening where the park offers it.",
  })
  @IsOptional()
  @IsBoolean()
  earlyEntry?: boolean;
}

export class FitChoiceDto {
  @ApiPropertyOptional({
    type: [String],
    description: "Wish keys switched off (`e:<entry id>`, `a:<slug>`).",
  })
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(MAX_CHOICE_ITEMS)
  @IsString({ each: true })
  @MaxLength(220, { each: true })
  dropped?: string[];

  @ApiPropertyOptional({
    type: [String],
    description: "Free blocks (entry ids) taken out of the day.",
  })
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(MAX_CHOICE_ITEMS)
  @IsString({ each: true })
  @MaxLength(100, { each: true })
  droppedBlocks?: string[];

  @ApiPropertyOptional({
    type: [String],
    description: "Free blocks (entry ids) cut to 30 minutes.",
  })
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(MAX_CHOICE_ITEMS)
  @IsString({ each: true })
  @MaxLength(100, { each: true })
  shortBlocks?: string[];

  @ApiPropertyOptional({
    type: [String],
    description:
      "Wish keys, most important first. Decides which wish falls out, never the order of the day.",
  })
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(MAX_CHOICE_ITEMS)
  @IsString({ each: true })
  @MaxLength(220, { each: true })
  priority?: string[];
}

export class PlanOptimizeRequestDto {
  @ApiProperty({ example: "2026-11-14", description: "Park-local date." })
  @IsString()
  @Matches(/^\d{4}-\d{2}-\d{2}$/)
  date: string;

  @ApiProperty({
    enum: OPTIMIZE_MODES,
    description:
      "`day` re-orders the rides the day holds. `headliners` adds the park's headliners this party can ride and plans everything. `fit` answers the fit assistant for one choice.",
  })
  @IsIn(OPTIMIZE_MODES)
  mode: (typeof OPTIMIZE_MODES)[number];

  @ApiProperty({
    type: () => [PlannerEntryDto],
    maxItems: MAX_OPTIMIZE_ENTRIES,
    description:
      "The day as it stands, free blocks, shows and ticked-off rides included.",
  })
  @IsArray()
  @ArrayMaxSize(MAX_OPTIMIZE_ENTRIES)
  @ValidateNested({ each: true })
  @Type(() => PlannerEntryDto)
  entries: PlannerEntryDto[];

  @ApiPropertyOptional({ type: () => PlannerDayPrefsDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => PlannerDayPrefsDto)
  prefs?: PlannerDayPrefsDto;

  @ApiPropertyOptional({
    example: 840,
    minimum: 0,
    maximum: 1439,
    description:
      "Park-local minute of the day the caller reads as now. Used only when `date` is the park's today; the server's own park-local minute otherwise.",
  })
  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(1439)
  nowMinute?: number;

  @ApiPropertyOptional({
    type: [String],
    maxItems: MAX_OPTIMIZE_ADDITIONS,
    description:
      "`fit` only: slugs of rides being added on top of the entries (the headliners a press would add, or the one ride an add-to-plan press asked for).",
  })
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(MAX_OPTIMIZE_ADDITIONS)
  @ArrayUnique()
  @IsString({ each: true })
  @MaxLength(200, { each: true })
  add?: string[];

  @ApiPropertyOptional({
    type: () => FitChoiceDto,
    description:
      "`fit` only: the visitor's answers. Absent is everything ticked, nothing pinned.",
  })
  @IsOptional()
  @ValidateNested()
  @Type(() => FitChoiceDto)
  choice?: FitChoiceDto;

  @ApiPropertyOptional({
    description:
      "`fit` only: also measure what taking out or shortening each free block would buy. One more search per block.",
  })
  @IsOptional()
  @IsBoolean()
  levers?: boolean;

  @ApiPropertyOptional({
    description:
      "`day` and `headliners` only: plan without first asking whether the day holds everything, for a button that shows what a press would gain.",
  })
  @IsOptional()
  @IsBoolean()
  preview?: boolean;
}

export class DayClockDto {
  @ApiProperty({ enum: ["past", "today", "future"] })
  phase: "past" | "today" | "future";

  @ApiPropertyOptional({ description: "Park-local minute; `today` only." })
  nowMinute?: number;
}

export class OptimizeStopDto {
  @ApiProperty({
    nullable: true,
    type: String,
    description: "The entry this stop moves, or null for a ride being added.",
  })
  entryId: string | null;

  @ApiProperty()
  attractionSlug: string;

  @ApiProperty()
  attractionName: string;

  @ApiProperty({ description: "Park-local minute the queue is joined." })
  startMinute: number;

  @ApiProperty({
    nullable: true,
    type: Number,
    description:
      "The wait the schedule was built on; null where the day has no figure.",
  })
  waitMinutes: number | null;

  @ApiProperty({
    description: "Whether the queue is joined before the park closes.",
  })
  fits: boolean;
}

export class PlanScoreDto {
  @ApiProperty({ description: "Minutes queued across the stops that happen." })
  totalWaitMinutes: number;

  @ApiProperty({
    description: "Minutes standing about that the plan decided on.",
  })
  idleMinutes: number;

  @ApiProperty({ description: "When the last queue is left, ride included." })
  endMinute: number;

  @ApiProperty({ description: "Stops whose queue is joined after closing." })
  overflow: number;

  @ApiProperty({
    description:
      "Times the next thing starts before the one before it is over and walked from.",
  })
  clashes: number;
}

export class OptimizedPlanDto {
  @ApiProperty({ type: () => [OptimizeStopDto] })
  stops: OptimizeStopDto[];

  @ApiProperty()
  totalWaitMinutes: number;

  @ApiProperty()
  idleMinutes: number;

  @ApiProperty()
  endMinute: number;

  @ApiProperty()
  overflow: number;

  @ApiProperty({
    description: "Rides left out because the search holds at most `maxStops`.",
  })
  capped: number;
}

export class PlannedEntryDto {
  @ApiProperty({
    nullable: true,
    type: String,
    description:
      "The entry's id; null on a ride the plan adds, which the caller gives an id.",
  })
  id: string | null;

  @ApiPropertyOptional()
  attractionSlug?: string;

  @ApiPropertyOptional()
  attractionName?: string;

  @ApiPropertyOptional({ type: () => PlannerCustomBlockDto })
  custom?: PlannerCustomBlockDto;

  @ApiPropertyOptional()
  showSlug?: string;

  @ApiProperty()
  startMinute: number;

  @ApiPropertyOptional()
  done?: boolean;

  @ApiPropertyOptional()
  actualWait?: number;

  @ApiPropertyOptional({ description: "Set on a ride the plan adds." })
  added?: boolean;
}

export class FitWishDto {
  @ApiProperty({
    example: "a:taron",
    description: "`e:<entry id>` or `a:<slug>`.",
  })
  key: string;

  @ApiProperty({ nullable: true, type: String })
  entryId: string | null;

  @ApiProperty()
  attractionSlug: string;

  @ApiProperty()
  attractionName: string;

  @ApiProperty()
  headliner: boolean;

  @ApiProperty({
    description:
      "Whether the day has a curve for it; one without keeps its minute.",
  })
  replannable: boolean;
}

export class FitBlockDto {
  @ApiProperty()
  entryId: string;

  @ApiProperty()
  label: string;

  @ApiProperty({ enum: PLANNER_BLOCK_ICONS })
  icon: string;

  @ApiProperty()
  durationMinutes: number;
}

export class FitLeverDto {
  @ApiProperty({ enum: ["drop-block", "shorten-block", "drop-all-blocks"] })
  kind: "drop-block" | "shorten-block" | "drop-all-blocks";

  @ApiPropertyOptional()
  entryId?: string;

  @ApiPropertyOptional()
  label?: string;

  @ApiPropertyOptional()
  icon?: string;

  @ApiPropertyOptional({
    description: "What the block would be cut to (`shorten-block`).",
  })
  minutes?: number;

  @ApiProperty({ description: "Wishes that fit once it is pulled." })
  fits: number;

  @ApiProperty({ description: "Wishes that fit as things stand." })
  fitsNow: number;

  @ApiProperty({ description: "Wishes asked for." })
  wanted: number;

  @ApiProperty({ description: "Pulling this one alone makes every wish fit." })
  solves: boolean;
}

export class FitResultDto {
  @ApiProperty({ type: () => [FitWishDto] })
  wishes: FitWishDto[];

  @ApiProperty({ type: () => [FitBlockDto] })
  blocks: FitBlockDto[];

  @ApiProperty({
    type: [String],
    description:
      "Wish keys in the order the day is decided in: pinned first, laps last.",
  })
  order: string[];

  @ApiProperty({ description: "Wishes still ticked." })
  wanted: number;

  @ApiProperty({
    type: [String],
    description: "Wish keys with a slot before closing.",
  })
  fitted: string[];

  @ApiProperty({
    type: [String],
    description: "Ticked wishes with no slot before closing.",
  })
  missed: string[];

  @ApiProperty()
  endMinute: number;

  @ApiProperty()
  totalWaitMinutes: number;

  @ApiProperty()
  idleMinutes: number;

  @ApiProperty({
    type: () => [PlannerEntryDto],
    description:
      "The entries the day keeps under this choice; the stops are laid over them.",
  })
  entries: PlannerEntryDto[];

  @ApiProperty({ type: () => [OptimizeStopDto] })
  stops: OptimizeStopDto[];

  @ApiProperty({ type: () => [FitLeverDto], nullable: true })
  levers: FitLeverDto[] | null;

  @ApiProperty({
    type: [String],
    nullable: true,
    description: "Levers already pulled in this choice, as `kind:entryId`.",
  })
  applied: string[] | null;
}

export class PlanOptimizeResponseDto {
  @ApiProperty({ example: "2026-11-14" })
  date: string;

  @ApiProperty({ enum: OPTIMIZE_MODES })
  mode: (typeof OPTIMIZE_MODES)[number];

  @ApiProperty({
    enum: ["optimized", "unchanged", "needs-fit", "fit", "refused"],
    description:
      "`optimized`: `plan` beats the day. `unchanged`: the day is as good as it gets. `needs-fit`: something asked for has no slot before closing; `fit` holds the assistant's opening answer. `fit`: the answer to a `fit` request. `refused`: see `refusal`.",
  })
  status: "optimized" | "unchanged" | "needs-fit" | "fit" | "refused";

  @ApiProperty({
    enum: [
      "past-day",
      "no-plan",
      "no-hours",
      "no-wait-times",
      "no-rides",
      "unknown-ride",
    ],
    nullable: true,
  })
  refusal: string | null;

  @ApiProperty({ type: () => DayClockDto })
  clock: DayClockDto;

  @ApiProperty({ example: 24, description: "Rides one search holds at most." })
  maxStops: number;

  @ApiProperty({
    type: [String],
    description: "Slugs of the rides offered on top of the entries.",
  })
  add: string[];

  @ApiProperty({
    description:
      "Headliners left out because this party cannot ride them (`headliners`).",
  })
  skippedHeadliners: number;

  @ApiProperty({ description: "Entries the optimizer may move." })
  movable: number;

  @ApiProperty({ type: () => PlanScoreDto, nullable: true })
  before: PlanScoreDto | null;

  @ApiProperty({ type: () => PlanScoreDto, nullable: true })
  after: PlanScoreDto | null;

  @ApiProperty({ type: () => OptimizedPlanDto, nullable: true })
  plan: OptimizedPlanDto | null;

  @ApiProperty({
    type: () => [PlannedEntryDto],
    nullable: true,
    description:
      "The day as the answer leaves it, in start order. Added rides have `id: null`.",
  })
  entries: PlannedEntryDto[] | null;

  @ApiProperty({ type: () => FitResultDto, nullable: true })
  fit: FitResultDto | null;
}
