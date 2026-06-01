import { ApiProperty } from "@nestjs/swagger";
import { CrowdLevel } from "../../common/types/crowd-level.type";

const CROWD_LEVEL_ENUM = [
  "very_low",
  "low",
  "moderate",
  "high",
  "very_high",
  "extreme",
] as const;

export class MonthStatDto {
  @ApiProperty({
    example: 7,
    description: "Month number (1=January, 12=December)",
  })
  month: number;

  @ApiProperty({ example: 3.2, description: "Average crowd score (1.0–5.0)" })
  avgCrowdScore: number;

  @ApiProperty({
    enum: CROWD_LEVEL_ENUM,
    enumName: "CrowdLevel",
    example: "high",
    description:
      "Crowd level for this month, derived occupancy-relative (this period's " +
      "average peak wait ÷ the park's typical-day-peak baseline). Uses the same " +
      "6-tier thresholds as the live endpoint — render this, do not re-classify.",
  })
  avgCrowdLevel: CrowdLevel;

  @ApiProperty({
    example: 22,
    description: "Average P50 (median) wait time in minutes",
  })
  avgWaitP50: number;

  @ApiProperty({ example: 45, description: "Average P90 wait time in minutes" })
  avgWaitP90: number;

  @ApiProperty({
    example: 28,
    description: "Number of days with sufficient data",
  })
  sampleDays: number;
}

export class DayOfWeekStatDto {
  @ApiProperty({
    example: 6,
    description: "Day of week (0=Sunday, 6=Saturday)",
  })
  dayOfWeek: number;

  @ApiProperty({ example: 4.1, description: "Average crowd score (1.0–5.0)" })
  avgCrowdScore: number;

  @ApiProperty({
    enum: CROWD_LEVEL_ENUM,
    enumName: "CrowdLevel",
    example: "very_high",
    description:
      "Crowd level for this weekday, derived occupancy-relative (this period's " +
      "average peak wait ÷ the park's typical-day-peak baseline). Uses the same " +
      "6-tier thresholds as the live endpoint — render this, do not re-classify.",
  })
  avgCrowdLevel: CrowdLevel;

  @ApiProperty({
    example: 28,
    description: "Average P50 (median) wait time in minutes",
  })
  avgWaitP50: number;

  @ApiProperty({ example: 52, description: "Average P90 wait time in minutes" })
  avgWaitP90: number;

  @ApiProperty({
    example: 90,
    description: "Number of days with sufficient data",
  })
  sampleDays: number;
}

export class TopAttractionStatDto {
  @ApiProperty({ example: "blue-fire-megacoaster" })
  attractionSlug: string;

  @ApiProperty({ example: "Blue Fire Megacoaster" })
  attractionName: string;

  @ApiProperty({ example: 38, description: "Average P50 wait time in minutes" })
  avgWaitP50: number;

  @ApiProperty({ example: 68, description: "Average P90 wait time in minutes" })
  avgWaitP90: number;

  @ApiProperty({ example: 120, description: "Number of days with data" })
  sampleDays: number;

  @ApiProperty({
    example: 1,
    description: "1-based rank within the top-attractions list (by avg P90)",
  })
  rank: number;
}

export class ParkHistoricalStatsMetaDto {
  @ApiProperty({ example: "europa-park" })
  parkSlug: string;

  @ApiProperty({ example: "2023-01-01" })
  dataFrom: string;

  @ApiProperty({ example: "2025-12-31" })
  dataTo: string;

  @ApiProperty({ example: 310 })
  totalSampleDays: number;

  @ApiProperty({
    example: 2,
    description:
      "Size of the look-back window in years (echoes the `years` query " +
      "parameter). Use this for the subtitle instead of deriving it from " +
      "dataFrom/dataTo.",
  })
  windowYears: number;

  @ApiProperty({
    example: true,
    description:
      "True when the data basis is large enough to display (totalSampleDays " +
      ">= minSampleDays). Use this as the render gate instead of a hardcoded " +
      "client-side threshold.",
  })
  displayable: boolean;

  @ApiProperty({
    example: "2026-06-01T03:14:00.000Z",
    description: "When this aggregate was computed (ISO 8601 UTC).",
  })
  generatedAt: string;

  @ApiProperty({
    example: 2,
    description: "Response schema version, for smooth frontend migration.",
  })
  schemaVersion: number;
}

export class ParkHistoricalStatsDto {
  @ApiProperty({ type: [MonthStatDto] })
  byMonth: MonthStatDto[];

  @ApiProperty({ type: [DayOfWeekStatDto] })
  byDayOfWeek: DayOfWeekStatDto[];

  @ApiProperty({ type: [TopAttractionStatDto] })
  topAttractions: TopAttractionStatDto[];

  @ApiProperty({ type: ParkHistoricalStatsMetaDto })
  meta: ParkHistoricalStatsMetaDto;
}
