import { ApiProperty } from "@nestjs/swagger";

export class MonthStatDto {
  @ApiProperty({
    example: 7,
    description: "Month number (1=January, 12=December)",
  })
  month: number;

  @ApiProperty({ example: 3.2, description: "Average crowd score (1.0–5.0)" })
  avgCrowdScore: number;

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
