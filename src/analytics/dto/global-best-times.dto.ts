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

export class BestTimeBucketDto {
  @ApiProperty({
    example: 2,
    description:
      "Bucket key. For byDayOfWeek: 0=Sunday … 6=Saturday. For byMonth: 1=January … 12=December.",
  })
  key: number;

  @ApiProperty({
    example: 0.82,
    description:
      "Relative busyness index across all parks. 1.0 = each park's own typical day; " +
      "values below 1 are quieter than usual, above 1 busier. Computed by normalising " +
      "every park to its own average first, then averaging across parks — so large and " +
      "small parks count equally and absolute wait differences don't skew the ranking. " +
      "This is the honest 'when is it quietest' signal; rank on it.",
  })
  relativeIndex: number;

  @ApiProperty({
    enum: CROWD_LEVEL_ENUM,
    enumName: "CrowdLevel",
    example: "low",
    description:
      "Crowd level derived from relativeIndex (relative to the all-park average, not an " +
      "absolute per-park baseline). Render this for the colour; do not re-classify.",
  })
  crowdLevel: CrowdLevel;

  @ApiProperty({
    example: 21,
    description:
      "Sample-weighted average P50 (median) wait time in minutes across all parks for this " +
      "bucket — informational context only (mixes park sizes), not the ranking signal.",
  })
  avgWaitP50: number;

  @ApiProperty({
    example: 4210,
    description: "Total park-days that contributed to this bucket.",
  })
  sampleDays: number;

  @ApiProperty({
    example: 118,
    description: "Number of distinct parks that contributed to this bucket.",
  })
  parkCount: number;
}

export class GlobalBestTimesMetaDto {
  @ApiProperty({ example: 24, description: "Look-back window size in months." })
  windowMonths: number;

  @ApiProperty({ example: "2024-07-01" })
  dataFrom: string;

  @ApiProperty({ example: "2026-07-21" })
  dataTo: string;

  @ApiProperty({
    example: 132,
    description:
      "Number of parks with enough data to contribute to the aggregate.",
  })
  parkCount: number;

  @ApiProperty({ example: 41200, description: "Total park-days aggregated." })
  totalSampleDays: number;

  @ApiProperty({
    example: true,
    description:
      "True when the data basis is large enough to display (enough parks and sample days). " +
      "Use this as the render gate.",
  })
  displayable: boolean;

  @ApiProperty({
    example: "2026-07-21T06:00:00.000Z",
    description: "When this aggregate was computed (ISO 8601 UTC).",
  })
  generatedAt: string;

  @ApiProperty({ example: 1, description: "Response schema version." })
  schemaVersion: number;
}

export class GlobalBestTimesDto {
  @ApiProperty({
    type: [BestTimeBucketDto],
    description:
      "Relative busyness per weekday (0=Sunday … 6=Saturday), always 7 entries.",
  })
  byDayOfWeek: BestTimeBucketDto[];

  @ApiProperty({
    type: [BestTimeBucketDto],
    description:
      "Relative busyness per month (1=January … 12=December), always 12 entries.",
  })
  byMonth: BestTimeBucketDto[];

  @ApiProperty({ type: GlobalBestTimesMetaDto })
  meta: GlobalBestTimesMetaDto;
}
