import { ApiProperty } from "@nestjs/swagger";
import { CrowdLevel } from "../../common/types/crowd-level.type";

export class ParkStatisticsDto {
  @ApiProperty({
    description: "Average wait time across all operating attractions (minutes)",
    example: 35,
  })
  avgWaitTime: number;

  @ApiProperty({
    description:
      "Average wait time for today across all operating attractions (minutes)",
    example: 32,
  })
  avgWaitToday: number;

  @ApiProperty({
    description: "Peak/maximum wait time recorded today (minutes)",
    example: 75,
  })
  peakWaitToday: number;

  @ApiProperty({
    description: "Hour when peak wait times typically occur (format: HH:00)",
    example: "14:00",
    nullable: true,
  })
  peakHour: string | null;

  @ApiProperty({
    description: "Current crowd level based on occupancy percentage",
    enum: ["very_low", "low", "moderate", "high", "very_high", "extreme"],
    example: "moderate",
    enumName: "CrowdLevel",
  })
  crowdLevel: CrowdLevel;
  totalAttractions: number;
  operatingAttractions: number;
  closedAttractions: number;
  timestamp: Date | string;

  // Phase 4: Optional percentile data
  percentilesToday?: {
    p50: number;
    p75: number;
    p90: number;
    p95: number;
  };

  // History of Park Average Wait Time (for Sparkline/Graph)
  history?: {
    timestamp: string;
    waitTime: number;
  }[];
}

export class AttractionStatisticsDto {
  avgWaitToday: number | null;
  peakWaitToday: number | null;
  peakWaitTimestamp: Date | null;
  minWaitToday: number | null;
  typicalWaitThisHour: number | null;
  percentile95ThisHour: number | null;
  currentVsTypical: number | null;
  dataPoints: number;
  timestamp: Date;
  history: { timestamp: string; waitTime: number }[];

  // Phase 4: Optional percentile data
  distributionToday?: {
    p25: number;
    p50: number;
    p75: number;
    p90: number;
    iqr: number;
  };

  recentPatterns?: {
    p50Last7d: number;
    p90Last7d: number;
  };
}

export interface StatisticsResponseDto {
  park?: ParkStatisticsDto;
  attraction?: AttractionStatisticsDto;
}
