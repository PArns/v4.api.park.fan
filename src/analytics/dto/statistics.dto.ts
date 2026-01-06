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

  @ApiProperty({
    description: "Total number of attractions in the park",
    example: 45,
  })
  totalAttractions: number;

  @ApiProperty({
    description: "Number of attractions currently operating",
    example: 42,
  })
  operatingAttractions: number;

  @ApiProperty({
    description: "Number of attractions currently closed",
    example: 3,
  })
  closedAttractions: number;

  @ApiProperty({
    description: "Timestamp when statistics were calculated",
    example: "2024-01-15T14:30:00Z",
  })
  timestamp: Date | string;

  @ApiProperty({
    description: "Optional percentile distribution for today",
    required: false,
  })
  percentilesToday?: {
    p50: number;
    p75: number;
    p90: number;
    p95: number;
  };

  @ApiProperty({
    description:
      "Historical wait time data points for sparkline/graph visualization",
    required: false,
    type: "array",
    items: {
      type: "object",
      properties: {
        timestamp: { type: "string", format: "date-time" },
        waitTime: { type: "number" },
      },
    },
  })
  history?: {
    timestamp: string;
    waitTime: number;
  }[];
}

export class AttractionStatisticsDto {
  @ApiProperty({
    description: "Average wait time today (minutes)",
    example: 25,
    nullable: true,
  })
  avgWaitToday: number | null;

  @ApiProperty({
    description: "Peak/maximum wait time today (minutes)",
    example: 60,
    nullable: true,
  })
  peakWaitToday: number | null;

  @ApiProperty({
    description: "Timestamp when peak wait time occurred",
    example: "2024-01-15T14:30:00Z",
    nullable: true,
  })
  peakWaitTimestamp: Date | null;

  @ApiProperty({
    description: "Minimum wait time today (minutes)",
    example: 5,
    nullable: true,
  })
  minWaitToday: number | null;

  @ApiProperty({
    description:
      "Typical wait time for current hour based on historical data (minutes)",
    example: 20,
    nullable: true,
  })
  typicalWaitThisHour: number | null;

  @ApiProperty({
    description: "95th percentile wait time for current hour (minutes)",
    example: 45,
    nullable: true,
  })
  percentile95ThisHour: number | null;

  @ApiProperty({
    description: "Percentage difference from typical wait time",
    example: 25,
    nullable: true,
  })
  currentVsTypical: number | null;

  @ApiProperty({
    description: "Number of data points collected today",
    example: 120,
  })
  dataPoints: number;

  @ApiProperty({
    description: "Timestamp when statistics were calculated",
    example: "2024-01-15T14:30:00Z",
  })
  timestamp: Date;

  @ApiProperty({
    description: "Historical wait time data points for visualization",
    type: "array",
    items: {
      type: "object",
      properties: {
        timestamp: { type: "string", format: "date-time" },
        waitTime: { type: "number" },
      },
    },
  })
  history: { timestamp: string; waitTime: number }[];

  @ApiProperty({
    description: "Optional percentile distribution for today",
    required: false,
  })
  distributionToday?: {
    p25: number;
    p50: number;
    p75: number;
    p90: number;
    iqr: number;
  };

  @ApiProperty({
    description: "Optional recent pattern statistics",
    required: false,
  })
  recentPatterns?: {
    p50Last7d: number;
    p90Last7d: number;
  };
}

export interface StatisticsResponseDto {
  park?: ParkStatisticsDto;
  attraction?: AttractionStatisticsDto;
}
