import { ApiProperty } from "@nestjs/swagger";

import { CrowdLevel } from "../../common/types/crowd-level.type";

export class OccupancyDto {
  @ApiProperty({
    description:
      "Current occupancy percentage (0-100+). Can exceed 100% on extremely busy days.",
    example: 85,
    minimum: 0,
  })
  current: number;

  @ApiProperty({
    description:
      "Gated crowd-level rating derived from `current`. Reads `unknown` " +
      "('keine Prognose') when the park is not ratable yet (< 30 operating " +
      "days of headliner data); otherwise one of the six occupancy tiers. " +
      "The numeric `current` stays valid regardless — only this label flips.",
    enum: [
      "very_low",
      "low",
      "moderate",
      "high",
      "very_high",
      "extreme",
      "unknown",
    ],
    example: "high",
    required: false,
  })
  crowdLevel?: CrowdLevel;

  @ApiProperty({
    description: "Park-wide trend direction based on last hour comparison",
    enum: ["up", "stable", "down"],
    example: "up",
  })
  trend: "up" | "stable" | "down";

  @ApiProperty({
    description:
      "Percentage difference from typical, on the SAME basis as `current` " +
      "(always equals `current` − 100). E.g. current 114 → +14 means 14% " +
      "above a typical day. Use `current` for the absolute reading and this " +
      "field for the signed delta — they will never contradict each other.",
    example: 14,
  })
  comparedToTypical: number;

  @ApiProperty({
    description: "Human-readable comparison status",
    enum: ["lower", "typical", "higher", "closed"],
    example: "higher",
  })
  comparisonStatus: "lower" | "typical" | "higher" | "closed";

  @ApiProperty({
    description:
      "Typical-day baseline wait time (minutes) used for the comparison — " +
      "the P50 median over the trailing window, NOT the 90th percentile. " +
      "The `90thPercentile` name is a historical misnomer retained for " +
      "backward compatibility.",
    example: 50,
  })
  baseline90thPercentile: number;

  @ApiProperty({
    description:
      "Confidence level of the P90 baseline (high: ≥90 days, medium: 30-89 days, low: <30 days)",
    enum: ["high", "medium", "low"],
    example: "high",
    required: false,
  })
  confidence?: "high" | "medium" | "low";

  @ApiProperty({
    description: "ISO 8601 timestamp when occupancy was calculated",
    example: "2024-01-15T14:30:00Z",
  })
  updatedAt: string;

  @ApiProperty({
    description: "Detailed breakdown of occupancy calculation",
    required: false,
  })
  breakdown?: {
    currentAvgWait: number;
    typicalAvgWait: number;
    activeAttractions: number;
  };
}

export class ParkOccupancyResponseDto {
  @ApiProperty({ type: OccupancyDto })
  occupancy: OccupancyDto;

  @ApiProperty({
    description: "Optional informational message",
    required: false,
    example: "Park is currently busier than typical for this time",
  })
  message?: string;
}
