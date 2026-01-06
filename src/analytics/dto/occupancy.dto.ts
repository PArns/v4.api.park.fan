import { ApiProperty } from "@nestjs/swagger";

export class OccupancyDto {
  @ApiProperty({
    description:
      "Current occupancy percentage (0-100+). Can exceed 100% on extremely busy days.",
    example: 85,
    minimum: 0,
  })
  current: number;

  @ApiProperty({
    description: "Park-wide trend direction based on last hour comparison",
    enum: ["up", "stable", "down"],
    example: "up",
  })
  trend: "up" | "stable" | "down";

  @ApiProperty({
    description:
      "Absolute difference from typical wait time (P90 baseline) in minutes",
    example: 15,
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
      "90th percentile baseline wait time used for comparison (minutes)",
    example: 30,
  })
  baseline90thPercentile: number;

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
