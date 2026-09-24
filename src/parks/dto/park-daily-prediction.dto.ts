import { ApiProperty } from "@nestjs/swagger";

export class ParkDailyPredictionDto {
  @ApiProperty({
    description: "Date of prediction (YYYY-MM-DD)",
    example: "2025-12-17",
  })
  date: string;

  @ApiProperty({
    description:
      "Predicted crowd level. `closed` when the park's schedule says the park " +
      "is shut that day (same rule as the calendar). `unknown` when the park " +
      "has no typical-day-peak to rate against.",
    enum: [
      "very_low",
      "low",
      "moderate",
      "high",
      "very_high",
      "extreme",
      "unknown",
      "closed",
    ],
  })
  crowdLevel:
    | "very_low"
    | "low"
    | "moderate"
    | "high"
    | "very_high"
    | "extreme"
    | "unknown"
    | "closed";

  @ApiProperty({
    description: "Confidence percentage of the prediction",
    example: 85.5,
  })
  confidencePercentage: number;

  @ApiProperty({
    description:
      "Recommendation for visiting. Absent when `crowdLevel` is `unknown`.",
    required: false,
    enum: [
      "highly_recommended",
      "recommended",
      "neutral",
      "avoid",
      "strongly_avoid",
      "closed",
    ],
  })
  recommendation?:
    | "highly_recommended"
    | "recommended"
    | "neutral"
    | "avoid"
    | "strongly_avoid"
    | "closed";

  @ApiProperty({ description: "Source of the prediction", example: "ml" })
  source: "ml";

  @ApiProperty({
    description: "Predicted average wait time in minutes",
    required: false,
    example: 35,
  })
  avgWaitTime?: number;
}
