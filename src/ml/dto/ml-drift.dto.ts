import { ApiProperty } from "@nestjs/swagger";

export class DailyAccuracyDto {
  @ApiProperty({ example: "2025-12-25", description: "Date" })
  date: string;

  @ApiProperty({
    example: 8.5,
    description: "Mean Absolute Error for this day",
  })
  mae: number;

  @ApiProperty({
    example: 1250,
    description: "Number of predictions compared",
  })
  predictionsCount: number;
}

export class MLDriftDto {
  @ApiProperty({
    example: 15.3,
    description: "Current drift percentage vs training MAE",
  })
  currentDrift: number;

  @ApiProperty({
    example: 20.0,
    description: "Alert threshold percentage",
  })
  threshold: number;

  @ApiProperty({
    example: "healthy",
    enum: ["healthy", "warning", "critical"],
    description: "Drift status",
  })
  status: string;

  @ApiProperty({
    example: 8.95,
    description: "Training MAE (baseline)",
  })
  trainingMae: number;

  @ApiProperty({
    example: 10.3,
    description: "Current live MAE",
  })
  liveMae: number;

  @ApiProperty({
    type: [DailyAccuracyDto],
    description: "Daily accuracy metrics",
  })
  dailyMetrics: DailyAccuracyDto[];
}
