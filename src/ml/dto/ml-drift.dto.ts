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

export class HorizonDriftDto {
  @ApiProperty({ enum: ["hourly", "daily"], description: "Serving horizon" })
  horizon: "hourly" | "daily";

  @ApiProperty({
    description:
      "false when this horizon is never scored against actuals (far-daily predictions " +
      "span up to 365d and are intentionally not compared)",
  })
  tracked: boolean;

  @ApiProperty({
    nullable: true,
    description: "Drift % vs training MAE (null if untracked)",
  })
  currentDrift: number | null;

  @ApiProperty({
    nullable: true,
    description: "Live MAE for this horizon (null if untracked)",
  })
  liveMae: number | null;

  @ApiProperty({ enum: ["healthy", "warning", "critical", "untracked"] })
  status: string;

  @ApiProperty({
    nullable: true,
    description: "What this horizon's drift actually measures",
  })
  note: string | null;
}

export class MLDriftDto {
  @ApiProperty({
    example: 15.3,
    description:
      "Current drift % vs training MAE. NOTE: this is the HOURLY horizon (the only one " +
      "scored) — CatBoost's intraday accuracy, which PCN now serves as the fallback. See " +
      "byHorizon for the per-horizon split.",
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
    description: "Daily accuracy metrics (hourly horizon)",
  })
  dailyMetrics: DailyAccuracyDto[];

  @ApiProperty({
    type: [HorizonDriftDto],
    description:
      "Drift split by serving horizon. hourly = CatBoost intraday (served by the PCN " +
      "fallback); daily = far-daily (31–365d), CatBoost's sole remaining role but unscored.",
  })
  byHorizon: HorizonDriftDto[];
}
