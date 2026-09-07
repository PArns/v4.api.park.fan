import { ApiProperty } from "@nestjs/swagger";

export class PredictionDto {
  @ApiProperty()
  attractionId: string;

  @ApiProperty({ description: "Predicted timestamp (ISO 8601)" })
  predictedTime: string;

  @ApiProperty({ description: "Predicted wait time in minutes" })
  predictedWaitTime: number;

  @ApiProperty({ enum: ["hourly", "daily"] })
  predictionType: "hourly" | "daily";

  @ApiProperty({ description: "Confidence score (0-1)" })
  confidence: number;

  @ApiProperty({
    description:
      "Width of the model's uncertainty band in minutes: the top trained " +
      "quantile (alpha=0.95) minus the served median. Absent when the model " +
      "reports no real spread, which is not the same as a zero-wide band. " +
      "Deliberately not rounded to 5 — a band is a difference, not a posted " +
      "wait time.",
    required: false,
  })
  uncertaintyMinutes?: number | null;

  @ApiProperty({
    enum: ["increasing", "decreasing", "stable"],
    required: false,
  })
  trend?: string;

  @ApiProperty({
    enum: [
      "very_low",
      "low",
      "moderate",
      "high",
      "very_high",
      "extreme",
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
    | "closed";

  @ApiProperty({ description: "Baseline wait time" })
  baseline: number;

  @ApiProperty({
    description: "Model version used - internal only",
    required: false,
  })
  modelVersion: string;

  @ApiProperty({ required: false })
  status?: string;

  // Confidence Downgrade fields (Phase 1)
  @ApiProperty({
    description: "Current actual wait time (if deviation detected)",
    required: false,
  })
  currentWaitTime?: number;

  @ApiProperty({
    description: "Adjusted confidence score when deviation detected (0-1)",
    required: false,
  })
  confidenceAdjusted?: number;

  @ApiProperty({
    description: "Whether a deviation from prediction was detected",
    required: false,
  })
  deviationDetected?: boolean;

  @ApiProperty({
    description: "Deviation details",
    required: false,
  })
  deviationInfo?: {
    message: string;
    deviation: number;
    percentageDeviation: number;
    detectedAt: string;
  };
}

export class PredictionItemDto {
  @ApiProperty({ description: "Attraction ID" })
  attractionId: string;

  @ApiProperty({ description: "Predicted wait time in minutes" })
  predictedWaitTime: number;

  @ApiProperty({ description: "Prediction confidence score (0-1)" })
  confidence: number;
}

export class PredictionResponseDto {
  @ApiProperty({ description: "List of predictions", type: [PredictionDto] })
  predictions: PredictionDto[];
}

export class BulkPredictionResponseDto {
  @ApiProperty({ type: [PredictionDto] })
  predictions: PredictionDto[];

  @ApiProperty()
  count: number;

  @ApiProperty({
    description: "Model version - internal only",
    required: false,
  })
  modelVersion: string;
}

export class ModelInfoDto {
  @ApiProperty()
  version: string;

  @ApiProperty({ required: false })
  trainedAt?: string;

  @ApiProperty({ required: false })
  metrics?: {
    mae: number;
    rmse: number;
    mape: number;
    r2: number;
  };

  @ApiProperty({ required: false, type: [String] })
  features?: string[];

  @ApiProperty({ required: false })
  file_size_mb?: number | null;
}
