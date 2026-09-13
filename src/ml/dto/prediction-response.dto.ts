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

  @ApiProperty({
    description:
      "Confidence score, 30-100. The description said 0-1 and the served " +
      "values never were: `predict.py` blends a distance term with a spread " +
      "term, each floored at 30. The daily TFT path emitted a flat 0.7 until " +
      "PAR-111 put it on this same formula.",
  })
  confidence: number;

  @ApiProperty({
    description:
      "How many minutes longer than the prediction the wait can plausibly " +
      "run — a one-sided upper half-width, not rounded to 5, because a band " +
      "is a difference and not a posted wait time. Absent means NOT KNOWN and " +
      "never 'narrow'; a zero-wide band would draw as a confident hairline. " +
      "WHERE IT COMES FROM DEPENDS ON WHICH MODEL ANSWERED, and the two are " +
      "not the same statistic: CatBoost reports its own top trained quantile " +
      "(alpha=0.95) minus its served median, which measured against realised " +
      "days covers only 53-57% of them; the daily TFT path (calendar days " +
      "1-60) carries the measured 95th percentile of that cell's residuals, " +
      "which covers 92-98%. So the figure steps DOWN at day 60 even though " +
      "the near term is the better-forecast one. Read it as a floor on how " +
      "wrong the day can be, never as a comparison between two days on " +
      "opposite sides of that seam. See docs/ml/quantile-serving-and-calibration.md.",
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
    description:
      "Adjusted confidence score when deviation detected: half of " +
      "`confidence`, so it shares that field's 30-100 scale and not the 0-1 " +
      "this said. Halving is applied in `park-integration.service.ts`.",
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

  @ApiProperty({
    description:
      "Prediction confidence score, 30-100 — the same scale as " +
      "`PredictionDto.confidence`, which is what fills it. It said 0-1 and " +
      "never was.",
  })
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
