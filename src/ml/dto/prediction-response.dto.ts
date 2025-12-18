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

  @ApiProperty({ required: false })
  status?: string;
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
}
