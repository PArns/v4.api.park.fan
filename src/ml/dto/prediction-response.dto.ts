export interface PredictionDto {
  attractionId: string;
  predictedTime: string; // ISO format
  predictedWaitTime: number;
  predictionType: "hourly" | "daily";
  confidence: number;
  trend?: string; // "increasing", "decreasin", "stable"
  crowdLevel:
    | "very_low"
    | "low"
    | "moderate"
    | "high"
    | "very_high"
    | "extreme"
    | "closed";
  baseline: number;
  modelVersion: string;
  status?: string;
}

export interface BulkPredictionResponseDto {
  predictions: PredictionDto[];
  count: number;
  modelVersion: string;
}

export interface ModelInfoDto {
  version: string;
  trainedAt?: string;
  metrics?: {
    mae: number;
    rmse: number;
    mape: number;
    r2: number;
  };
  features?: string[];
}
