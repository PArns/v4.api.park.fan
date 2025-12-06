import { ApiProperty } from "@nestjs/swagger";

/**
 * ML Dashboard DTOs
 * Complete type definitions for ML monitoring dashboard endpoints
 */

// ==================== MODEL SECTION ====================

export class ModelTrainingMetricsDto {
  @ApiProperty({ example: 10.98, description: "Mean Absolute Error (minutes)" })
  mae: number;

  @ApiProperty({
    example: 15.23,
    description: "Root Mean Square Error (minutes)",
  })
  rmse: number;

  @ApiProperty({
    example: 18.45,
    description: "Mean Absolute Percentage Error (%)",
  })
  mape: number;

  @ApiProperty({ example: 0.87, description: "R² coefficient (0-1)" })
  r2Score: number;
}

export class TrainingDataInfoDto {
  @ApiProperty({
    example: "2023-12-05",
    description: "Training data start date (YYYY-MM-DD)",
  })
  startDate: string;

  @ApiProperty({
    example: "2025-12-05",
    description: "Training data end date (YYYY-MM-DD)",
  })
  endDate: string;

  @ApiProperty({
    example: 1250000,
    description: "Total samples (train + validation)",
  })
  totalSamples: number;

  @ApiProperty({ example: 1000000, description: "Training samples" })
  trainSamples: number;

  @ApiProperty({ example: 250000, description: "Validation samples" })
  validationSamples: number;

  @ApiProperty({ example: 730, description: "Training data duration in days" })
  dataDurationDays: number;
}

export class ModelConfigurationDto {
  @ApiProperty({
    type: [String],
    description: "List of features used in training",
  })
  featuresUsed: string[];

  @ApiProperty({ example: 22, description: "Number of features" })
  featureCount: number;

  @ApiProperty({ description: "Model hyperparameters" })
  hyperparameters: Record<string, any>;
}

export class CurrentModelDto {
  @ApiProperty({ example: "1.1.0", description: "Model version" })
  version: string;

  @ApiProperty({ description: "Training completion timestamp (ISO 8601)" })
  trainedAt: string;

  @ApiProperty({
    example: "/app/models/catboost_v1.1.0.cbm",
    description: "Path to model file",
  })
  filePath: string;

  @ApiProperty({
    example: 2457600,
    nullable: true,
    description: "Model file size in bytes",
  })
  fileSizeBytes: number | null;

  @ApiProperty({
    example: 2.34,
    nullable: true,
    description: "Model file size in MB",
  })
  fileSizeMB: number | null;

  @ApiProperty({ example: "CATBOOST", description: "Model type" })
  modelType: string;

  @ApiProperty({
    example: true,
    description: "Whether this is the active model",
  })
  isActive: boolean;

  @ApiProperty({ description: "Training metrics (MAE, RMSE, MAPE, R²)" })
  trainingMetrics: ModelTrainingMetricsDto;

  @ApiProperty({ description: "Training data information" })
  trainingData: TrainingDataInfoDto;

  @ApiProperty({
    description: "Model configuration (features, hyperparameters)",
  })
  configuration: ModelConfigurationDto;
}

// ==================== ACCURACY SECTION ====================

export class SystemAccuracyOverallDto {
  @ApiProperty({ example: 11.2, description: "Mean Absolute Error (minutes)" })
  mae: number;

  @ApiProperty({
    example: 16.1,
    description: "Root Mean Square Error (minutes)",
  })
  rmse: number;

  @ApiProperty({
    example: 19.3,
    description: "Mean Absolute Percentage Error (%)",
  })
  mape: number;

  @ApiProperty({ example: 0.85, description: "R² coefficient (0-1)" })
  r2Score: number;

  @ApiProperty({
    enum: ["excellent", "good", "fair", "poor", "insufficient_data"],
    description: "Accuracy badge based on MAE thresholds",
  })
  badge: string;

  @ApiProperty({ example: 45000, description: "Total predictions made" })
  totalPredictions: number;

  @ApiProperty({
    example: 38000,
    description: "Predictions matched with actual data",
  })
  matchedPredictions: number;

  @ApiProperty({
    example: 84.4,
    description: "Percentage of predictions matched (%)",
  })
  coveragePercent: number;
}

export class PredictionTypeBreakdownDto {
  @ApiProperty({
    example: 9.8,
    description: "Mean Absolute Error for this prediction type",
  })
  mae: number;

  @ApiProperty({
    example: 30000,
    description: "Total predictions of this type",
  })
  totalPredictions: number;

  @ApiProperty({
    example: 88.2,
    description: "Coverage percentage for this type (%)",
  })
  coveragePercent: number;
}

export class AttractionPerformanceDto {
  @ApiProperty({ description: "Attraction UUID" })
  attractionId: string;

  @ApiProperty({ example: "Space Mountain", description: "Attraction name" })
  attractionName: string;

  @ApiProperty({ example: "Disneyland Paris", description: "Park name" })
  parkName: string;

  @ApiProperty({ example: 3.2, description: "Mean Absolute Error (minutes)" })
  mae: number;

  @ApiProperty({
    example: 500,
    description: "Number of predictions for this attraction",
  })
  predictionsCount: number;
}

export class SystemAccuracyDto {
  @ApiProperty({ description: "Overall system-wide accuracy metrics" })
  overall: SystemAccuracyOverallDto;

  @ApiProperty({ description: "Accuracy breakdown by prediction type" })
  byPredictionType: {
    HOURLY: PredictionTypeBreakdownDto;
    DAILY: PredictionTypeBreakdownDto;
  };

  @ApiProperty({
    type: [AttractionPerformanceDto],
    description: "Top 5 best performing attractions",
  })
  topPerformers: AttractionPerformanceDto[];

  @ApiProperty({
    type: [AttractionPerformanceDto],
    description: "Top 5 worst performing attractions",
  })
  bottomPerformers: AttractionPerformanceDto[];
}

// ==================== TRENDS SECTION ====================

export class ModelVersionInfoDto {
  @ApiProperty({ example: "1.1.0", description: "Model version" })
  version: string;

  @ApiProperty({ example: 10.98, description: "Mean Absolute Error (minutes)" })
  mae: number;

  @ApiProperty({ description: "Training completion timestamp (ISO 8601)" })
  trainedAt: string;
}

export class ModelImprovementDto {
  @ApiProperty({
    example: -1.47,
    description: "Change in MAE (negative = improvement)",
  })
  maeDelta: number;

  @ApiProperty({
    example: -11.8,
    description: "Percentage change in MAE (negative = improvement)",
  })
  maePercentChange: number;

  @ApiProperty({ example: true, description: "Whether the model has improved" })
  isImproving: boolean;
}

export class ModelComparisonDto {
  @ApiProperty({ description: "Current model information", nullable: true })
  current: ModelVersionInfoDto | null;

  @ApiProperty({ description: "Previous model information", nullable: true })
  previous: ModelVersionInfoDto | null;

  @ApiProperty({ description: "Improvement metrics", nullable: true })
  improvement: ModelImprovementDto | null;
}

export class DailyAccuracyDto {
  @ApiProperty({ example: "2025-12-05", description: "Date (YYYY-MM-DD)" })
  date: string;

  @ApiProperty({
    example: 10.5,
    description: "Mean Absolute Error for this day (minutes)",
  })
  mae: number;

  @ApiProperty({
    example: 1500,
    description: "Number of predictions for this day",
  })
  predictionsCount: number;

  @ApiProperty({
    example: 85.3,
    description: "Coverage percentage for this day (%)",
  })
  coveragePercent: number;
}

export class HourlyAccuracyDto {
  @ApiProperty({ example: 14, description: "Hour of day (0-23)" })
  hour: number;

  @ApiProperty({
    example: 15.3,
    description: "Mean Absolute Error for this hour (minutes)",
  })
  mae: number;

  @ApiProperty({
    example: 2000,
    description: "Number of predictions for this hour",
  })
  predictionsCount: number;
}

export class DayOfWeekAccuracyDto {
  @ApiProperty({
    example: 0,
    description: "Day of week (0=Sunday, 6=Saturday)",
  })
  dayOfWeek: number;

  @ApiProperty({ example: "Sunday", description: "Day name" })
  dayName: string;

  @ApiProperty({
    example: 12.3,
    description: "Mean Absolute Error for this day (minutes)",
  })
  mae: number;

  @ApiProperty({
    example: 5000,
    description: "Number of predictions for this day",
  })
  predictionsCount: number;
}

export class TrendsDto {
  @ApiProperty({ description: "Model comparison (current vs previous)" })
  modelComparison: ModelComparisonDto;

  @ApiProperty({
    type: [DailyAccuracyDto],
    description: "Daily accuracy trends (last 30 days)",
  })
  dailyAccuracy: DailyAccuracyDto[];

  @ApiProperty({
    type: [HourlyAccuracyDto],
    description: "Accuracy by hour of day (0-23)",
  })
  byHourOfDay: HourlyAccuracyDto[];

  @ApiProperty({
    type: [DayOfWeekAccuracyDto],
    description: "Accuracy by day of week",
  })
  byDayOfWeek: DayOfWeekAccuracyDto[];
}

// ==================== SYSTEM HEALTH SECTION ====================

export class LastJobDto {
  @ApiProperty({ description: "Job completion timestamp (ISO 8601)" })
  completedAt: string;

  @ApiProperty({ example: 420, description: "Job duration in seconds" })
  durationSeconds: number;

  @ApiProperty({
    enum: ["success", "failed", "unknown"],
    description: "Job status",
  })
  status: string;
}

export class LastAccuracyCheckDto {
  @ApiProperty({ description: "Last accuracy check timestamp (ISO 8601)" })
  completedAt: string;

  @ApiProperty({
    example: 150,
    description: "Number of new comparisons added in last run",
  })
  newComparisonsAdded: number;
}

export class ModelAgeDto {
  @ApiProperty({ example: 1, description: "Days since model was trained" })
  days: number;

  @ApiProperty({
    example: 8,
    description: "Hours since model was trained (excluding full days)",
  })
  hours: number;
}

export class SystemHealthDto {
  @ApiProperty({ description: "Last training job information" })
  lastTrainingJob: LastJobDto;

  @ApiProperty({ description: "Last accuracy check information" })
  lastAccuracyCheck: LastAccuracyCheckDto;

  @ApiProperty({ description: "Next scheduled training timestamp (ISO 8601)" })
  nextScheduledTraining: string;

  @ApiProperty({ description: "Model age (days and hours since training)" })
  modelAge: ModelAgeDto;
}

// ==================== MAIN DASHBOARD RESPONSE ====================

export class MLDashboardDto {
  @ApiProperty({ description: "Current model information with file size" })
  currentModel: CurrentModelDto;

  @ApiProperty({
    description: "System-wide accuracy metrics and top/bottom performers",
  })
  systemAccuracy: SystemAccuracyDto;

  @ApiProperty({ description: "Trends and model comparison" })
  trends: TrendsDto;

  @ApiProperty({ description: "System health and job status" })
  systemHealth: SystemHealthDto;
}
