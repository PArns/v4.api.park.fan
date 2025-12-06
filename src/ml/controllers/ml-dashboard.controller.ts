import {
  Controller,
  Get,
  Query,
  ParseIntPipe,
  DefaultValuePipe,
} from "@nestjs/common";
import { ApiTags, ApiOperation, ApiQuery, ApiResponse } from "@nestjs/swagger";
import { MLDashboardService } from "../services/ml-dashboard.service";
import { MLModelService } from "../services/ml-model.service";
import { PredictionAccuracyService } from "../services/prediction-accuracy.service";
import { MLDashboardDto } from "../dto/ml-dashboard.dto";

/**
 * MLDashboardController
 *
 * REST endpoints for ML monitoring dashboard
 * - Main dashboard endpoint (comprehensive view)
 * - Model endpoints (active, history)
 * - Accuracy endpoints (system-wide, trends)
 */
@ApiTags("ML Dashboard")
@Controller("ml")
export class MLDashboardController {
  constructor(
    private dashboardService: MLDashboardService,
    private modelService: MLModelService,
    private accuracyService: PredictionAccuracyService,
  ) {}

  /**
   * Main ML Dashboard Endpoint
   *
   * GET /v1/ml/dashboard
   *
   * Returns complete ML system health in a single call:
   * - Current model info with file size
   * - System-wide accuracy (last 7 days)
   * - Model comparison (current vs previous)
   * - Daily trends (last 30 days)
   * - Hourly/day-of-week patterns
   * - Top/bottom performing attractions
   *
   * Expected response time: ~300-500ms
   */
  @Get("dashboard")
  @ApiOperation({
    summary: "Get comprehensive ML system dashboard",
    description:
      "Returns current model info, system-wide accuracy, trends, and health metrics in a single call",
  })
  @ApiResponse({
    status: 200,
    description: "Dashboard data retrieved successfully",
    type: MLDashboardDto,
  })
  @ApiResponse({
    status: 404,
    description: "No active model found",
  })
  async getDashboard(): Promise<MLDashboardDto> {
    return this.dashboardService.getDashboard();
  }

  /**
   * Get Active Model Details
   *
   * GET /v1/ml/models/active
   *
   * Returns detailed information about the currently active model:
   * - Version, training date, file size
   * - Training metrics (MAE, RMSE, MAPE, R²)
   * - Training data info (samples, date range)
   * - Configuration (features, hyperparameters)
   */
  @Get("models/active")
  @ApiOperation({
    summary: "Get detailed information about active model",
    description:
      "Returns complete model metadata including file size, training metrics, and configuration",
  })
  @ApiResponse({
    status: 200,
    description: "Active model details",
  })
  @ApiResponse({
    status: 404,
    description: "No active model found",
  })
  async getActiveModel() {
    const model = await this.modelService.getActiveModelWithDetails();

    if (!model) {
      return {
        error: "No active model found",
        message: "Train a model first to see details",
      };
    }

    return model;
  }

  /**
   * Get Model Version History
   *
   * GET /v1/ml/models/history?limit=10
   *
   * Returns last N models ordered by training date (most recent first)
   */
  @Get("models/history")
  @ApiOperation({
    summary: "Get model version history",
    description: "Returns list of past models ordered by training date",
  })
  @ApiQuery({
    name: "limit",
    required: false,
    type: Number,
    description: "Number of models to return (default: 10)",
  })
  @ApiResponse({
    status: 200,
    description: "Model history retrieved successfully",
  })
  async getModelHistory(
    @Query("limit", new DefaultValuePipe(10), ParseIntPipe) limit: number,
  ) {
    return this.modelService.getModelHistory(limit);
  }

  /**
   * Get System-Wide Accuracy Statistics
   *
   * GET /v1/ml/accuracy/system?days=7
   *
   * Returns aggregated accuracy across all attractions:
   * - Overall MAE, RMSE, MAPE, R²
   * - Breakdown by prediction type (HOURLY vs DAILY)
   * - Accuracy badge (excellent/good/fair/poor)
   * - Top/bottom 5 performing attractions
   */
  @Get("accuracy/system")
  @ApiOperation({
    summary: "Get system-wide prediction accuracy",
    description:
      "Returns aggregated accuracy metrics across all attractions with top/bottom performers",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Number of days to analyze (default: 7)",
  })
  @ApiResponse({
    status: 200,
    description: "System accuracy stats retrieved successfully",
  })
  async getSystemAccuracy(
    @Query("days", new DefaultValuePipe(7), ParseIntPipe) days: number,
  ) {
    const stats = await this.accuracyService.getSystemAccuracyStats(days);
    const performers = await this.accuracyService.getTopBottomPerformers(
      days,
      5,
    );

    const badge = this.accuracyService.calculateAccuracyBadge(
      stats.overall.mae,
      stats.overall.matchedPredictions,
    );

    return {
      period: `Last ${days} days`,
      overall: {
        ...stats.overall,
        badge: badge.badge,
        badgeMessage: badge.message,
      },
      byPredictionType: stats.byPredictionType,
      topPerformers: performers.topPerformers,
      bottomPerformers: performers.bottomPerformers,
    };
  }

  /**
   * Get Daily Accuracy Trends
   *
   * GET /v1/ml/accuracy/trends/daily?days=30
   *
   * Returns daily accuracy breakdown over time
   * Useful for visualizing if the model is improving or degrading
   */
  @Get("accuracy/trends/daily")
  @ApiOperation({
    summary: "Get daily accuracy trends",
    description:
      "Returns daily MAE breakdown to visualize model performance over time",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Number of days to analyze (default: 30)",
  })
  @ApiResponse({
    status: 200,
    description: "Daily trends retrieved successfully",
  })
  async getDailyTrends(
    @Query("days", new DefaultValuePipe(30), ParseIntPipe) days: number,
  ) {
    return this.accuracyService.getDailyAccuracyTrends(days);
  }

  /**
   * Get Hourly and Day-of-Week Accuracy Patterns
   *
   * GET /v1/ml/accuracy/trends/hourly?days=30
   *
   * Returns accuracy patterns by:
   * - Hour of day (0-23) - which hours are hardest to predict
   * - Day of week (0-6) - weekends vs weekdays
   */
  @Get("accuracy/trends/hourly")
  @ApiOperation({
    summary: "Get hourly and day-of-week accuracy patterns",
    description:
      "Returns accuracy breakdown by hour of day and day of week to identify problematic time periods",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Number of days to analyze (default: 30)",
  })
  @ApiResponse({
    status: 200,
    description: "Hourly patterns retrieved successfully",
  })
  async getHourlyPatterns(
    @Query("days", new DefaultValuePipe(30), ParseIntPipe) days: number,
  ) {
    const hourly = await this.accuracyService.getHourlyAccuracyPatterns(days);
    const dayOfWeek =
      await this.accuracyService.getDayOfWeekAccuracyPatterns(days);

    return {
      byHourOfDay: hourly,
      byDayOfWeek: dayOfWeek,
    };
  }
}
