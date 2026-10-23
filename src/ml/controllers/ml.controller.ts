import { Controller, Get } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiQuery, ApiResponse } from "@nestjs/swagger";
import { QueryInt } from "../../common/pipes/query-int.pipe";
import { MLDashboardService } from "../services/ml-dashboard.service";
import { MLModelService } from "../services/ml-model.service";
import { PredictionAccuracyService } from "../services/prediction-accuracy.service";
import {
  MLDashboardDto,
  ModelMetricsHistoryDto,
} from "../dto/ml-dashboard.dto";

/**
 * MLController
 *
 * Core ML endpoints:
 * - Dashboard (System Health, Models)
 * - Accuracy Analytics (system level)
 * - Model metrics history
 *
 * Note: Monitoring endpoints (drift, alerts, anomalies) are in MLMonitoringController
 */
@ApiTags("ML")
@Controller("ml")
export class MLController {
  constructor(
    private dashboardService: MLDashboardService,
    private modelService: MLModelService,
    private accuracyService: PredictionAccuracyService,
  ) {}

  /**
   * Main ML Dashboard Endpoint
   * Returns complete ML system health in a single call.
   */
  @Get("dashboard")
  @ApiOperation({
    summary: "Get comprehensive ML system dashboard",
    description:
      "Returns current model info, system accuracy, and health metrics.",
  })
  @ApiResponse({
    status: 200,
    description: "Dashboard data retrieved successfully",
    type: MLDashboardDto,
  })
  async getDashboard(): Promise<MLDashboardDto> {
    return this.dashboardService.getDashboard();
  }

  /**
   * Get model metrics history for sparklines
   * Returns MAE, RMSE, MAPE, R² over time ordered oldest→newest.
   */
  @Get("models/metrics-history")
  @ApiOperation({
    summary: "Get model metrics history for sparklines",
    description:
      "Returns key accuracy metrics (MAE, RMSE, MAPE, R²) for each trained model, " +
      "ordered oldest to newest. Designed for sparkline charts in the ML dashboard.",
  })
  @ApiQuery({
    name: "limit",
    required: false,
    type: Number,
    description: "Max models to return (default 30, clamped to 1-100)",
  })
  @ApiResponse({ status: 200, type: ModelMetricsHistoryDto })
  async getMetricsHistory(
    @QueryInt({ name: "limit", fallback: 30, min: 1, max: 100 })
    limit: number,
  ): Promise<ModelMetricsHistoryDto> {
    return this.modelService.getMetricsHistory(limit);
  }

  /**
   * Get System-Wide Accuracy Statistics
   */
  @Get("accuracy/system")
  @ApiOperation({
    summary: "Get system-wide prediction accuracy",
    description: "Aggregated accuracy metrics across all attractions.",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Look-back window in days (default 7, clamped to 1-365)",
  })
  async getSystemAccuracy(
    @QueryInt({ name: "days", fallback: 7, min: 1, max: 365 })
    days: number,
  ) {
    const stats = await this.accuracyService.getSystemAccuracyStats(days);
    const performers = await this.accuracyService.getTopBottomPerformers(
      days,
      5,
    );
    // Served-intraday MAE from the PCN board — `byPredictionType.HOURLY` above
    // measures the CatBoost fallback, not what users are actually served.
    const servedIntraday =
      await this.accuracyService.getServedIntradayAccuracy(days);

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
      servedIntraday,
      topPerformers: performers.topPerformers,
      bottomPerformers: performers.bottomPerformers,
    };
  }
}
