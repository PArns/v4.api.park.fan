import {
  Controller,
  Get,
  Query,
  Param,
  ParseIntPipe,
  DefaultValuePipe,
} from "@nestjs/common";
import { ApiTags, ApiOperation, ApiQuery, ApiResponse } from "@nestjs/swagger";
import { QueryIntPipe } from "../../common/pipes/query-int.pipe";
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
 * - Accuracy Analytics (System, Park, Attraction levels)
 * - Feature Analysis
 * - Model Management
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
   * Get Model Version History (full model objects)
   */
  @Get("models/history")
  @ApiOperation({
    summary: "Get model version history",
  })
  @ApiQuery({
    name: "limit",
    required: false,
    type: Number,
    description: "Max models to return (default 10, clamped to 1-100)",
  })
  async getModelHistory(
    @Query(
      "limit",
      new QueryIntPipe({ name: "limit", fallback: 10, min: 1, max: 100 }),
    )
    limit: number,
  ) {
    return this.modelService.getModelHistory(limit);
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
    @Query(
      "limit",
      new QueryIntPipe({ name: "limit", fallback: 30, min: 1, max: 100 }),
    )
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
    @Query(
      "days",
      new QueryIntPipe({ name: "days", fallback: 7, min: 1, max: 365 }),
    )
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

  /**
   * Get Park Accuracy Statistics
   */
  @Get("accuracy/parks/:parkId/stats")
  @ApiOperation({
    summary: "Get park accuracy statistics",
    description: "Aggregated prediction accuracy for a specific park.",
  })
  async getParkStats(
    @Param("parkId") parkId: string,
    @Query(
      "days",
      new QueryIntPipe({ name: "days", fallback: 30, min: 1, max: 365 }),
    )
    days: number,
  ) {
    const stats = await this.accuracyService.getParkAccuracyStats(parkId, days);
    return {
      parkId,
      period: `Last ${days} days`,
      statistics: stats,
    };
  }

  /**
   * Get Attraction Accuracy Statistics
   */
  @Get("accuracy/attractions/:attractionId/stats")
  @ApiOperation({
    summary: "Get attraction accuracy statistics",
  })
  async getAttractionStats(
    @Param("attractionId") attractionId: string,
    @Query(
      "days",
      new QueryIntPipe({ name: "days", fallback: 30, min: 1, max: 365 }),
    )
    days: number,
  ) {
    const stats = await this.accuracyService.getAttractionAccuracyStats(
      attractionId,
      days,
    );
    return {
      attractionId,
      period: `Last ${days} days`,
      statistics: stats,
    };
  }

  /**
   * Get Hourly and Day-of-Week Patterns
   */
  @Get("accuracy/trends/hourly")
  @ApiOperation({
    summary: "Get hourly and day-of-week accuracy patterns",
  })
  async getHourlyPatterns(
    @Query(
      "days",
      new QueryIntPipe({ name: "days", fallback: 30, min: 1, max: 365 }),
    )
    days: number,
  ) {
    const hourly = await this.accuracyService.getHourlyAccuracyPatterns(days);
    const dayOfWeek =
      await this.accuracyService.getDayOfWeekAccuracyPatterns(days);

    return {
      byHourOfDay: hourly,
      byDayOfWeek: dayOfWeek,
    };
  }

  /**
   * Analyze Feature Errors
   * Optimized to use SQL aggregation
   */
  @Get("accuracy/features/analysis")
  @ApiOperation({
    summary: "Analyze prediction error correlations",
    description: "Identifies features associated with high prediction errors.",
  })
  async analyzeFeatureErrors(
    @Query("threshold", new DefaultValuePipe(15), ParseIntPipe)
    threshold: number,
    @Query(
      "days",
      new QueryIntPipe({ name: "days", fallback: 30, min: 1, max: 365 }),
    )
    days: number,
    @Query("attractionId") attractionId?: string,
  ) {
    return this.accuracyService.analyzeFeatureErrors(
      threshold,
      days,
      attractionId,
    );
  }
}
