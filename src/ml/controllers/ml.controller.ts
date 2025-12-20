import {
  Controller,
  Get,
  Query,
  Param,
  ParseIntPipe,
  DefaultValuePipe,
} from "@nestjs/common";
import { ApiTags, ApiOperation, ApiQuery, ApiResponse } from "@nestjs/swagger";
import { MLDashboardService } from "../services/ml-dashboard.service";
import { MLModelService } from "../services/ml-model.service";
import { PredictionAccuracyService } from "../services/prediction-accuracy.service";
import { MLDashboardDto } from "../dto/ml-dashboard.dto";

/**
 * MLController
 *
 * Unified REST controller for all ML-related endpoints:
 * - Dashboard (System Health, Models)
 * - Accuracy Analytics (System, Park, Attraction levels)
 * - Feature Analysis
 *
 * Replaces MLDashboardController and PredictionAccuracyController.
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
   * Get Active Model Details
   */
  @Get("models/active")
  @ApiOperation({
    summary: "Get detailed information about active model",
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
   */
  @Get("models/history")
  @ApiOperation({
    summary: "Get model version history",
  })
  @ApiQuery({ name: "limit", required: false, type: Number })
  async getModelHistory(
    @Query("limit", new DefaultValuePipe(10), ParseIntPipe) limit: number,
  ) {
    return this.modelService.getModelHistory(limit);
  }

  /**
   * Get System-Wide Accuracy Statistics
   */
  @Get("accuracy/system")
  @ApiOperation({
    summary: "Get system-wide prediction accuracy",
    description: "Aggregated accuracy metrics across all attractions.",
  })
  @ApiQuery({ name: "days", required: false, type: Number })
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
   * Get Park Accuracy Statistics
   */
  @Get("accuracy/parks/:parkId/stats")
  @ApiOperation({
    summary: "Get park accuracy statistics",
    description: "Aggregated prediction accuracy for a specific park.",
  })
  async getParkStats(
    @Param("parkId") parkId: string,
    @Query("days", new DefaultValuePipe(30), ParseIntPipe) days: number,
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
    @Query("days", new DefaultValuePipe(30), ParseIntPipe) days: number,
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
    @Query("days", new DefaultValuePipe(30), ParseIntPipe) days: number,
    @Query("attractionId") attractionId?: string,
  ) {
    return this.accuracyService.analyzeFeatureErrors(
      threshold,
      days,
      attractionId,
    );
  }
}
