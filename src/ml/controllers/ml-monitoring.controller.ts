import {
  Controller,
  Get,
  Query,
  ParseIntPipe,
  DefaultValuePipe,
} from "@nestjs/common";
import { ApiTags, ApiOperation, ApiQuery } from "@nestjs/swagger";
import { MLAlertService } from "../services/ml-alert.service";
import { MLAnomalyDetectionService } from "../services/ml-anomaly-detection.service";
import { PredictionAccuracyService } from "../services/prediction-accuracy.service";

/**
 * ML Monitoring Controller
 *
 * Read-only endpoints the admin ML dashboard fetches through its GET-only
 * passthrough:
 * - Active alerts
 * - Anomaly statistics
 * - TFT per-attraction performers
 */
@ApiTags("ML Monitoring")
@Controller("ml/monitoring")
export class MLMonitoringController {
  constructor(
    private alertService: MLAlertService,
    private anomalyDetectionService: MLAnomalyDetectionService,
    private accuracyService: PredictionAccuracyService,
  ) {}

  // ==================== Alerts ====================

  /**
   * Get active alerts
   */
  @Get("alerts")
  @ApiOperation({
    summary: "Get active ML alerts",
    description: "Returns all active alerts for model performance issues",
  })
  async getActiveAlerts() {
    return await this.alertService.getActiveAlerts();
  }

  // ==================== Anomaly Detection ====================

  /**
   * Get anomaly statistics
   */
  @Get("anomalies/stats")
  @ApiOperation({
    summary: "Get anomaly statistics",
    description: "Returns aggregated statistics about detected anomalies",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Days to analyze (default: 7)",
  })
  async getAnomalyStats(
    @Query("days", new DefaultValuePipe(7), ParseIntPipe) days: number,
  ) {
    return await this.anomalyDetectionService.getAnomalyStats(days);
  }

  // ==================== TFT per-attraction accuracy ====================

  /**
   * Best/worst per-attraction performers for the TFT daily model.
   * Mirrors the CatBoost best/worst board (insights.topPerformers in the ML
   * dashboard) but scores TFT daily-peak forecasts against the realised daily
   * P90, so the admin can see TFT quality per attraction alongside CatBoost.
   */
  @Get("tft/performers")
  @ApiOperation({
    summary: "Get best/worst TFT daily-forecast performers per attraction",
    description:
      "TFT predicts the daily peak; scored against realised daily P90. Same shape as the CatBoost best/worst board.",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Days to analyze (default: 14)",
  })
  async getTftPerformers(
    @Query("days", new DefaultValuePipe(14), ParseIntPipe) days: number,
  ) {
    return await this.accuracyService.getTftTopBottomPerformers(days);
  }
}
