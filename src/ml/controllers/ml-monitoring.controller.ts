import {
  Controller,
  Get,
  Post,
  Query,
  Param,
  Body,
  ParseIntPipe,
  DefaultValuePipe,
} from "@nestjs/common";
import {
  ApiTags,
  ApiOperation,
  ApiQuery,
  ApiResponse,
  ApiParam,
} from "@nestjs/swagger";
import { MLFeatureDriftService } from "../services/ml-feature-drift.service";
import { MLAlertService } from "../services/ml-alert.service";
import { MLRequestLoggingService } from "../services/ml-request-logging.service";
import { MLAnomalyDetectionService } from "../services/ml-anomaly-detection.service";
import { MLDriftMonitoringService } from "../services/ml-drift-monitoring.service";
import { MLDriftDto } from "../dto/ml-drift.dto";

/**
 * ML Monitoring Controller
 *
 * Endpoints for ML monitoring and observability:
 * - Feature Drift Detection
 * - Alerts Management
 * - Request Logging & Analytics
 * - Anomaly Detection
 */
@ApiTags("ML Monitoring")
@Controller("ml/monitoring")
export class MLMonitoringController {
  constructor(
    private featureDriftService: MLFeatureDriftService,
    private alertService: MLAlertService,
    private requestLoggingService: MLRequestLoggingService,
    private anomalyDetectionService: MLAnomalyDetectionService,
    private driftService: MLDriftMonitoringService,
  ) {}

  // ==================== Drift Monitoring ====================

  /**
   * Get model drift metrics
   * Track model performance degradation over time
   */
  @Get("drift/model")
  @ApiOperation({
    summary: "Get model drift metrics",
    description:
      "Returns drift analysis comparing live performance vs training performance",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Number of days to analyze (default: 30)",
  })
  @ApiResponse({
    status: 200,
    description: "Drift metrics retrieved successfully",
    type: MLDriftDto,
  })
  async getModelDrift(
    @Query("days", new DefaultValuePipe(30), ParseIntPipe) days: number,
  ): Promise<MLDriftDto> {
    return this.driftService.getDriftMetrics(days);
  }

  /**
   * Get feature drift status
   */
  @Get("drift/features")
  @ApiOperation({
    summary: "Get feature drift detection results",
    description: "Returns detected feature drift for all features",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Days to analyze (default: 7)",
  })
  async getFeatureDrift(
    @Query("days", new DefaultValuePipe(7), ParseIntPipe) days: number,
  ) {
    return await this.featureDriftService.detectFeatureDrift(days);
  }

  /**
   * Get feature drift history for a specific feature
   */
  @Get("drift/features/:featureName")
  @ApiOperation({
    summary: "Get feature drift history",
    description: "Returns drift history for a specific feature",
  })
  @ApiParam({ name: "featureName", description: "Feature name" })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Days to analyze (default: 30)",
  })
  async getFeatureDriftHistory(
    @Param("featureName") featureName: string,
    @Query("days", new DefaultValuePipe(30), ParseIntPipe) days: number,
  ) {
    return await this.featureDriftService.getFeatureDriftHistory(
      featureName,
      days,
    );
  }

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

  /**
   * Acknowledge alert
   */
  @Post("alerts/:id/acknowledge")
  @ApiOperation({
    summary: "Acknowledge an alert",
    description: "Marks an alert as acknowledged",
  })
  @ApiParam({ name: "id", description: "Alert ID" })
  async acknowledgeAlert(
    @Param("id") alertId: string,
    @Body() body: { acknowledgedBy: string },
  ) {
    return await this.alertService.acknowledgeAlert(
      alertId,
      body.acknowledgedBy,
    );
  }

  /**
   * Resolve alert
   */
  @Post("alerts/:id/resolve")
  @ApiOperation({
    summary: "Resolve an alert",
    description: "Marks an alert as resolved",
  })
  @ApiParam({ name: "id", description: "Alert ID" })
  async resolveAlert(
    @Param("id") alertId: string,
    @Body() body?: { resolutionNote?: string },
  ) {
    return await this.alertService.resolveAlert(alertId, body?.resolutionNote);
  }

  /**
   * Check and create alerts (manual trigger)
   */
  @Post("alerts/check")
  @ApiOperation({
    summary: "Check for issues and create alerts",
    description: "Manually triggers alert check (usually done by cron job)",
  })
  async checkAndCreateAlerts() {
    return await this.alertService.checkAndCreateAlerts();
  }

  // ==================== Request Logging ====================

  /**
   * Get request statistics
   */
  @Get("requests/stats")
  @ApiOperation({
    summary: "Get prediction request statistics",
    description: "Returns aggregated statistics about prediction requests",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Days to analyze (default: 7)",
  })
  async getRequestStats(
    @Query("days", new DefaultValuePipe(7), ParseIntPipe) days: number,
  ) {
    return await this.requestLoggingService.getRequestStats(days);
  }

  /**
   * Get request trends
   */
  @Get("requests/trends")
  @ApiOperation({
    summary: "Get prediction request trends",
    description: "Returns daily trends of prediction requests",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Days to analyze (default: 30)",
  })
  async getRequestTrends(
    @Query("days", new DefaultValuePipe(30), ParseIntPipe) days: number,
  ) {
    return await this.requestLoggingService.getRequestTrends(days);
  }

  // ==================== Anomaly Detection ====================

  /**
   * Get detected anomalies
   */
  @Get("anomalies")
  @ApiOperation({
    summary: "Get detected prediction anomalies",
    description: "Returns anomalies detected in recent predictions",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Days to analyze (default: 7)",
  })
  @ApiQuery({
    name: "severity",
    required: false,
    enum: ["low", "medium", "high"],
    description: "Filter by severity",
  })
  async getAnomalies(
    @Query("days", new DefaultValuePipe(7), ParseIntPipe) days: number,
    @Query("severity") severity?: "low" | "medium" | "high",
  ) {
    return await this.anomalyDetectionService.getAnomalies(days, severity);
  }

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

  /**
   * Detect anomalies (manual trigger)
   */
  @Post("anomalies/detect")
  @ApiOperation({
    summary: "Detect anomalies in recent predictions",
    description:
      "Manually triggers anomaly detection (usually done by cron job)",
  })
  @ApiQuery({
    name: "days",
    required: false,
    type: Number,
    description: "Days to analyze (default: 7)",
  })
  async detectAnomalies(
    @Query("days", new DefaultValuePipe(7), ParseIntPipe) days: number,
  ) {
    return await this.anomalyDetectionService.detectAnomalies(days);
  }
}
