import { Injectable, Logger, Inject } from "@nestjs/common";
import { Attraction } from "../entities/attraction.entity";
import { AttractionResponseDto } from "../dto/attraction-response.dto";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { MLService } from "../../ml/ml.service";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * Attraction Integration Service
 *
 * Dedicated service for building integrated attraction responses with live data.
 * Follows NestJS best practices - separates complex business logic from controller.
 *
 * Responsibilities:
 * - Fetches and integrates data from multiple sources (queue, forecasts, ML, analytics)
 * - Caches responses for performance
 * - Builds complete AttractionResponseDto
 */
@Injectable()
export class AttractionIntegrationService {
  private readonly logger = new Logger(AttractionIntegrationService.name);
  private readonly TTL_INTEGRATED_RESPONSE = 5 * 60; // 5 minutes for real-time data

  constructor(
    private readonly queueDataService: QueueDataService,
    private readonly analyticsService: AnalyticsService,
    private readonly mlService: MLService,
    private readonly predictionAccuracyService: PredictionAccuracyService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Build integrated attraction response with live data
   *
   * Fetches and integrates:
   * - Current queue data (all queue types)
   * - Status + trend analysis
   * - ThemeParks.wiki forecasts (24 hours)
   * - ML predictions (daily, up to 1 year)
   * - Statistics (analytics)
   * - Prediction accuracy metrics
   *
   * Cached for 5 minutes for better performance on frequent requests
   *
   * @param attraction - Attraction entity
   * @returns Complete attraction DTO with all integrated live data
   */
  async buildIntegratedResponse(
    attraction: Attraction,
  ): Promise<AttractionResponseDto> {
    // Try cache first
    const cacheKey = `attraction:integrated:${attraction.id}`;
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      return JSON.parse(cached);
    }

    // Start with base DTO
    const dto = AttractionResponseDto.fromEntity(attraction);

    // Fetch current queue data
    const queueData = await this.queueDataService.findCurrentStatusByAttraction(
      attraction.id,
    );

    if (queueData.length > 0) {
      // Convert to DTOs and add trend data
      dto.queues = await Promise.all(
        queueData.map(async (qd) => {
          const queueDto: QueueDataItemDto = {
            queueType: qd.queueType,
            status: qd.status,
            waitTime: qd.waitTime ?? null,
            state: qd.state ?? null,
            returnStart: qd.returnStart ? qd.returnStart.toISOString() : null,
            returnEnd: qd.returnEnd ? qd.returnEnd.toISOString() : null,
            price: qd.price ?? null,
            allocationStatus: qd.allocationStatus ?? null,
            currentGroupStart: qd.currentGroupStart ?? null,
            currentGroupEnd: qd.currentGroupEnd ?? null,
            estimatedWait: qd.estimatedWait ?? null,
            lastUpdated: (qd.lastUpdated || qd.timestamp).toISOString(),
          };

          // Add trend data for wait-time based queues (STANDBY, SINGLE_RIDER, etc.)
          if (
            qd.waitTime !== null &&
            qd.waitTime !== undefined &&
            qd.status === "OPERATING"
          ) {
            try {
              const trendData =
                await this.analyticsService.detectAttractionTrend(
                  attraction.id,
                  qd.queueType,
                );
              queueDto.trend = {
                direction: trendData.trend,
                changeRate: trendData.changeRate,
                recentAverage: trendData.recentAverage,
                previousAverage: trendData.previousAverage,
              };
            } catch (error) {
              // If trend calculation fails, don't include it (optional field)
              this.logger.warn(
                `Failed to calculate trend for ${qd.queueType}:`,
                error,
              );
            }
          }

          return queueDto;
        }),
      );

      // Set overall status (use first queue's status as representative)
      dto.status = queueData[0].status;
    }

    // Fetch forecasts (ThemeParks.wiki - next 24 hours)
    const forecasts = await this.queueDataService.findForecastsByAttraction(
      attraction.id,
      24,
    );

    if (forecasts.length > 0) {
      dto.forecasts = forecasts.map((f) => ({
        predictedTime: f.predictedTime.toISOString(),
        predictedWaitTime: f.predictedWaitTime,
        confidencePercentage: f.confidencePercentage,
        source: f.source,
      }));
    }

    // Fetch ML predictions (our model - daily, up to 1 year)
    // Only if ML service is available
    try {
      const isHealthy = await this.mlService.isHealthy();
      if (isHealthy) {
        const predictions =
          await this.mlService.getAttractionPredictionsWithFallback(
            attraction.id,
            "hourly",
          );

        dto.hourlyForecast = predictions.map((p) => ({
          predictedTime: p.predictedTime,
          predictedWaitTime: p.predictedWaitTime,
          confidence: p.confidence,
          crowdLevel: p.crowdLevel,
          baseline: p.baseline,
          trend: p.trend || "stable",
        }));
      }
    } catch (error) {
      // ML service not available - log but don't fail
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.warn("ML predictions unavailable:", errorMessage);
      dto.hourlyForecast = [];
    }

    // Fetch attraction statistics
    try {
      const statistics = await this.analyticsService.getAttractionStatistics(
        attraction.id,
      );

      dto.statistics = {
        avgWaitToday: statistics.avgWaitToday,
        peakWaitToday: statistics.peakWaitToday,
        minWaitToday: statistics.minWaitToday,
        typicalWaitThisHour: statistics.typicalWaitThisHour,
        percentile95ThisHour: statistics.percentile95ThisHour,
        currentVsTypical: statistics.currentVsTypical,
        dataPoints: statistics.dataPoints,
        timestamp: statistics.timestamp.toISOString(),
      };
    } catch (error) {
      this.logger.error("Failed to fetch attraction statistics:", error);
      dto.statistics = null;
    }

    // Fetch prediction accuracy
    try {
      const accuracy =
        await this.predictionAccuracyService.getAttractionAccuracyWithBadge(
          attraction.id,
          30, // Last 30 days
        );

      dto.predictionAccuracy = {
        badge: accuracy.badge,
        last30Days: {
          // Only expose counts to public, not technical metrics
          comparedPredictions: accuracy.last30Days.comparedPredictions,
          totalPredictions: accuracy.last30Days.totalPredictions,
        },
        message: accuracy.message,
      };
    } catch (error) {
      // Log error but don't fail - accuracy is optional
      this.logger.warn("Failed to fetch prediction accuracy:", error);
      dto.predictionAccuracy = null;
    }

    // Cache the complete response (5 minutes for real-time freshness)
    await this.redis.set(
      cacheKey,
      JSON.stringify(dto),
      "EX",
      this.TTL_INTEGRATED_RESPONSE,
    );

    return dto;
  }
}
