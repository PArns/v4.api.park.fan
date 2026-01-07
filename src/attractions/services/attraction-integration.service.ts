import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Attraction } from "../entities/attraction.entity";
import { Park } from "../../parks/entities/park.entity";
import { AttractionResponseDto } from "../dto/attraction-response.dto";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { MLService } from "../../ml/ml.service";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";
import { ParksService } from "../../parks/parks.service";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { CrowdLevel } from "../../common/types/crowd-level.type";

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
    private readonly parksService: ParksService,
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
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

    // Fetch current queue data (all queue types)
    // Uses park opening hours to determine valid data cutoff
    // Falls back to 6 hours if no schedule available
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
                  qd.waitTime,
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

      // Extract trend from primary queue (STANDBY or first available)
      const primaryQueue =
        dto.queues?.find((q) => q.queueType === "STANDBY") || dto.queues?.[0];
      if (primaryQueue?.trend?.direction) {
        // Map "increasing" -> "up", "decreasing" -> "down", "stable" -> "stable"
        dto.trend =
          primaryQueue.trend.direction === "increasing"
            ? "up"
            : primaryQueue.trend.direction === "decreasing"
              ? "down"
              : "stable";
      } else {
        dto.trend = null;
      }
    }

    // Check park status and calculate effectiveStatus
    // Attractions inherit park's closed status to prevent showing operating rides when park is closed
    let parkStatus: "OPERATING" | "CLOSED" = "CLOSED";
    if (attraction.parkId) {
      try {
        const statusMap = await this.parksService.getBatchParkStatus([
          attraction.parkId,
        ]);
        parkStatus = statusMap.get(attraction.parkId) || "CLOSED";
      } catch (error) {
        this.logger.warn(
          `Failed to fetch park status for attraction ${attraction.id}:`,
          error,
        );
        // Safe default: assume closed
        parkStatus = "CLOSED";
      }
    }

    // Calculate effective status
    // If park is CLOSED, attraction is effectively CLOSED regardless of queue data
    dto.effectiveStatus = parkStatus === "CLOSED" ? "CLOSED" : dto.status;

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
    let enrichedPredictions: Array<{
      predictedTime: string;
      predictedWaitTime: number;
      confidence: number;
      crowdLevel?: string;
      baseline?: number;
      trend: string;
    }> = [];
    try {
      const isHealthy = await this.mlService.isHealthy();
      if (isHealthy) {
        const predictions =
          await this.mlService.getAttractionPredictionsWithFallback(
            attraction.id,
            "hourly",
          );

        enrichedPredictions = predictions.map((p) => ({
          predictedTime: p.predictedTime,
          predictedWaitTime: p.predictedWaitTime,
          confidence: p.confidence,
          crowdLevel: p.crowdLevel,
          baseline: p.baseline,
          trend: p.trend || "stable",
        }));
        dto.hourlyForecast = enrichedPredictions.map((p) => ({
          predictedTime: p.predictedTime,
          predictedWaitTime: p.predictedWaitTime,
          confidence: p.confidence,
          trend: p.trend,
        }));
      }
    } catch (error) {
      // ML service not available - log but don't fail
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.warn("ML predictions unavailable:", errorMessage);
      dto.hourlyForecast = [];
    }

    // Calculate crowd level
    // Strategy: Use real-time wait time with P90 baseline if available,
    // otherwise fallback to ML prediction crowdLevel
    let crowdLevel: CrowdLevel | "closed" | null = null;
    if (dto.effectiveStatus === "CLOSED") {
      crowdLevel = "closed";
    } else {
      // Find current hour prediction for fallback
      const nowStr = new Date().toISOString().split(":")[0]; // "YYYY-MM-DDTHH"
      const currentPred = enrichedPredictions.find((p) =>
        p.predictedTime.startsWith(nowStr),
      );

      // 1. Try to use REAL-TIME Wait Time first (Ground Truth)
      const wait = dto.queues?.[0]?.waitTime;
      if (wait !== undefined && wait !== null) {
        // Get P90 baseline for relative crowd level (context-aware)
        try {
          const percentiles =
            await this.analyticsService.getAttractionPercentilesToday(
              attraction.id,
            );
          const p90 = percentiles?.p90 || 0;
          const { rating } = this.analyticsService.getLoadRating(wait, p90);
          crowdLevel = rating;
        } catch (error) {
          // If percentile lookup fails, fallback to ML prediction
          this.logger.warn(
            `Failed to get percentiles for crowd level, using fallback:`,
            error,
          );
          if (currentPred?.crowdLevel) {
            crowdLevel = currentPred.crowdLevel as any;
          } else {
            crowdLevel = "very_low";
          }
        }
      } else {
        // 2. Fallback to ML Prediction if no live data
        if (currentPred?.crowdLevel) {
          crowdLevel = currentPred.crowdLevel as any;
        } else {
          // 3. Last resort default
          crowdLevel = "very_low";
        }
      }
    }
    dto.crowdLevel = crowdLevel;

    // Fetch attraction statistics (requires park timezone for accurate daily filtering)
    try {
      // Fetch park entity to get timezone
      const park = await this.parkRepository.findOne({
        where: { id: attraction.parkId },
      });
      if (!park) {
        throw new Error(`Park not found for attraction ${attraction.id}`);
      }

      const startTime = await this.analyticsService.getEffectiveStartTime(
        attraction.parkId,
        park.timezone,
      );

      const statistics = await this.analyticsService.getAttractionStatistics(
        attraction.id,
        startTime,
        park.timezone,
      );

      dto.statistics = {
        avgWaitToday: statistics.avgWaitToday,
        peakWaitToday: statistics.peakWaitToday,
        peakWaitTimestamp: statistics.peakWaitTimestamp
          ? statistics.peakWaitTimestamp.toISOString()
          : null,
        minWaitToday: statistics.minWaitToday,
        typicalWaitThisHour: statistics.typicalWaitThisHour,
        percentile95ThisHour: statistics.percentile95ThisHour,
        currentVsTypical: statistics.currentVsTypical,
        dataPoints: statistics.dataPoints,
        history: statistics.history || [],
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
