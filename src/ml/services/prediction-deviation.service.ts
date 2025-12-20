import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between } from "typeorm";
import { WaitTimePrediction } from "../entities/wait-time-prediction.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { Redis } from "ioredis";

/**
 * Prediction Deviation Service
 *
 * Detects when actual wait times deviate significantly from predictions
 * and flags them for API response enrichment.
 *
 * - Thresholds: 10min absolute OR 20% percentage
 * - Redis storage: `prediction:deviation:{attractionId}` (TTL: 1h)
 * - Used for "Confidence Downgrade" strategy (not regeneration)
 */
@Injectable()
export class PredictionDeviationService {
  private readonly logger = new Logger(PredictionDeviationService.name);

  // Deviation thresholds
  private readonly ABSOLUTE_THRESHOLD = 10; // minutes
  private readonly PERCENTAGE_THRESHOLD = 20; // percent

  constructor(
    @InjectRepository(WaitTimePrediction)
    private predictionRepository: Repository<WaitTimePrediction>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Check if actual wait time deviates from prediction
   *
   * @param attractionId - Attraction ID
   * @param actualWaitTime - Current actual wait time (minutes)
   * @returns Deviation info if detected, null otherwise
   */
  async checkDeviation(
    attractionId: string,
    actualWaitTime: number,
  ): Promise<{
    hasDeviation: boolean;
    deviation?: number;
    percentageDeviation?: number;
    predictedWaitTime?: number;
  }> {
    try {
      // Get the latest prediction for current hour
      const prediction =
        await this.getLatestPredictionForCurrentHour(attractionId);

      if (!prediction) {
        // No prediction available - can't compare
        return { hasDeviation: false };
      }

      // Calculate deviations
      const absoluteDeviation = Math.abs(
        actualWaitTime - prediction.predictedWaitTime,
      );
      const percentageDeviation =
        prediction.predictedWaitTime > 0
          ? (absoluteDeviation / prediction.predictedWaitTime) * 100
          : 0;

      // Check thresholds
      const hasDeviation =
        absoluteDeviation > this.ABSOLUTE_THRESHOLD ||
        percentageDeviation > this.PERCENTAGE_THRESHOLD;

      if (hasDeviation) {
        this.logger.debug(
          `Deviation detected for ${attractionId}: ` +
            `predicted=${prediction.predictedWaitTime}min, ` +
            `actual=${actualWaitTime}min, ` +
            `deviation=${absoluteDeviation.toFixed(1)}min (${percentageDeviation.toFixed(1)}%)`,
        );
      }

      return {
        hasDeviation,
        deviation: absoluteDeviation,
        percentageDeviation,
        predictedWaitTime: prediction.predictedWaitTime,
      };
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to check deviation for ${attractionId}: ${errorMessage}`,
      );
      return { hasDeviation: false };
    }
  }

  /**
   * Flag a deviation in Redis for API enrichment
   *
   * @param attractionId - Attraction ID
   * @param metadata - Deviation metadata
   */
  async flagDeviation(
    attractionId: string,
    metadata: {
      actualWaitTime: number;
      predictedWaitTime: number;
      deviation: number;
      percentageDeviation: number;
      detectedAt: Date;
    },
  ): Promise<void> {
    try {
      const key = `prediction:deviation:${attractionId}`;
      const value = JSON.stringify(metadata);

      // Store with 1 hour TTL (matches prediction refresh rate)
      await this.redis.set(key, value, "EX", 3600);

      this.logger.verbose(
        `Flagged deviation for ${attractionId}: ${metadata.deviation.toFixed(1)}min (${metadata.percentageDeviation.toFixed(1)}%)`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to flag deviation for ${attractionId}: ${errorMessage}`,
      );
    }
  }

  /**
   * Get deviation flag from Redis
   *
   * @param attractionId - Attraction ID
   * @returns Deviation metadata if flagged, null otherwise
   */
  async getDeviationFlag(attractionId: string): Promise<{
    actualWaitTime: number;
    predictedWaitTime: number;
    deviation: number;
    percentageDeviation: number;
    detectedAt: string;
  } | null> {
    try {
      const key = `prediction:deviation:${attractionId}`;
      const value = await this.redis.get(key);

      if (!value) {
        return null;
      }

      return JSON.parse(value);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to get deviation flag for ${attractionId}: ${errorMessage}`,
      );
      return null;
    }
  }

  /**
   * Get latest prediction for current hour
   *
   * Finds the most recent prediction that targets the current hour
   *
   * @param attractionId - Attraction ID
   * @returns Latest prediction or null
   */
  private async getLatestPredictionForCurrentHour(
    attractionId: string,
  ): Promise<WaitTimePrediction | null> {
    try {
      // Get current hour window
      const now = new Date();
      const currentHourStart = new Date(now);
      currentHourStart.setMinutes(0, 0, 0);

      const currentHourEnd = new Date(currentHourStart);
      currentHourEnd.setHours(currentHourEnd.getHours() + 1);

      // Find prediction for current hour
      const prediction = await this.predictionRepository.findOne({
        where: {
          attractionId,
          predictionType: "hourly",
          predictedTime: Between(currentHourStart, currentHourEnd),
        },
        order: {
          createdAt: "DESC", // Most recent prediction
        },
      });

      return prediction;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to get prediction for ${attractionId}: ${errorMessage}`,
      );
      return null;
    }
  }

  /**
   * Clear deviation flag (for testing or manual intervention)
   *
   * @param attractionId - Attraction ID
   */
  async clearDeviationFlag(attractionId: string): Promise<void> {
    try {
      const key = `prediction:deviation:${attractionId}`;
      await this.redis.del(key);
      this.logger.verbose(`Cleared deviation flag for ${attractionId}`);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to clear deviation flag for ${attractionId}: ${errorMessage}`,
      );
    }
  }
}
