import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between } from "typeorm";
import { WaitTimePrediction } from "../entities/wait-time-prediction.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { Redis } from "ioredis";
import {
  PCN_MAX_FORECAST_AGE_H,
  servePcnIntraday,
} from "../pcn-serving.constants";

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

      // Champion-swap consistency: deviation must be measured against the wait the
      // user actually SEES. When PCN serves intraday, the read paths override the
      // stored CatBoost row with PCN's freshest q0.5 — comparing live waits against
      // the hidden CatBoost number would flag deviations on values nobody is shown.
      const servedWait =
        (await this.getServedPcnWait(attractionId, prediction.predictedTime)) ??
        prediction.predictedWaitTime;

      // Calculate deviations
      const absoluteDeviation = Math.abs(actualWaitTime - servedWait);
      const percentageDeviation =
        servedWait > 0 ? (absoluteDeviation / servedWait) * 100 : 0;

      // Check thresholds
      const hasDeviation =
        absoluteDeviation > this.ABSOLUTE_THRESHOLD ||
        percentageDeviation > this.PERCENTAGE_THRESHOLD;

      if (hasDeviation) {
        this.logger.debug(
          `Deviation detected for ${attractionId}: ` +
            `predicted=${servedWait}min, ` +
            `actual=${actualWaitTime}min, ` +
            `deviation=${absoluteDeviation.toFixed(1)}min (${percentageDeviation.toFixed(1)}%)`,
        );
      }

      return {
        hasDeviation,
        deviation: absoluteDeviation,
        percentageDeviation,
        predictedWaitTime: servedWait,
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
   * Get deviation flags for multiple attractions in a single Redis call
   * OPTIMIZED: Uses MGET instead of N individual GET calls
   *
   * @param attractionIds - Array of attraction IDs
   * @returns Map of attractionId -> deviation flag info
   */
  async getBatchDeviationFlags(attractionIds: string[]): Promise<
    Map<
      string,
      {
        actualWaitTime: number;
        predictedWaitTime: number;
        deviation: number;
        percentageDeviation: number;
        detectedAt: string;
      }
    >
  > {
    const resultMap = new Map<
      string,
      {
        actualWaitTime: number;
        predictedWaitTime: number;
        deviation: number;
        percentageDeviation: number;
        detectedAt: string;
      }
    >();

    if (attractionIds.length === 0) {
      return resultMap;
    }

    try {
      const keys = attractionIds.map((id) => `prediction:deviation:${id}`);
      const values = await this.redis.mget(...keys);

      attractionIds.forEach((id, index) => {
        const value = values[index];
        if (value) {
          try {
            resultMap.set(id, JSON.parse(value));
          } catch (_parseError) {
            // Skip malformed entries
          }
        }
      });
    } catch (error) {
      this.logger.warn(`Failed to batch fetch deviation flags:`, error);
    }

    return resultMap;
  }

  /**
   * The wait the serving layer actually shows for this slot when the PCN
   * champion-swap is live: PCN's freshest forward q0.5 within the shared staleness
   * guard (same selection as MLService.getPcnIntradayWaits). Returns null when the
   * flag is off, PCN has no fresh forecast, or the table is absent — the caller
   * falls back to the stored CatBoost value (exactly like the read paths do).
   */
  private async getServedPcnWait(
    attractionId: string,
    predictedTime: Date,
  ): Promise<number | null> {
    if (!servePcnIntraday()) return null;
    try {
      const rows: Array<{ wait: string }> =
        await this.predictionRepository.manager.query(
          `SELECT f.predicted_wait AS wait
             FROM pcn_forecasts f
             JOIN attractions a ON a.id = f.attraction_id
             JOIN parks p ON p.id = a."parkId"
            WHERE f.attraction_id = $1::uuid
              AND f.quantile = 0.5
              AND f.created_at >= now() - ($3 || ' hours')::interval
              AND (f.target_slot AT TIME ZONE p.timezone) = $2
            ORDER BY f.origin_slot DESC
            LIMIT 1`,
          [attractionId, predictedTime, PCN_MAX_FORECAST_AGE_H],
        );
      return rows.length > 0 ? Math.round(Number(rows[0].wait)) : null;
    } catch {
      // Missing table / transient DB error → CatBoost fallback, never a hard fail.
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
