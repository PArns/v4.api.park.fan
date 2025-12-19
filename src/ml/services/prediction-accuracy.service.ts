import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between, IsNull } from "typeorm";
import { PredictionAccuracy } from "../entities/prediction-accuracy.entity";
import { WaitTimePrediction } from "../entities/wait-time-prediction.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";

/**
 * PredictionAccuracyService
 *
 * Tracks prediction vs reality for model monitoring and continuous improvement
 * - Records predictions when made
 * - Compares predictions with actual wait times
 * - Calculates error metrics (MAE, RMSE, MAPE)
 * - Provides feedback loop for model retraining
 */
@Injectable()
export class PredictionAccuracyService {
  private readonly logger = new Logger(PredictionAccuracyService.name);

  constructor(
    @InjectRepository(PredictionAccuracy)
    private accuracyRepository: Repository<PredictionAccuracy>,
    @InjectRepository(WaitTimePrediction)
    private predictionRepository: Repository<WaitTimePrediction>,
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
  ) { }

  /**
   * Record prediction for accuracy tracking
   * Called when a prediction is stored in the database
   */
  async recordPrediction(prediction: WaitTimePrediction): Promise<void> {
    const accuracy = new PredictionAccuracy();
    accuracy.attractionId = prediction.attractionId;
    accuracy.predictionTime = prediction.createdAt;
    accuracy.targetTime = prediction.predictedTime;
    accuracy.predictedWaitTime = prediction.predictedWaitTime;
    // Will be filled later by compareWithActuals
    accuracy.modelVersion = prediction.modelVersion;
    accuracy.predictionType = prediction.predictionType;
    accuracy.features = prediction.features;

    await this.accuracyRepository.save(accuracy);
    // Logging handled by caller with progress updates
  }

  /**
   * Compare predictions with actual wait times
   * Run this periodically (e.g., hourly) to update accuracy records
   */
  async compareWithActuals(): Promise<void> {
    const startTime = Date.now();
    this.logger.log("🔄 Comparing predictions with actual wait times...");

    const now = new Date();
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

    // CLEANUP: Delete old predictions that will never be compared
    // These are predictions older than 7 days with no actualWaitTime
    // This prevents table bloat (on live we had 5.2M pending records!)
    try {
      const cleanupResult = await this.accuracyRepository
        .createQueryBuilder()
        .delete()
        .where('targetTime < :sevenDaysAgo', { sevenDaysAgo })
        .andWhere('actualWaitTime IS NULL')
        .execute();

      if (cleanupResult.affected && cleanupResult.affected > 0) {
        this.logger.log(
          `🧹 Cleaned up ${cleanupResult.affected} old predictions (>7 days, never compared)`,
        );
      }
    } catch (error) {
      this.logger.warn('Failed to cleanup old predictions:', error);
      // Continue with comparison even if cleanup fails
    }

    // Find all predictions that have passed but don't have actual wait times yet
    this.logger.debug(
      `Searching for pending comparisons between ${sevenDaysAgo.toISOString()} and ${now.toISOString()}`,
    );

    const pendingAccuracy = await this.accuracyRepository.find({
      where: {
        targetTime: Between(sevenDaysAgo, now),
        actualWaitTime: IsNull(), // Not yet compared
      },
      order: {
        targetTime: "ASC",
      },
      take: 1000, // Process in batches of 1000
    });

    if (pendingAccuracy.length === 0) {
      this.logger.log("✅ No pending predictions to compare");
      return;
    }

    this.logger.log(
      `📊 Found ${pendingAccuracy.length} pending predictions to compare`,
    );
    this.logger.debug(
      `Date range: ${pendingAccuracy[0].targetTime.toISOString()} to ${pendingAccuracy[pendingAccuracy.length - 1].targetTime.toISOString()}`,
    );

    let updated = 0;
    let notFound = 0;
    let closureDetected = 0;

    for (let i = 0; i < pendingAccuracy.length; i++) {
      const accuracy = pendingAccuracy[i];

      // Log first 3 as samples for debugging
      const isSample = i < 3;
      if (isSample) {
        this.logger.debug(
          `Sample ${i + 1}: Checking prediction for attraction ${accuracy.attractionId}, target time ${accuracy.targetTime.toISOString()}, predicted wait: ${accuracy.predictedWaitTime}min`,
        );
      }

      // Find actual wait time at the target time (±15 minutes window)
      const windowStart = new Date(
        accuracy.targetTime.getTime() - 15 * 60 * 1000,
      );
      const windowEnd = new Date(
        accuracy.targetTime.getTime() + 15 * 60 * 1000,
      );

      if (isSample) {
        this.logger.debug(
          `Sample ${i + 1}: Search window ${windowStart.toISOString()} to ${windowEnd.toISOString()}`,
        );
      }

      const actualData = await this.queueDataRepository
        .createQueryBuilder("qd")
        .where("qd.attractionId = :attractionId", {
          attractionId: accuracy.attractionId,
        })
        .andWhere("qd.timestamp BETWEEN :start AND :end", {
          start: windowStart,
          end: windowEnd,
        })
        .andWhere("qd.queueType = :queueType", { queueType: "STANDBY" })
        // REMOVED: Status filter - we want to detect unplanned closures
        .orderBy("qd.timestamp", "ASC")
        .getOne();

      if (actualData && actualData.waitTime !== null) {
        // Normal case: attraction was operating
        accuracy.actualWaitTime = actualData.waitTime;
        accuracy.wasUnplannedClosure = false;

        // Calculate error metrics
        const absoluteError = Math.abs(
          accuracy.predictedWaitTime - actualData.waitTime,
        );
        const percentageError =
          actualData.waitTime > 0
            ? (absoluteError / actualData.waitTime) * 100
            : null;

        accuracy.absoluteError = absoluteError;
        if (percentageError !== null) {
          accuracy.percentageError = percentageError;
        }

        await this.accuracyRepository.save(accuracy);
        updated++;

        if (isSample) {
          this.logger.debug(
            `Sample ${i + 1}: ✅ Match found! Actual wait: ${actualData.waitTime}min, Error: ${absoluteError}min (${Math.round(percentageError || 0)}%)`,
          );
        }
      } else if (actualData && actualData.status !== "OPERATING") {
        // Unplanned closure: We predicted a wait time, but attraction was closed
        // This is a prediction error that we want to track
        accuracy.actualWaitTime = 0;
        accuracy.wasUnplannedClosure = true;
        accuracy.absoluteError = accuracy.predictedWaitTime; // Error = predicted wait time
        accuracy.percentageError = null; // Cannot calculate % error when actual is 0

        await this.accuracyRepository.save(accuracy);
        updated++;
        closureDetected++;

        this.logger.debug(
          `Detected unplanned closure for ${accuracy.attractionId} at ${accuracy.targetTime.toISOString()}`,
        );
      } else {
        notFound++;
        if (isSample) {
          this.logger.debug(
            `Sample ${i + 1}: ⚠️ No matching queue data found in search window`,
          );
        }
      }
    }

    const duration = Date.now() - startTime;
    this.logger.log(
      `✅ Prediction accuracy comparison complete: ${updated} updated (${closureDetected} closures detected), ${notFound} no actual data found. Duration: ${duration}ms`,
    );

    if (notFound > 0) {
      const notFoundPercentage = Math.round((notFound / pendingAccuracy.length) * 100);
      this.logger.warn(
        `⚠️ ${notFoundPercentage}% of predictions had no matching queue data. This may indicate timing issues or missing data.`,
      );
    }
  }

  /**
   * Get prediction accuracy statistics for an attraction
   */
  async getAttractionAccuracyStats(
    attractionId: string,
    days: number = 30,
  ): Promise<{
    totalPredictions: number;
    comparedPredictions: number;
    averageAbsoluteError: number;
    averagePercentageError: number;
    rmse: number;
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const accuracyRecords = await this.accuracyRepository.find({
      where: {
        attractionId,
        targetTime: Between(startDate, new Date()),
      },
    });

    const comparedRecords = accuracyRecords.filter(
      (r) => r.actualWaitTime !== null,
    );

    if (comparedRecords.length === 0) {
      return {
        totalPredictions: accuracyRecords.length,
        comparedPredictions: 0,
        averageAbsoluteError: 0,
        averagePercentageError: 0,
        rmse: 0,
      };
    }

    // Calculate MAE (Mean Absolute Error)
    const mae =
      comparedRecords.reduce((sum, r) => sum + (r.absoluteError || 0), 0) /
      comparedRecords.length;

    // Calculate MAPE (Mean Absolute Percentage Error)
    const validPercentageErrors = comparedRecords.filter(
      (r) => r.percentageError !== null,
    );
    const mape =
      validPercentageErrors.length > 0
        ? validPercentageErrors.reduce(
          (sum, r) => sum + (r.percentageError || 0),
          0,
        ) / validPercentageErrors.length
        : 0;

    // Calculate RMSE (Root Mean Square Error)
    const squaredErrors = comparedRecords.map((r) =>
      Math.pow(r.absoluteError || 0, 2),
    );
    const mse =
      squaredErrors.reduce((sum, sq) => sum + sq, 0) / comparedRecords.length;
    const rmse = Math.sqrt(mse);

    return {
      totalPredictions: accuracyRecords.length,
      comparedPredictions: comparedRecords.length,
      averageAbsoluteError: Math.round(mae * 10) / 10,
      averagePercentageError: Math.round(mape * 10) / 10,
      rmse: Math.round(rmse * 10) / 10,
    };
  }

  /**
   * Get recent prediction vs actual comparisons for display
   */
  async getRecentComparisons(
    attractionId: string,
    limit: number = 50,
  ): Promise<
    Array<{
      targetTime: Date;
      predictedWaitTime: number;
      actualWaitTime: number | null;
      absoluteError: number | null;
      percentageError: number | null;
      modelVersion: string;
    }>
  > {
    const comparisons = await this.accuracyRepository.find({
      where: {
        attractionId,
        actualWaitTime: Between(0, 999), // Only show comparisons with actual data
      },
      order: {
        targetTime: "DESC",
      },
      take: limit,
    });

    return comparisons.map((c) => ({
      targetTime: c.targetTime,
      predictedWaitTime: c.predictedWaitTime,
      actualWaitTime: c.actualWaitTime,
      absoluteError: c.absoluteError,
      percentageError: c.percentageError,
      modelVersion: c.modelVersion,
    }));
  }

  /**
   * Calculate accuracy badge based on MAE
   *
   * Badge criteria:
   * - excellent: MAE < 5 min (very accurate)
   * - good: MAE < 10 min (reliable for planning)
   * - fair: MAE < 15 min (somewhat useful)
   * - poor: MAE >= 15 min (needs improvement)
   * - insufficient_data: < 10 compared predictions
   */
  calculateAccuracyBadge(
    mae: number,
    comparedPredictions: number,
  ): {
    badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
    message?: string;
  } {
    // Need at least 10 comparisons for meaningful badge
    if (comparedPredictions < 10) {
      return {
        badge: "insufficient_data",
        message: `Need at least 10 compared predictions (currently ${comparedPredictions})`,
      };
    }

    if (mae < 5) {
      return {
        badge: "excellent",
        message: "Predictions are highly accurate (±5 min average error)",
      };
    } else if (mae < 10) {
      return {
        badge: "good",
        message:
          "Predictions are reliable for planning (±10 min average error)",
      };
    } else if (mae < 15) {
      return {
        badge: "fair",
        message: "Predictions provide general guidance (±15 min average error)",
      };
    } else {
      return {
        badge: "poor",
        message: `Predictions need improvement (${Math.round(mae)} min average error)`,
      };
    }
  }

  /**
   * Get health status of the prediction accuracy system
   * Useful for diagnosing if compareWithActuals is running properly
   */
  async getHealthStatus(): Promise<{
    pendingComparisons: number;
    completedLast7Days: number;
    completedLast30Days: number;
    recentSamples: Array<{
      attractionId: string;
      targetTime: Date;
      predictedWaitTime: number;
      actualWaitTime: number | null;
      absoluteError: number | null;
      comparedAt: Date;
    }>;
    successRate: {
      last7Days: number;
      last30Days: number;
    };
  }> {
    const now = new Date();
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

    // Count pending comparisons (predictions with null actualWaitTime)
    const pendingCount = await this.accuracyRepository.count({
      where: {
        targetTime: Between(sevenDaysAgo, now),
        actualWaitTime: IsNull(),
      },
    });

    // Count completed comparisons in last 7 days
    const completed7Days = await this.accuracyRepository
      .createQueryBuilder("pa")
      .where("pa.targetTime >= :sevenDaysAgo", { sevenDaysAgo })
      .andWhere("pa.targetTime <= :now", { now })
      .andWhere("pa.actualWaitTime IS NOT NULL")
      .getCount();

    // Count completed comparisons in last 30 days
    const completed30Days = await this.accuracyRepository
      .createQueryBuilder("pa")
      .where("pa.targetTime >= :thirtyDaysAgo", { thirtyDaysAgo })
      .andWhere("pa.targetTime <= :now", { now })
      .andWhere("pa.actualWaitTime IS NOT NULL")
      .getCount();

    // Get total predictions (for success rate calculation)
    const total7Days = await this.accuracyRepository.count({
      where: {
        targetTime: Between(sevenDaysAgo, now),
      },
    });

    const total30Days = await this.accuracyRepository.count({
      where: {
        targetTime: Between(thirtyDaysAgo, now),
      },
    });

    // Get recent completed comparisons as samples
    const recentSamples = await this.accuracyRepository.find({
      where: {
        actualWaitTime: Between(0, 999), // Not null
      },
      order: {
        createdAt: "DESC",
      },
      take: 5,
    });

    return {
      pendingComparisons: pendingCount,
      completedLast7Days: completed7Days,
      completedLast30Days: completed30Days,
      recentSamples: recentSamples.map((s) => ({
        attractionId: s.attractionId,
        targetTime: s.targetTime,
        predictedWaitTime: s.predictedWaitTime,
        actualWaitTime: s.actualWaitTime,
        absoluteError: s.absoluteError,
        comparedAt: s.createdAt,
      })),
      successRate: {
        last7Days:
          total7Days > 0
            ? Math.round((completed7Days / total7Days) * 100)
            : 0,
        last30Days:
          total30Days > 0
            ? Math.round((completed30Days / total30Days) * 100)
            : 0,
      },
    };
  }

  /**
   * Get prediction accuracy with badge for display in API
   */
  async getAttractionAccuracyWithBadge(
    attractionId: string,
    days: number = 30,
  ): Promise<{
    badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
    last30Days: {
      mae: number;
      mape: number;
      rmse: number;
      comparedPredictions: number;
      totalPredictions: number;
    };
    message?: string;
  }> {
    const stats = await this.getAttractionAccuracyStats(attractionId, days);
    const badgeInfo = this.calculateAccuracyBadge(
      stats.averageAbsoluteError,
      stats.comparedPredictions,
    );

    return {
      badge: badgeInfo.badge,
      last30Days: {
        mae: stats.averageAbsoluteError,
        mape: stats.averagePercentageError,
        rmse: stats.rmse,
        comparedPredictions: stats.comparedPredictions,
        totalPredictions: stats.totalPredictions,
      },
      message: badgeInfo.message,
    };
  }

  /**
   * Analyze which features correlate with high prediction errors
   *
   * Helps identify:
   * - Which hours are hardest to predict
   * - Which weather conditions cause issues
   * - Which days of week have higher errors
   * - Other feature patterns in error cases
   *
   * @param errorThreshold - Absolute error threshold (default: 15 min)
   * @param days - Days to look back (default: 30)
   * @returns Feature breakdown for high vs low error predictions
   */
  async analyzeFeatureErrors(
    errorThreshold: number = 15,
    days: number = 30,
    attractionId?: string,
  ): Promise<{
    summary: {
      totalRecords: number;
      highErrorRecords: number;
      lowErrorRecords: number;
      errorThreshold: number;
      period: string;
    };
    featureAnalysis: {
      hour?: {
        highError: Record<number, number>;
        lowError: Record<number, number>;
        mostProblematicHours: Array<{ hour: number; errorRate: number }>;
      };
      dayOfWeek?: {
        highError: Record<number, number>;
        lowError: Record<number, number>;
        mostProblematicDays: Array<{ day: number; errorRate: number }>;
      };
      isWeekend?: {
        highError: { true: number; false: number };
        lowError: { true: number; false: number };
      };
      weatherCode?: {
        highError: Record<number, number>;
        lowError: Record<number, number>;
        mostProblematicWeather: Array<{ code: number; errorRate: number }>;
      };
      isRaining?: {
        highError: { true: number; false: number };
        lowError: { true: number; false: number };
      };
      isParkOpen?: {
        highError: { true: number; false: number };
        lowError: { true: number; false: number };
      };
      temperatureRanges?: {
        highError: Record<string, number>;
        lowError: Record<string, number>;
      };
    };
    insights: string[];
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    // Build query
    const queryBuilder = this.accuracyRepository
      .createQueryBuilder("pa")
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.actualWaitTime IS NOT NULL")
      .andWhere("pa.absoluteError IS NOT NULL")
      .andWhere("pa.features IS NOT NULL");

    if (attractionId) {
      queryBuilder.andWhere("pa.attractionId = :attractionId", {
        attractionId,
      });
    }

    const records = await queryBuilder.getMany();

    if (records.length === 0) {
      return {
        summary: {
          totalRecords: 0,
          highErrorRecords: 0,
          lowErrorRecords: 0,
          errorThreshold,
          period: `Last ${days} days`,
        },
        featureAnalysis: {},
        insights: ["No data available for analysis"],
      };
    }

    // Separate high vs low error records
    const highErrorRecords = records.filter(
      (r) => (r.absoluteError || 0) >= errorThreshold,
    );
    const lowErrorRecords = records.filter(
      (r) => (r.absoluteError || 0) < errorThreshold,
    );

    // Initialize analysis object
    const analysis: {
      hour?: {
        highError: Record<number, number>;
        lowError: Record<number, number>;
        mostProblematicHours: Array<{ hour: number; errorRate: number }>;
      };
      dayOfWeek?: {
        highError: Record<number, number>;
        lowError: Record<number, number>;
        mostProblematicDays: Array<{ day: number; errorRate: number }>;
      };
      isWeekend?: {
        highError: { true: number; false: number };
        lowError: { true: number; false: number };
      };
      weatherCode?: {
        highError: Record<number, number>;
        lowError: Record<number, number>;
        mostProblematicWeather: Array<{ code: number; errorRate: number }>;
      };
      isRaining?: {
        highError: { true: number; false: number };
        lowError: { true: number; false: number };
      };
      isParkOpen?: {
        highError: { true: number; false: number };
        lowError: { true: number; false: number };
      };
      temperatureRanges?: {
        highError: Record<string, number>;
        lowError: Record<string, number>;
      };
    } = {};

    const insights: string[] = [];

    // Helper to safely extract feature value
    const getFeature = (
      features: Record<string, unknown>,
      key: string,
    ): unknown => {
      return features?.[key];
    };

    // Analyze hour distribution
    const hourHigh: Record<number, number> = {};
    const hourLow: Record<number, number> = {};
    highErrorRecords.forEach((r) => {
      const hour = getFeature(r.features, "hour");
      if (typeof hour === "number") {
        hourHigh[hour] = (hourHigh[hour] || 0) + 1;
      }
    });
    lowErrorRecords.forEach((r) => {
      const hour = getFeature(r.features, "hour");
      if (typeof hour === "number") {
        hourLow[hour] = (hourLow[hour] || 0) + 1;
      }
    });

    if (Object.keys(hourHigh).length > 0) {
      // Calculate error rates per hour
      const hourErrorRates = Object.keys(hourHigh)
        .map((h) => {
          const hour = parseInt(h);
          const highCount = hourHigh[hour] || 0;
          const lowCount = hourLow[hour] || 0;
          const total = highCount + lowCount;
          const errorRate = total > 0 ? (highCount / total) * 100 : 0;
          return { hour, errorRate, total };
        })
        .filter((h) => h.total >= 3) // Only include hours with enough data
        .sort((a, b) => b.errorRate - a.errorRate);

      analysis.hour = {
        highError: hourHigh,
        lowError: hourLow,
        mostProblematicHours: hourErrorRates.slice(0, 5).map((h) => ({
          hour: h.hour,
          errorRate: Math.round(h.errorRate * 10) / 10,
        })),
      };

      if (hourErrorRates.length > 0) {
        const worstHour = hourErrorRates[0];
        insights.push(
          `Hour ${worstHour.hour}:00 has highest error rate (${Math.round(worstHour.errorRate)}%)`,
        );
      }
    }

    // Analyze day of week distribution
    const dayHigh: Record<number, number> = {};
    const dayLow: Record<number, number> = {};
    highErrorRecords.forEach((r) => {
      const day = getFeature(r.features, "day_of_week");
      if (typeof day === "number") {
        dayHigh[day] = (dayHigh[day] || 0) + 1;
      }
    });
    lowErrorRecords.forEach((r) => {
      const day = getFeature(r.features, "day_of_week");
      if (typeof day === "number") {
        dayLow[day] = (dayLow[day] || 0) + 1;
      }
    });

    if (Object.keys(dayHigh).length > 0) {
      const dayErrorRates = Object.keys(dayHigh)
        .map((d) => {
          const day = parseInt(d);
          const highCount = dayHigh[day] || 0;
          const lowCount = dayLow[day] || 0;
          const total = highCount + lowCount;
          const errorRate = total > 0 ? (highCount / total) * 100 : 0;
          return { day, errorRate, total };
        })
        .filter((d) => d.total >= 3)
        .sort((a, b) => b.errorRate - a.errorRate);

      analysis.dayOfWeek = {
        highError: dayHigh,
        lowError: dayLow,
        mostProblematicDays: dayErrorRates.slice(0, 3).map((d) => ({
          day: d.day,
          errorRate: Math.round(d.errorRate * 10) / 10,
        })),
      };

      if (dayErrorRates.length > 0) {
        const dayNames = [
          "Sunday",
          "Monday",
          "Tuesday",
          "Wednesday",
          "Thursday",
          "Friday",
          "Saturday",
        ];
        const worstDay = dayErrorRates[0];
        insights.push(
          `${dayNames[worstDay.day]} has highest error rate (${Math.round(worstDay.errorRate)}%)`,
        );
      }
    }

    // Analyze weekend vs weekday
    let weekendHigh = { true: 0, false: 0 };
    let weekendLow = { true: 0, false: 0 };
    highErrorRecords.forEach((r) => {
      const isWeekend = getFeature(r.features, "is_weekend");
      if (typeof isWeekend === "boolean") {
        weekendHigh[isWeekend ? "true" : "false"]++;
      }
    });
    lowErrorRecords.forEach((r) => {
      const isWeekend = getFeature(r.features, "is_weekend");
      if (typeof isWeekend === "boolean") {
        weekendLow[isWeekend ? "true" : "false"]++;
      }
    });

    if (weekendHigh.true + weekendHigh.false > 0) {
      analysis.isWeekend = { highError: weekendHigh, lowError: weekendLow };

      const weekendErrorRate =
        (weekendHigh.true / (weekendHigh.true + weekendLow.true || 1)) * 100;
      const weekdayErrorRate =
        (weekendHigh.false / (weekendHigh.false + weekendLow.false || 1)) * 100;

      if (Math.abs(weekendErrorRate - weekdayErrorRate) > 10) {
        if (weekendErrorRate > weekdayErrorRate) {
          insights.push(
            `Weekends are harder to predict (${Math.round(weekendErrorRate)}% vs ${Math.round(weekdayErrorRate)}% error rate)`,
          );
        } else {
          insights.push(
            `Weekdays are harder to predict (${Math.round(weekdayErrorRate)}% vs ${Math.round(weekendErrorRate)}% error rate)`,
          );
        }
      }
    }

    // Analyze weather code distribution
    const weatherHigh: Record<number, number> = {};
    const weatherLow: Record<number, number> = {};
    highErrorRecords.forEach((r) => {
      const code = getFeature(r.features, "weatherCode");
      if (typeof code === "number") {
        weatherHigh[code] = (weatherHigh[code] || 0) + 1;
      }
    });
    lowErrorRecords.forEach((r) => {
      const code = getFeature(r.features, "weatherCode");
      if (typeof code === "number") {
        weatherLow[code] = (weatherLow[code] || 0) + 1;
      }
    });

    if (Object.keys(weatherHigh).length > 0) {
      const weatherErrorRates = Object.keys(weatherHigh)
        .map((w) => {
          const code = parseInt(w);
          const highCount = weatherHigh[code] || 0;
          const lowCount = weatherLow[code] || 0;
          const total = highCount + lowCount;
          const errorRate = total > 0 ? (highCount / total) * 100 : 0;
          return { code, errorRate, total };
        })
        .filter((w) => w.total >= 3)
        .sort((a, b) => b.errorRate - a.errorRate);

      analysis.weatherCode = {
        highError: weatherHigh,
        lowError: weatherLow,
        mostProblematicWeather: weatherErrorRates.slice(0, 3).map((w) => ({
          code: w.code,
          errorRate: Math.round(w.errorRate * 10) / 10,
        })),
      };
    }

    // Analyze rain impact
    let rainHigh = { true: 0, false: 0 };
    let rainLow = { true: 0, false: 0 };
    highErrorRecords.forEach((r) => {
      const isRaining = getFeature(r.features, "is_raining");
      if (typeof isRaining === "boolean") {
        rainHigh[isRaining ? "true" : "false"]++;
      }
    });
    lowErrorRecords.forEach((r) => {
      const isRaining = getFeature(r.features, "is_raining");
      if (typeof isRaining === "boolean") {
        rainLow[isRaining ? "true" : "false"]++;
      }
    });

    if (rainHigh.true + rainHigh.false > 0) {
      analysis.isRaining = { highError: rainHigh, lowError: rainLow };

      const rainErrorRate =
        (rainHigh.true / (rainHigh.true + rainLow.true || 1)) * 100;
      const noRainErrorRate =
        (rainHigh.false / (rainHigh.false + rainLow.false || 1)) * 100;

      if (Math.abs(rainErrorRate - noRainErrorRate) > 15) {
        if (rainErrorRate > noRainErrorRate) {
          insights.push(
            `Rainy conditions are harder to predict (${Math.round(rainErrorRate)}% vs ${Math.round(noRainErrorRate)}% error rate)`,
          );
        }
      }
    }

    // Analyze park open/closed impact
    let parkOpenHigh = { true: 0, false: 0 };
    let parkOpenLow = { true: 0, false: 0 };
    highErrorRecords.forEach((r) => {
      const isParkOpen = getFeature(r.features, "is_park_open");
      if (typeof isParkOpen === "boolean") {
        parkOpenHigh[isParkOpen ? "true" : "false"]++;
      }
    });
    lowErrorRecords.forEach((r) => {
      const isParkOpen = getFeature(r.features, "is_park_open");
      if (typeof isParkOpen === "boolean") {
        parkOpenLow[isParkOpen ? "true" : "false"]++;
      }
    });

    if (parkOpenHigh.true + parkOpenHigh.false > 0) {
      analysis.isParkOpen = { highError: parkOpenHigh, lowError: parkOpenLow };
    }

    // Analyze temperature ranges
    const tempRangeHigh: Record<string, number> = {};
    const tempRangeLow: Record<string, number> = {};
    const getTempRange = (temp: number): string => {
      if (temp < 0) return "freezing (<0°C)";
      if (temp < 10) return "cold (0-10°C)";
      if (temp < 20) return "moderate (10-20°C)";
      if (temp < 30) return "warm (20-30°C)";
      return "hot (>30°C)";
    };

    highErrorRecords.forEach((r) => {
      const temp = getFeature(r.features, "temperature_avg");
      if (typeof temp === "number") {
        const range = getTempRange(temp);
        tempRangeHigh[range] = (tempRangeHigh[range] || 0) + 1;
      }
    });
    lowErrorRecords.forEach((r) => {
      const temp = getFeature(r.features, "temperature_avg");
      if (typeof temp === "number") {
        const range = getTempRange(temp);
        tempRangeLow[range] = (tempRangeLow[range] || 0) + 1;
      }
    });

    if (Object.keys(tempRangeHigh).length > 0) {
      analysis.temperatureRanges = {
        highError: tempRangeHigh,
        lowError: tempRangeLow,
      };
    }

    // Add general insights
    if (insights.length === 0) {
      insights.push("No significant patterns found in error distribution");
    }

    return {
      summary: {
        totalRecords: records.length,
        highErrorRecords: highErrorRecords.length,
        lowErrorRecords: lowErrorRecords.length,
        errorThreshold,
        period: `Last ${days} days`,
      },
      featureAnalysis: analysis,
      insights,
    };
  }

  /**
   * Get system-wide accuracy statistics (all attractions aggregated)
   *
   * Provides comprehensive system-level metrics:
   * - Overall MAE, RMSE, MAPE, R² across all attractions
   * - Breakdown by prediction type (HOURLY vs DAILY)
   * - Coverage percentage (predictions matched vs total)
   *
   * @param {number} days - Number of days to analyze (default: 7)
   * @returns {Promise<SystemAccuracyStats>} System-wide accuracy statistics
   */
  async getSystemAccuracyStats(days: number = 7): Promise<{
    overall: {
      mae: number;
      rmse: number;
      mape: number;
      r2Score: number;
      totalPredictions: number;
      matchedPredictions: number;
      coveragePercent: number;
    };
    byPredictionType: {
      HOURLY: {
        mae: number;
        totalPredictions: number;
        coveragePercent: number;
      };
      DAILY: { mae: number; totalPredictions: number; coveragePercent: number };
    };
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    // Get all accuracy records in time range
    const allRecords = await this.accuracyRepository.find({
      where: {
        targetTime: Between(startDate, new Date()),
      },
    });

    const matchedRecords = allRecords.filter((r) => r.actualWaitTime !== null);

    // Overall metrics
    const overall = this.calculateMetricsFromRecords(matchedRecords);
    const coveragePercent =
      allRecords.length > 0
        ? parseFloat(
          ((matchedRecords.length / allRecords.length) * 100).toFixed(1),
        )
        : 0;

    // Breakdown by prediction type
    const hourlyRecords = matchedRecords.filter(
      (r) => r.predictionType === "hourly",
    );
    const dailyRecords = matchedRecords.filter(
      (r) => r.predictionType === "daily",
    );

    const hourlyAll = allRecords.filter((r) => r.predictionType === "hourly");
    const dailyAll = allRecords.filter((r) => r.predictionType === "daily");

    this.logger.debug(
      `System accuracy stats (${days} days): MAE ${overall.mae} min, coverage ${coveragePercent}%`,
    );

    return {
      overall: {
        ...overall,
        totalPredictions: allRecords.length,
        matchedPredictions: matchedRecords.length,
        coveragePercent,
      },
      byPredictionType: {
        HOURLY: {
          mae: this.calculateMAE(hourlyRecords),
          totalPredictions: hourlyAll.length,
          coveragePercent:
            hourlyAll.length > 0
              ? parseFloat(
                ((hourlyRecords.length / hourlyAll.length) * 100).toFixed(1),
              )
              : 0,
        },
        DAILY: {
          mae: this.calculateMAE(dailyRecords),
          totalPredictions: dailyAll.length,
          coveragePercent:
            dailyAll.length > 0
              ? parseFloat(
                ((dailyRecords.length / dailyAll.length) * 100).toFixed(1),
              )
              : 0,
        },
      },
    };
  }

  /**
   * Get top and bottom performing attractions
   *
   * Identifies which attractions have the most/least accurate predictions
   * Useful for understanding which ride types or park areas are hardest to predict
   *
   * Requires at least 10 predictions per attraction to be included
   *
   * @param {number} days - Number of days to analyze (default: 7)
   * @param {number} limit - Number of attractions to return per category (default: 5)
   * @returns {Promise<TopBottomPerformers>} Top and bottom performers
   */
  async getTopBottomPerformers(
    days: number = 7,
    limit: number = 5,
  ): Promise<{
    topPerformers: Array<{
      attractionId: string;
      attractionName: string;
      parkName: string;
      mae: number;
      predictionsCount: number;
    }>;
    bottomPerformers: Array<{
      attractionId: string;
      attractionName: string;
      parkName: string;
      mae: number;
      predictionsCount: number;
    }>;
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    // Query with JOIN to get attraction and park names
    const results = await this.accuracyRepository
      .createQueryBuilder("pa")
      .leftJoin("attractions", "a", "pa.attractionId = a.id")
      .leftJoin("parks", "p", "a.parkId = p.id")
      .select("pa.attractionId", "attractionId")
      .addSelect("a.name", "attractionName")
      .addSelect("p.name", "parkName")
      .addSelect("AVG(pa.absoluteError)", "mae")
      .addSelect("COUNT(*)", "predictionsCount")
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.actualWaitTime IS NOT NULL")
      .groupBy("pa.attractionId")
      .addGroupBy("a.name")
      .addGroupBy("p.name")
      .having("COUNT(*) >= :minPredictions", { minPredictions: 10 })
      .getRawMany();

    // Sort by MAE (ascending = best performers)
    const sorted = results
      .map((r) => ({
        attractionId: r.attractionId,
        attractionName: r.attractionName || "Unknown",
        parkName: r.parkName || "Unknown",
        mae: parseFloat(parseFloat(r.mae).toFixed(1)),
        predictionsCount: parseInt(r.predictionsCount, 10),
      }))
      .sort((a, b) => a.mae - b.mae);

    this.logger.debug(
      `Top/bottom performers: ${sorted.length} attractions with ≥10 predictions`,
    );

    return {
      topPerformers: sorted.slice(0, limit),
      bottomPerformers: sorted.slice(-limit).reverse(),
    };
  }

  /**
   * Get daily accuracy trends
   *
   * Shows how prediction accuracy varies over time (last N days)
   * Useful for identifying if the model is improving or degrading
   *
   * @param {number} days - Number of days to analyze (default: 30)
   * @returns {Promise<DailyAccuracyTrend[]>} Daily accuracy breakdown
   */
  async getDailyAccuracyTrends(days: number = 30): Promise<
    Array<{
      date: string;
      mae: number;
      predictionsCount: number;
      coveragePercent: number;
    }>
  > {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    // Aggregate by date (GROUP BY)
    const results = await this.accuracyRepository
      .createQueryBuilder("pa")
      .select("DATE(pa.targetTime AT TIME ZONE 'UTC')", "date")
      .addSelect("AVG(pa.absoluteError)", "mae")
      .addSelect("COUNT(*)", "totalCount")
      .addSelect(
        "COUNT(CASE WHEN pa.actualWaitTime IS NOT NULL THEN 1 END)",
        "matchedCount",
      )
      .where("pa.targetTime >= :startDate", { startDate })
      .groupBy("DATE(pa.targetTime AT TIME ZONE 'UTC')")
      .orderBy("date", "DESC")
      .getRawMany();

    const trends = results.map((r) => ({
      date: r.date,
      mae: r.mae ? parseFloat(parseFloat(r.mae).toFixed(1)) : 0,
      predictionsCount: parseInt(r.totalCount, 10),
      coveragePercent:
        parseInt(r.totalCount, 10) > 0
          ? parseFloat(
            (
              (parseInt(r.matchedCount, 10) / parseInt(r.totalCount, 10)) *
              100
            ).toFixed(1),
          )
          : 0,
    }));

    this.logger.debug(`Daily accuracy trends: ${trends.length} days`);
    return trends;
  }

  /**
   * Get hourly accuracy patterns
   *
   * Identifies which hours of the day are hardest to predict
   * Useful for understanding if morning/afternoon/evening predictions differ
   *
   * @param {number} days - Number of days to analyze (default: 30)
   * @returns {Promise<HourlyAccuracyPattern[]>} Accuracy by hour (0-23)
   */
  async getHourlyAccuracyPatterns(days: number = 30): Promise<
    Array<{
      hour: number;
      mae: number;
      predictionsCount: number;
    }>
  > {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const results = await this.accuracyRepository
      .createQueryBuilder("pa")
      .select("EXTRACT(HOUR FROM pa.targetTime AT TIME ZONE 'UTC')", "hour")
      .addSelect("AVG(pa.absoluteError)", "mae")
      .addSelect("COUNT(*)", "predictionsCount")
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.actualWaitTime IS NOT NULL")
      .groupBy("EXTRACT(HOUR FROM pa.targetTime AT TIME ZONE 'UTC')")
      .orderBy("hour", "ASC")
      .getRawMany();

    const patterns = results.map((r) => ({
      hour: parseInt(r.hour, 10),
      mae: parseFloat(parseFloat(r.mae).toFixed(1)),
      predictionsCount: parseInt(r.predictionsCount, 10),
    }));

    this.logger.debug(`Hourly patterns: ${patterns.length} hours analyzed`);
    return patterns;
  }

  /**
   * Get day-of-week accuracy patterns
   *
   * Shows if weekends vs weekdays have different prediction accuracy
   * Useful for identifying if crowd patterns on certain days are harder to predict
   *
   * @param {number} days - Number of days to analyze (default: 30)
   * @returns {Promise<DayOfWeekAccuracyPattern[]>} Accuracy by day of week
   */
  async getDayOfWeekAccuracyPatterns(days: number = 30): Promise<
    Array<{
      dayOfWeek: number;
      dayName: string;
      mae: number;
      predictionsCount: number;
    }>
  > {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const results = await this.accuracyRepository
      .createQueryBuilder("pa")
      .select(
        "EXTRACT(DOW FROM pa.targetTime AT TIME ZONE 'UTC')",
        "day_of_week",
      )
      .addSelect("AVG(pa.absoluteError)", "mae")
      .addSelect("COUNT(*)", "predictionsCount")
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.actualWaitTime IS NOT NULL")
      .groupBy("EXTRACT(DOW FROM pa.targetTime AT TIME ZONE 'UTC')")
      .orderBy("day_of_week", "ASC")
      .getRawMany();

    const dayNames = [
      "Sunday",
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
    ];

    const patterns = results.map((r) => ({
      dayOfWeek: parseInt(r.day_of_week, 10),
      dayName: dayNames[parseInt(r.day_of_week, 10)],
      mae: parseFloat(parseFloat(r.mae).toFixed(1)),
      predictionsCount: parseInt(r.predictionsCount, 10),
    }));

    this.logger.debug(`Day-of-week patterns: ${patterns.length} days analyzed`);
    return patterns;
  }

  /**
   * Helper: Calculate metrics from PredictionAccuracy records
   *
   * Computes MAE, RMSE, MAPE, and R² from an array of matched predictions
   *
   * @private
   * @param {PredictionAccuracy[]} records - Array of prediction accuracy records
   * @returns {{ mae: number; rmse: number; mape: number; r2Score: number }} Calculated metrics
   */
  private calculateMetricsFromRecords(records: PredictionAccuracy[]): {
    mae: number;
    rmse: number;
    mape: number;
    r2Score: number;
  } {
    if (records.length === 0) {
      return { mae: 0, rmse: 0, mape: 0, r2Score: 0 };
    }

    // MAE (Mean Absolute Error)
    const mae =
      records.reduce((sum, r) => sum + (r.absoluteError || 0), 0) /
      records.length;

    // RMSE (Root Mean Square Error)
    const squaredErrors = records.map((r) => Math.pow(r.absoluteError || 0, 2));
    const mse = squaredErrors.reduce((sum, sq) => sum + sq, 0) / records.length;
    const rmse = Math.sqrt(mse);

    // MAPE (Mean Absolute Percentage Error)
    const validPercentageErrors = records.filter(
      (r) => r.percentageError !== null,
    );
    const mape =
      validPercentageErrors.length > 0
        ? validPercentageErrors.reduce(
          (sum, r) => sum + (r.percentageError || 0),
          0,
        ) / validPercentageErrors.length
        : 0;

    // R² (Coefficient of Determination)
    const actualValues = records.map((r) => r.actualWaitTime || 0);
    const _predictedValues = records.map((r) => r.predictedWaitTime);
    const meanActual =
      actualValues.reduce((sum, val) => sum + val, 0) / actualValues.length;

    const ssTot = actualValues.reduce(
      (sum, val) => sum + Math.pow(val - meanActual, 2),
      0,
    );
    const ssRes = records.reduce(
      (sum, r) =>
        sum + Math.pow((r.actualWaitTime || 0) - r.predictedWaitTime, 2),
      0,
    );

    const r2Score = ssTot > 0 ? 1 - ssRes / ssTot : 0;

    return {
      mae: parseFloat(mae.toFixed(1)),
      rmse: parseFloat(rmse.toFixed(1)),
      mape: parseFloat(mape.toFixed(1)),
      r2Score: parseFloat(r2Score.toFixed(2)),
    };
  }

  /**
   * Helper: Calculate MAE from PredictionAccuracy records
   *
   * Simpler version of calculateMetricsFromRecords when only MAE is needed
   *
   * @private
   * @param {PredictionAccuracy[]} records - Array of prediction accuracy records
   * @returns {number} Mean Absolute Error
   */
  private calculateMAE(records: PredictionAccuracy[]): number {
    if (records.length === 0) return 0;
    const mae =
      records.reduce((sum, r) => sum + (r.absoluteError || 0), 0) /
      records.length;
    return parseFloat(mae.toFixed(1));
  }

  /**
   * Get global accuracy statistics (all parks and attractions)
   */
  async getGlobalAccuracyStats(days: number = 30): Promise<{
    totalPredictions: number;
    comparedPredictions: number;
    averageAbsoluteError: number;
    averagePercentageError: number;
    rmse: number;
    uniqueAttractions: number;
    uniqueParks: number;
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const accuracyRecords = await this.accuracyRepository.find({
      where: {
        targetTime: Between(startDate, new Date()),
      },
      relations: ["attraction"],
    });

    const comparedRecords = accuracyRecords.filter(
      (r) => r.actualWaitTime !== null,
    );

    if (comparedRecords.length === 0) {
      return {
        totalPredictions: accuracyRecords.length,
        comparedPredictions: 0,
        averageAbsoluteError: 0,
        averagePercentageError: 0,
        rmse: 0,
        uniqueAttractions: 0,
        uniqueParks: 0,
      };
    }

    // Calculate MAE
    const mae =
      comparedRecords.reduce((sum, r) => sum + (r.absoluteError || 0), 0) /
      comparedRecords.length;

    // Calculate MAPE
    const validPercentageErrors = comparedRecords.filter(
      (r) => r.percentageError !== null,
    );
    const mape =
      validPercentageErrors.length > 0
        ? validPercentageErrors.reduce(
          (sum, r) => sum + (r.percentageError || 0),
          0,
        ) / validPercentageErrors.length
        : 0;

    // Calculate RMSE
    const squaredErrors = comparedRecords.map((r) =>
      Math.pow(r.absoluteError || 0, 2),
    );
    const mse =
      squaredErrors.reduce((sum, sq) => sum + sq, 0) / comparedRecords.length;
    const rmse = Math.sqrt(mse);

    // Count unique attractions and parks
    const uniqueAttractions = new Set(
      accuracyRecords.map((r) => r.attractionId),
    ).size;

    // For parks, we'd need to join with attractions table, but let's estimate for now
    const uniqueParks = 0; // TODO: Implement if needed

    return {
      totalPredictions: accuracyRecords.length,
      comparedPredictions: comparedRecords.length,
      averageAbsoluteError: Math.round(mae * 10) / 10,
      averagePercentageError: Math.round(mape * 10) / 10,
      rmse: Math.round(rmse * 10) / 10,
      uniqueAttractions,
      uniqueParks,
    };
  }

  /**
   * Get park-wide accuracy statistics (all attractions in park averaged)
   */
  async getParkAccuracyStats(
    parkId: string,
    days: number = 30,
  ): Promise<{
    totalPredictions: number;
    comparedPredictions: number;
    averageAbsoluteError: number;
    averagePercentageError: number;
    rmse: number;
    attractionsCount: number;
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    // Get all predictions for attractions in this park
    const accuracyRecords = await this.accuracyRepository
      .createQueryBuilder("pa")
      .innerJoin("attractions", "a", "pa.attractionId = a.id")
      .where("a.parkId = :parkId", { parkId })
      .andWhere("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.targetTime <= :endDate", { endDate: new Date() })
      .select([
        "pa.id",
        "pa.attractionId",
        "pa.targetTime",
        "pa.predictedWaitTime",
        "pa.actualWaitTime",
        "pa.absoluteError",
        "pa.percentageError",
      ])
      .getMany();

    const comparedRecords = accuracyRecords.filter(
      (r) => r.actualWaitTime !== null,
    );

    if (comparedRecords.length === 0) {
      return {
        totalPredictions: accuracyRecords.length,
        comparedPredictions: 0,
        averageAbsoluteError: 0,
        averagePercentageError: 0,
        rmse: 0,
        attractionsCount: 0,
      };
    }

    // Calculate MAE
    const mae =
      comparedRecords.reduce((sum, r) => sum + (r.absoluteError || 0), 0) /
      comparedRecords.length;

    // Calculate MAPE
    const validPercentageErrors = comparedRecords.filter(
      (r) => r.percentageError !== null,
    );
    const mape =
      validPercentageErrors.length > 0
        ? validPercentageErrors.reduce(
          (sum, r) => sum + (r.percentageError || 0),
          0,
        ) / validPercentageErrors.length
        : 0;

    // Calculate RMSE
    const squaredErrors = comparedRecords.map((r) =>
      Math.pow(r.absoluteError || 0, 2),
    );
    const mse =
      squaredErrors.reduce((sum, sq) => sum + sq, 0) / comparedRecords.length;
    const rmse = Math.sqrt(mse);

    // Count unique attractions
    const attractionsCount = new Set(accuracyRecords.map((r) => r.attractionId))
      .size;

    return {
      totalPredictions: accuracyRecords.length,
      comparedPredictions: comparedRecords.length,
      averageAbsoluteError: Math.round(mae * 10) / 10,
      averagePercentageError: Math.round(mape * 10) / 10,
      rmse: Math.round(rmse * 10) / 10,
      attractionsCount,
    };
  }

  /**
   * Check if model retraining is needed based on accuracy degradation
   *
   * Triggers retraining if:
   * 1. MAE > 15 minutes (poor accuracy)
   * 2. Coverage < 80% (not enough predictions matching actuals)
   * 3. MAPE > 35% (high percentage error)
   *
   * @param {number} days - Days to analyze (default: 7)
   * @returns {Promise<{needed: boolean; reason?: string; metrics: any}>}
   */
  async checkRetrainingNeeded(days: number = 7): Promise<{
    needed: boolean;
    reason?: string;
    metrics: any;
  }> {
    try {
      const stats = await this.getSystemAccuracyStats(days);

      // Thresholds
      const MAE_THRESHOLD = 15; // minutes
      const COVERAGE_THRESHOLD = 80; // percent
      const MAPE_THRESHOLD = 35; // percent

      // Check conditions
      if (stats.overall.mae > MAE_THRESHOLD) {
        this.logger.warn(
          `⚠️  Retraining recommended: MAE ${stats.overall.mae.toFixed(1)} > ${MAE_THRESHOLD} min`,
        );
        return {
          needed: true,
          reason: `accuracy_degradation`,
          metrics: stats.overall,
        };
      }

      if (stats.overall.coveragePercent < COVERAGE_THRESHOLD) {
        this.logger.warn(
          `⚠️  Retraining recommended: Coverage ${stats.overall.coveragePercent}% < ${COVERAGE_THRESHOLD}%`,
        );
        return {
          needed: true,
          reason: `low_coverage`,
          metrics: stats.overall,
        };
      }

      if (stats.overall.mape > MAPE_THRESHOLD) {
        this.logger.warn(
          `⚠️  Retraining recommended: MAPE ${stats.overall.mape.toFixed(1)}% > ${MAPE_THRESHOLD}%`,
        );
        return {
          needed: true,
          reason: `high_percentage_error`,
          metrics: stats.overall,
        };
      }

      // All good
      this.logger.debug(
        `✅ Model performance acceptable: MAE ${stats.overall.mae.toFixed(1)} min, Coverage ${stats.overall.coveragePercent}%`,
      );
      return {
        needed: false,
        metrics: stats.overall,
      };
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`Failed to check retraining need: ${errorMessage}`);
      return {
        needed: false,
        metrics: null,
      };
    }
  }
}
