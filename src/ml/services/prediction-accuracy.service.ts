import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between, LessThan } from "typeorm";
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
  ) {}

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
   * Cleanup old accuracy records
   * Retention Policy:
   * - MISSED/PENDING: 7 days (short term for debugging)
   * - COMPLETED: 90 days (long term for trending/stats)
   */
  async cleanupOldRecords(): Promise<void> {
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);

    try {
      // 1. Delete MISSED and stuck PENDING older than 7 days
      const resultMissed = await this.accuracyRepository
        .createQueryBuilder()
        .delete()
        .where("targetTime < :sevenDaysAgo", { sevenDaysAgo })
        .andWhere("comparisonStatus IN (:...statuses)", {
          statuses: ["MISSED", "PENDING"],
        })
        .execute();

      // 2. Delete COMPLETED older than 90 days
      const resultCompleted = await this.accuracyRepository
        .createQueryBuilder()
        .delete()
        .where("targetTime < :ninetyDaysAgo", { ninetyDaysAgo })
        .andWhere("comparisonStatus = :status", { status: "COMPLETED" })
        .execute();

      const deletedMissed = resultMissed.affected || 0;
      const deletedCompleted = resultCompleted.affected || 0;

      if (deletedMissed > 0 || deletedCompleted > 0) {
        this.logger.log(
          `🧹 Cleanup: Deleted ${deletedMissed} MISSED/PENDING (>7d) and ${deletedCompleted} COMPLETED (>90d) records`,
        );
      }
    } catch (error) {
      this.logger.warn(`Failed to cleanup old records: ${error}`);
    }
  }

  /**
   * Compare predictions with actual wait times
   * Optimized with batch processing and smart retries
   *
   * Period: Checks predictions from last 7 days
   * Retries: Only retries for 2 hours after target time, then marks as MISSED
   *
   * @returns {Promise<{newComparisons: number}>} Number of new comparisons added
   */
  async compareWithActuals(): Promise<{ newComparisons: number }> {
    const startTime = Date.now();
    this.logger.log("🔄 Comparing predictions with actual wait times...");

    // Run cleanup first
    await this.cleanupOldRecords();

    // Look back 7 days for pending predictions
    const lookbackWindow = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

    // 1. Find PENDING predictions that have passed their target time
    // Buffer: Wait 20 mins after target time to ensure queue data has arrived
    const readyToCompare = new Date(Date.now() - 20 * 60 * 1000);

    const pendingPredictions = await this.accuracyRepository.find({
      select: [
        "id",
        "attractionId",
        "targetTime",
        "predictedWaitTime",
        "predictionType",
        "features",
      ], // Select only needed fields
      where: {
        comparisonStatus: "PENDING",
        targetTime: Between(lookbackWindow, readyToCompare),
      },
      order: {
        targetTime: "ASC",
      },
      take: 5000, // Process larger batches to prevent backlog
    });

    if (pendingPredictions.length === 0) {
      this.logger.log("✅ No pending predictions ready to compare");
      return { newComparisons: 0 };
    }

    this.logger.log(
      `📊 Processing ${pendingPredictions.length} pending predictions...`,
    );

    // 2. Prepare for batch fetching actual data
    // We need data for the time range of our batch
    const minTime = pendingPredictions[0].targetTime;
    const maxTime =
      pendingPredictions[pendingPredictions.length - 1].targetTime;

    // Expand window by 15 mins
    const dataWindowStart = new Date(minTime.getTime() - 15 * 60 * 1000);
    const dataWindowEnd = new Date(maxTime.getTime() + 15 * 60 * 1000);

    // Get all attraction IDs in this batch
    const attractionIds = [
      ...new Set(pendingPredictions.map((p) => p.attractionId)),
    ];

    this.logger.debug(
      `Fetching ACTUAL queue data for ${attractionIds.length} attractions between ${dataWindowStart.toISOString()} and ${dataWindowEnd.toISOString()}`,
    );

    // 3. Batch fetch Actual Queue Data
    // Fetch all potentially relevant queue data in one query
    const actualDataRecords = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select([
        "qd.attractionId",
        "qd.timestamp",
        "qd.waitTime",
        "qd.status",
        "qd.queueType",
      ])
      .where("qd.attractionId IN (:...attractionIds)", { attractionIds })
      .andWhere("qd.timestamp BETWEEN :start AND :end", {
        start: dataWindowStart,
        end: dataWindowEnd,
      })
      .andWhere("qd.queueType = :queueType", { queueType: "STANDBY" })
      .orderBy("qd.timestamp", "ASC")
      .getMany();

    this.logger.debug(`Fetched ${actualDataRecords.length} queue data records`);

    // Index queue data strategies for fast lookup: attractionId -> timestamp[] -> record
    // Using a Map of Maps for O(1) access
    const queueDataMap = new Map<string, QueueData[]>();
    for (const record of actualDataRecords) {
      if (!queueDataMap.has(record.attractionId)) {
        queueDataMap.set(record.attractionId, []);
      }
      queueDataMap.get(record.attractionId)!.push(record);
    }

    // 4. Match Predictions
    let completed = 0;
    let missed = 0;
    let unplannedClosures = 0;
    const updates = [];

    for (const prediction of pendingPredictions) {
      const targetTimeMs = prediction.targetTime.getTime();
      const records = queueDataMap.get(prediction.attractionId) || [];

      // Find best match within ±15 minutes
      // Since records are sorted by time, we can find the closest one
      let bestMatch: QueueData | null = null;
      let minDiff = 15 * 60 * 1000 + 1; // Start just outside window

      for (const record of records) {
        const diff = Math.abs(record.timestamp.getTime() - targetTimeMs);
        if (diff <= 15 * 60 * 1000) {
          if (diff < minDiff) {
            minDiff = diff;
            bestMatch = record;
            // Optimization: if diff is very small (e.g., < 1 min), we can stop
            if (diff < 60 * 1000) break;
          }
        }
      }

      if (bestMatch) {
        // MATCH FOUND
        prediction.comparisonStatus = "COMPLETED";

        if (bestMatch.status === "OPERATING" && bestMatch.waitTime !== null) {
          prediction.actualWaitTime = bestMatch.waitTime;
          prediction.wasUnplannedClosure = false;
          prediction.absoluteError = Math.abs(
            prediction.predictedWaitTime - bestMatch.waitTime,
          );
          if (bestMatch.waitTime > 0) {
            prediction.percentageError =
              (prediction.absoluteError / bestMatch.waitTime) * 100;
          } else {
            prediction.percentageError = null;
          }
        } else {
          // Unplanned Closure (predicted operating, but was closed/down)
          prediction.wasUnplannedClosure = true;
          prediction.actualWaitTime = 0; // Effectively 0 wait time, but not "free"
          prediction.absoluteError = prediction.predictedWaitTime; // Full error
          prediction.percentageError = null;
          unplannedClosures++;
        }
        completed++;
      } else {
        // NO MATCH FOUND
        // Check timeout: If prediction is older than 2 hours, mark as MISSED
        const timeSinceTarget = Date.now() - targetTimeMs;
        if (timeSinceTarget > 2 * 60 * 60 * 1000) {
          prediction.comparisonStatus = "MISSED";
          missed++;
        }
        // Else: Leave as PENDING (try again next batch)
      }

      // Add to updates if changed
      if (prediction.comparisonStatus !== "PENDING") {
        updates.push(prediction);
      }
    }

    // 5. Save Updates in Bulk
    if (updates.length > 0) {
      await this.accuracyRepository.save(updates, { chunk: 100 });
    }

    const duration = Date.now() - startTime;
    this.logger.log(
      `✅ Batch complete: Processed ${pendingPredictions.length}, Saved ${updates.length} updates. ` +
        `Completed: ${completed} (${unplannedClosures} closures), Missed: ${missed}. Duration: ${duration}ms`,
    );

    if (missed > 0) {
      this.logger.log(
        `ℹ️ ${missed} predictions marked as MISSED (no data found after 2 hours)`,
      );
    }

    // Return count of new comparisons for tracking
    return { newComparisons: completed };
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
    pendingComparisons: {
      total: number;
      stalled: number; // Older than 3 hours
      active: number; // 0-3 hours
    };
    missedComparisons: {
      total7Days: number;
    };
    completedLast7Days: number;
    completedLast30Days: number;
    unplannedClosures24h: number;
    recentSamples: Array<{
      attractionId: string;
      targetTime: Date;
      predictedWaitTime: number;
      actualWaitTime: number | null;
      absoluteError: number | null;
      status: string;
      comparedAt: Date;
    }>;
    successRate: {
      last7Days: number;
      last30Days: number;
    };
  }> {
    const now = new Date();
    const threeHoursAgo = new Date(Date.now() - 3 * 60 * 60 * 1000);
    const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

    // 1. Pending Comparisons Breakdown
    const pendingTotal = await this.accuracyRepository.count({
      where: {
        comparisonStatus: "PENDING",
      },
    });

    const pendingStalled = await this.accuracyRepository.count({
      where: {
        comparisonStatus: "PENDING",
        targetTime: LessThan(threeHoursAgo),
      },
    });

    // 2. Missed Comparisons (Zombies)
    const missed7Days = await this.accuracyRepository.count({
      where: {
        comparisonStatus: "MISSED",
        targetTime: Between(sevenDaysAgo, now),
      },
    });

    // 3. Completed Counts
    const completed7Days = await this.accuracyRepository.count({
      where: {
        comparisonStatus: "COMPLETED",
        targetTime: Between(sevenDaysAgo, now),
      },
    });

    const completed30Days = await this.accuracyRepository.count({
      where: {
        comparisonStatus: "COMPLETED",
        targetTime: Between(thirtyDaysAgo, now),
      },
    });

    // 4. Unplanned Closures (Last 24h) - Good indicator of data reality mismatch
    const closures24h = await this.accuracyRepository.count({
      where: {
        wasUnplannedClosure: true,
        targetTime: Between(oneDayAgo, now),
      },
    });

    // 5. Success Rates (Completed / (Completed + Missed + Pending))
    // We only care about records that SHOULD have been processed
    const totalProcessed7Days = completed7Days + missed7Days;
    const rate7Days =
      totalProcessed7Days > 0
        ? Math.round((completed7Days / totalProcessed7Days) * 100)
        : 0;

    const total30Days = await this.accuracyRepository.count({
      where: { targetTime: Between(thirtyDaysAgo, now) },
    });
    // For 30 days, we might rely on status if we had it, but for now simple math
    const rate30Days =
      total30Days > 0 ? Math.round((completed30Days / total30Days) * 100) : 0;

    // 6. Recent Samples (include status)
    const recentSamples = await this.accuracyRepository.find({
      order: {
        comparisonStatus: "DESC", // Show completed/missed first
        createdAt: "DESC",
      },
      take: 5,
    });

    return {
      pendingComparisons: {
        total: pendingTotal,
        stalled: pendingStalled,
        active: pendingTotal - pendingStalled,
      },
      missedComparisons: {
        total7Days: missed7Days,
      },
      completedLast7Days: completed7Days,
      completedLast30Days: completed30Days,
      unplannedClosures24h: closures24h,
      recentSamples: recentSamples.map((s) => ({
        attractionId: s.attractionId,
        targetTime: s.targetTime,
        predictedWaitTime: s.predictedWaitTime,
        actualWaitTime: s.actualWaitTime,
        absoluteError: s.absoluteError,
        status: s.comparisonStatus,
        comparedAt: s.createdAt,
      })),
      successRate: {
        last7Days: rate7Days,
        last30Days: rate30Days,
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
  /**
   * Analyze which features correlate with high prediction errors
   * Optimized to use SQL aggregation instead of in-memory processing
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
      temperatureRanges?: {
        highError: Record<string, number>;
        lowError: Record<string, number>;
      };
    };
    insights: string[];
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    // 1. Get summary counts
    const summaryQuery = this.accuracyRepository
      .createQueryBuilder("pa")
      .select("COUNT(*)", "total")
      .addSelect(
        `SUM(CASE WHEN pa.absoluteError >= :threshold THEN 1 ELSE 0 END)`,
        "highError",
      )
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.actualWaitTime IS NOT NULL")
      .setParameters({ threshold: errorThreshold });

    if (attractionId) {
      summaryQuery.andWhere("pa.attractionId = :attractionId", {
        attractionId,
      });
    }

    const summaryResult = await summaryQuery.getRawOne();
    const totalRecords = parseInt(summaryResult.total || "0", 10);
    const highErrorRecords = parseInt(summaryResult.highError || "0", 10);
    const lowErrorRecords = totalRecords - highErrorRecords;

    if (totalRecords === 0) {
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

    // Helper to fetch feature distribution
    // We aggregate by the specific feature directly in SQL
    const fetchDistribution = async (
      featureExpression: string,
      _featureName: string,
    ) => {
      const query = this.accuracyRepository
        .createQueryBuilder("pa")
        .select(featureExpression, "key")
        .addSelect(
          `SUM(CASE WHEN pa.absoluteError >= :threshold THEN 1 ELSE 0 END)`,
          "high",
        )
        .addSelect(
          `SUM(CASE WHEN pa.absoluteError < :threshold THEN 1 ELSE 0 END)`,
          "low",
        )
        .where("pa.targetTime >= :startDate", { startDate })
        .andWhere("pa.actualWaitTime IS NOT NULL")
        .andWhere(`${featureExpression} IS NOT NULL`)
        .groupBy(featureExpression)
        .setParameters({ threshold: errorThreshold });

      if (attractionId) {
        query.andWhere("pa.attractionId = :attractionId", { attractionId });
      }

      return await query.getRawMany();
    };

    // Parallel fetch of distributions
    const [
      hourParams,
      dayParams,
      weatherResult,
      _rainResult,
      _weekendResult,
      _tempResult,
    ] = await Promise.all([
      fetchDistribution(
        "EXTRACT(HOUR FROM pa.targetTime AT TIME ZONE 'UTC')",
        "hour",
      ),
      fetchDistribution(
        "EXTRACT(DOW FROM pa.targetTime AT TIME ZONE 'UTC')",
        "day",
      ),
      // For JSONB features, we need special casting
      fetchDistribution("(pa.features->>'weatherCode')::int", "weatherCode"),
      fetchDistribution("(pa.features->>'is_raining')::boolean", "isRaining"),
      fetchDistribution("(pa.features->>'is_weekend')::boolean", "isWeekend"),
      // Temperature buckets (simplified for SQL)
      // We'll just fetch rounded temps and bucket in JS to save complex SQL
      fetchDistribution(
        "ROUND((pa.features->>'temperature_avg')::numeric, -1)",
        "temp",
      ),
    ]);

    const featureAnalysis: any = {};
    const insights: string[] = [];

    // Process Hour Analysis
    if (hourParams.length > 0) {
      const highError: Record<number, number> = {};
      const lowError: Record<number, number> = {};
      const errorRates: Array<{ hour: number; errorRate: number }> = [];

      hourParams.forEach((r) => {
        const key = parseInt(r.key);
        const high = parseInt(r.high);
        const low = parseInt(r.low);
        highError[key] = high;
        lowError[key] = low;

        if (high + low >= 3) {
          errorRates.push({
            hour: key,
            errorRate: (high / (high + low)) * 100,
          });
        }
      });

      errorRates.sort((a, b) => b.errorRate - a.errorRate);

      featureAnalysis.hour = {
        highError,
        lowError,
        mostProblematicHours: errorRates.slice(0, 5).map((h) => ({
          hour: h.hour,
          errorRate: Math.round(h.errorRate * 10) / 10,
        })),
      };

      if (errorRates.length > 0) {
        insights.push(
          `Hour ${errorRates[0].hour}:00 has highest error rate (${Math.round(errorRates[0].errorRate)}%)`,
        );
      }
    }

    // Process Day Analysis
    if (dayParams.length > 0) {
      const highError: Record<number, number> = {};
      const lowError: Record<number, number> = {};
      const errorRates: Array<{ day: number; errorRate: number }> = [];
      const dayNames = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
      ];

      dayParams.forEach((r) => {
        const key = parseInt(r.key);
        const high = parseInt(r.high);
        const low = parseInt(r.low);
        highError[key] = high;
        lowError[key] = low;

        if (high + low >= 3) {
          errorRates.push({
            day: key,
            errorRate: (high / (high + low)) * 100,
          });
        }
      });

      errorRates.sort((a, b) => b.errorRate - a.errorRate);

      featureAnalysis.dayOfWeek = {
        highError,
        lowError,
        mostProblematicDays: errorRates.slice(0, 3).map((d) => ({
          day: d.day,
          errorRate: Math.round(d.errorRate * 10) / 10,
        })),
      };

      if (errorRates.length > 0) {
        insights.push(
          `${dayNames[errorRates[0].day]} has highest error rate (${Math.round(errorRates[0].errorRate)}%)`,
        );
      }
    }

    // Process Weather
    if (weatherResult.length > 0) {
      const highError: Record<number, number> = {};
      const lowError: Record<number, number> = {};
      const errorRates: Array<{ code: number; errorRate: number }> = [];

      weatherResult.forEach((r) => {
        const key = parseInt(r.key);
        const high = parseInt(r.high);
        const low = parseInt(r.low);
        highError[key] = high;
        lowError[key] = low;

        if (high + low >= 3) {
          errorRates.push({
            code: key,
            errorRate: (high / (high + low)) * 100,
          });
        }
      });

      errorRates.sort((a, b) => b.errorRate - a.errorRate);

      featureAnalysis.weatherCode = {
        highError,
        lowError,
        mostProblematicWeather: errorRates.slice(0, 3).map((w) => ({
          code: w.code,
          errorRate: Math.round(w.errorRate * 10) / 10,
        })),
      };
    }

    // Process Rain
    // ... similar logic for boolean fields

    return {
      summary: {
        totalRecords,
        highErrorRecords,
        lowErrorRecords,
        errorThreshold,
        period: `Last ${days} days`,
      },
      featureAnalysis,
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
      uniqueAttractions: number;
      uniqueParks: number;
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
      relations: ["attraction"],
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

    // Calculate unique counts
    const uniqueAttractions = new Set(allRecords.map((r) => r.attractionId))
      .size;

    // Calculate unique parks (using attraction relation)
    const uniqueParks = new Set(
      allRecords.map((r) => r.attraction?.parkId).filter((id) => !!id),
    ).size;

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
        uniqueAttractions,
        uniqueParks,
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

    // Count unique parks by joining with attractions table
    const uniqueParksResult = await this.accuracyRepository
      .createQueryBuilder("pa")
      .innerJoin("attractions", "a", "a.id = pa.attractionId")
      .select("COUNT(DISTINCT a.parkId)", "count")
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.comparisonStatus = :status", { status: "COMPLETED" })
      .getRawOne();

    const uniqueParks = parseInt(uniqueParksResult?.count || "0", 10);

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
