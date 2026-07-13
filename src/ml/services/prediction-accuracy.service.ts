import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between, LessThan } from "typeorm";
import { PredictionAccuracy } from "../entities/prediction-accuracy.entity";
import { AttractionAccuracyStats } from "../entities/attraction-accuracy-stats.entity";
import { WaitTimePrediction } from "../entities/wait-time-prediction.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { MAX_PLAUSIBLE_WAIT_TIME } from "../../common/utils/wait-time.utils";
import { safeJsonParse } from "../../common/utils/json.util";

/** Per-prediction-type accuracy breakdown. mae/coveragePercent are null when the
 *  type is not scored against actuals (`tracked: false`) — e.g. daily predictions,
 *  which span up to 365 days and are never compared, so 0% would read as broken. */
type TypeBreakdown = {
  mae: number | null;
  totalPredictions: number;
  coveragePercent: number | null;
  tracked: boolean;
};

/** Accuracy badge payload served by /attractions/:id/accuracy (Redis-cached). */
export type AccuracyBadgeResult = {
  badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
  last30Days: {
    mae: number;
    mape: number;
    rmse: number;
    comparedPredictions: number;
    totalPredictions: number;
  };
  message?: string;
};

/**
 * Served-intraday accuracy — the value users ACTUALLY get for 15-min slots.
 *
 * The champion-swap serves the PCN model (q0.5) for intraday, but
 * `prediction_accuracy` only ever stores CatBoost predictions — so
 * `getSystemAccuracyStats().byPredictionType.HOURLY` measures the CatBoost
 * *fallback*, not the served product (docs/ml/pcn-intraday-review.md §6a). The
 * PCN shadow board (`pcn_intraday_comparisons`) already scores the served value
 * vs actuals on a matched (attraction, 15-min slot) population, with CatBoost
 * scored on the same population for a fair head-to-head. `null` when the board
 * table is absent (fresh DB / PCN not deployed) or no PCN rows exist in-window
 * (override inactive → CatBoost IS the served intraday model).
 */
export type ServedIntradayAccuracy = {
  servedModel: "pcn";
  mae: number;
  n: number;
  catboostMae: number | null;
  /** catboostMae − mae; > 0 ⇒ the served model beats the CatBoost fallback. */
  delta: number | null;
  days: number;
};

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

  // aggregate-stats runs hourly → badge data changes at most once per hour.
  // 30min TTL keeps Redis fresh within one aggregate cycle.
  private readonly TTL_ACCURACY_BADGE = 30 * 60;

  // Read-through TTL for the system-wide accuracy aggregations (system stats + hour/DOW
  // patterns), shared by the dashboard AND the standalone /accuracy/* endpoints. 7–30d
  // rolling numbers that barely move intraday. 2h, NOT 30min: the admin monitor polls
  // these ~hourly, so a sub-poll TTL missed on essentially every poll (the 07-13 review
  // still saw ~17–23 slow hits/day per aggregation) — the cache only helped the dashboard
  // path. A TTL comfortably ABOVE the poll interval is what actually lands the hits.
  private readonly TTL_ACCURACY_AGG = 2 * 60 * 60;

  constructor(
    @InjectRepository(PredictionAccuracy)
    private accuracyRepository: Repository<PredictionAccuracy>,
    @InjectRepository(AttractionAccuracyStats)
    private statsRepository: Repository<AttractionAccuracyStats>,
    @InjectRepository(WaitTimePrediction)
    private predictionRepository: Repository<WaitTimePrediction>,
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Read-through Redis cache for an expensive aggregation. Cache miss → run compute();
   * a throw propagates WITHOUT caching, so a transient DB failure doesn't get stuck.
   */
  private async cachedAgg<T>(
    key: string,
    ttlSec: number,
    compute: () => Promise<T>,
  ): Promise<T> {
    const cached = safeJsonParse<T>(
      await this.redis.get(key).catch(() => null),
    );
    if (cached !== null && cached !== undefined) return cached;
    const value = await compute();
    await this.redis
      .set(key, JSON.stringify(value), "EX", ttlSec)
      .catch(() => undefined);
    return value;
  }

  /**
   * Record predictions for accuracy tracking in a few multi-row upserts
   * instead of one round-trip per prediction.
   *
   * Upsert: one record per (attractionId, targetTime).
   * Each prediction run re-covers the same future slots — without this,
   * 15-min prediction cycles create ~8x duplicates per slot, inflating
   * MAE stats and sample weights. Keep the latest prediction_time (most
   * recent forecast is most accurate due to updated real-time features).
   *
   * Why: the per-row loop fired N single-row `ON CONFLICT` upserts against the
   * large unique index, which under the concurrent comparison job's bulk UPDATE
   * showed up as multi-second lock waits. Batching collapses N round-trips into
   * ceil(N / CHUNK) statements.
   *
   * `synchronous_commit = off` (LOCAL to this transaction) is Postgres' closest
   * thing to a "delayed insert": the commit returns without waiting for the WAL
   * fsync. Safe here because accuracy rows are regenerable telemetry — losing
   * the last few hundred ms of them on a crash has no correctness impact.
   *
   * @returns the number of predictions written
   */
  async recordPredictions(predictions: WaitTimePrediction[]): Promise<number> {
    if (!predictions || predictions.length === 0) return 0;

    const rows = predictions.map((prediction) => {
      const accuracy = new PredictionAccuracy();
      accuracy.attractionId = prediction.attractionId;
      accuracy.predictionTime = prediction.createdAt;
      accuracy.targetTime = prediction.predictedTime;
      accuracy.predictedWaitTime = prediction.predictedWaitTime;
      accuracy.modelVersion = prediction.modelVersion;
      accuracy.predictionType = prediction.predictionType;
      accuracy.features = prediction.features;
      return accuracy;
    });

    // Keep each statement well under Postgres' 65535-parameter limit
    // (~8 columns per row → ~8k row ceiling; 1000 leaves ample headroom).
    const CHUNK = 1000;

    await this.accuracyRepository.manager.transaction(async (em) => {
      await em.query("SET LOCAL synchronous_commit = off");
      const repo = em.getRepository(PredictionAccuracy);
      for (let i = 0; i < rows.length; i += CHUNK) {
        await repo.upsert(rows.slice(i, i + CHUNK) as any, {
          conflictPaths: ["attractionId", "targetTime"],
          skipUpdateIfNoValuesChanged: false,
        });
      }
    });

    return rows.length;
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
      take: 10000, // Increased batch size for faster backlog processing
    });

    if (pendingPredictions.length === 0) {
      this.logger.log("✅ No pending predictions ready to compare");
      await this.recordAccuracyCheck(0);
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

    // Expand window by 30 mins
    const dataWindowStart = new Date(minTime.getTime() - 30 * 60 * 1000);
    const dataWindowEnd = new Date(maxTime.getTime() + 30 * 60 * 1000);

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

      // Find best match within ±30 minutes
      // Since records are sorted by time, we can find the closest one
      let bestMatch: QueueData | null = null;
      let minDiff = 30 * 60 * 1000 + 1; // Start just outside window

      for (const record of records) {
        const diff = Math.abs(record.timestamp.getTime() - targetTimeMs);
        if (diff <= 30 * 60 * 1000) {
          if (diff < minDiff) {
            minDiff = diff;
            bestMatch = record;
            // Optimization: if diff is very small (e.g., < 1 min), we can stop
            if (diff < 60 * 1000) break;
          }
        }
      }

      if (bestMatch) {
        if (bestMatch.status === "OPERATING" && bestMatch.waitTime !== null) {
          // MATCH FOUND — comparable: ride was operating with a real wait value
          prediction.comparisonStatus = "COMPLETED";
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
          completed++;
        } else if (bestMatch.status === "OPERATING") {
          // Ride was OPEN but the source reported NO wait value (waitTime=null).
          // This is NOT a closure — status-only parks (e.g. Chimelong and many
          // Asian/water parks) report OPERATING without ever publishing a queue
          // length. Counting it as an unplanned closure with full error (the old
          // behaviour) wrongly inflated MAE and dragged "verified coverage" down
          // (~37% of all "unplanned closures" were actually open-but-null rides).
          // There is no ground truth to compare against, so leave it uncompared:
          // PENDING until it times out, then MISSED (a genuine data gap), never
          // a fabricated 0-wait COMPLETED row.
          const timeSinceTarget = Date.now() - targetTimeMs;
          if (timeSinceTarget > 2 * 60 * 60 * 1000) {
            prediction.comparisonStatus = "MISSED";
            missed++;
          }
        } else {
          // Genuine unplanned closure: predicted operating, but ride was
          // CLOSED / DOWN / REFURBISHMENT at the target time.
          prediction.comparisonStatus = "COMPLETED";
          prediction.wasUnplannedClosure = true;
          prediction.actualWaitTime = 0; // Effectively 0 wait time, but not "free"
          prediction.absoluteError = prediction.predictedWaitTime; // Full error
          prediction.percentageError = null;
          unplannedClosures++;
          completed++;
        }
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

    // 5. Save Updates in Bulk — two SQL statements instead of N individual UPDATEs
    if (updates.length > 0) {
      const missed = updates.filter((u) => u.comparisonStatus === "MISSED");
      const completed = updates.filter(
        (u) => u.comparisonStatus === "COMPLETED",
      );

      // MISSED: only status changes — single UPDATE ... WHERE id = ANY(...)
      if (missed.length > 0) {
        await this.accuracyRepository.query(
          `UPDATE prediction_accuracy SET comparison_status = 'MISSED' WHERE id = ANY($1)`,
          [missed.map((u) => u.id)],
        );
      }

      // COMPLETED: bulk UPDATE via unnest (1 round-trip for all rows)
      if (completed.length > 0) {
        await this.accuracyRepository.query(
          `UPDATE prediction_accuracy AS pa
           SET
             comparison_status    = 'COMPLETED',
             actual_wait_time     = u.actual_wait_time::int,
             absolute_error       = u.absolute_error::int,
             percentage_error     = u.percentage_error::float,
             "wasUnplannedClosure" = u.was_unplanned_closure::boolean
           FROM unnest(
             $1::uuid[],
             $2::int[],
             $3::int[],
             $4::float[],
             $5::boolean[]
           ) AS u(id, actual_wait_time, absolute_error, percentage_error, was_unplanned_closure)
           WHERE pa.id = u.id`,
          [
            completed.map((u) => u.id),
            completed.map((u) => u.actualWaitTime ?? 0),
            completed.map((u) => u.absoluteError ?? 0),
            completed.map((u) => u.percentageError ?? null),
            completed.map((u) => u.wasUnplannedClosure ?? false),
          ],
        );
      }
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
    await this.recordAccuracyCheck(completed);
    return { newComparisons: completed };
  }

  /**
   * Persist the "last accuracy check" marker the ML dashboard reads
   * (MlDashboardService.getLastAccuracyCheck). Without it the dashboard always
   * fabricated "checked just now, 0 new" because nothing ever wrote the key.
   */
  private async recordAccuracyCheck(
    newComparisonsAdded: number,
  ): Promise<void> {
    await this.redis
      .set(
        "ml:last-accuracy-check",
        JSON.stringify({
          completedAt: new Date().toISOString(),
          newComparisonsAdded,
        }),
      )
      .catch(() => undefined);
  }

  /**
   * Get prediction accuracy statistics for an attraction
   *
   * Uses a single SQL aggregate query instead of loading all rows into memory.
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

    const [result] = await this.accuracyRepository.query(
      `
      SELECT
        COUNT(*) AS total_predictions,
        COUNT(CASE WHEN comparison_status = 'COMPLETED'
                    AND actual_wait_time >= 5
                    AND actual_wait_time <= $3
                    AND "wasUnplannedClosure" = false THEN 1 END) AS compared_predictions,
        AVG(absolute_error)
          FILTER (WHERE comparison_status = 'COMPLETED'
                    AND actual_wait_time >= 5
                    AND actual_wait_time <= $3
                    AND "wasUnplannedClosure" = false) AS mae,
        AVG(percentage_error)
          FILTER (WHERE comparison_status = 'COMPLETED'
                    AND actual_wait_time >= 5
                    AND actual_wait_time <= $3
                    AND "wasUnplannedClosure" = false
                    AND percentage_error IS NOT NULL) AS mape,
        SQRT(AVG(absolute_error * absolute_error)
          FILTER (WHERE comparison_status = 'COMPLETED'
                    AND actual_wait_time >= 5
                    AND actual_wait_time <= $3
                    AND "wasUnplannedClosure" = false)) AS rmse
      FROM prediction_accuracy
      WHERE attraction_id = $1
        AND target_time >= $2
      `,
      [attractionId, startDate, MAX_PLAUSIBLE_WAIT_TIME],
    );

    const comparedPredictions = parseInt(result.compared_predictions) || 0;

    if (comparedPredictions === 0) {
      return {
        totalPredictions: parseInt(result.total_predictions) || 0,
        comparedPredictions: 0,
        averageAbsoluteError: 0,
        averagePercentageError: 0,
        rmse: 0,
      };
    }

    return {
      totalPredictions: parseInt(result.total_predictions) || 0,
      comparedPredictions,
      averageAbsoluteError: Math.round((parseFloat(result.mae) || 0) * 10) / 10,
      averagePercentageError:
        Math.round((parseFloat(result.mape) || 0) * 10) / 10,
      rmse: Math.round((parseFloat(result.rmse) || 0) * 10) / 10,
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
        // Upper bound drops data-source sentinels (700/999/1000) so the
        // comparison list shows real waits, not "closed/unavailable" codes.
        actualWaitTime: Between(0, MAX_PLAUSIBLE_WAIT_TIME),
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
   * Get prediction accuracy with badge for display in API.
   *
   * 3-tier lookup:
   *   L1 — Redis (30min TTL): avoids DB on repeated requests
   *   L2 — attraction_accuracy_stats: pre-aggregated by hourly cron, single PK lookup
   *   L3 — prediction_accuracy (aggregate SQL): fallback for new rides not yet aggregated
   */
  async getAttractionAccuracyWithBadge(
    attractionId: string,
    days: number = 30,
  ): Promise<AccuracyBadgeResult> {
    // L1: Redis cache (safeJsonParse: corrupt entry = miss, fall to L2)
    const cacheKey = `accuracy:badge:${attractionId}:${days}d`;
    const cached = safeJsonParse<AccuracyBadgeResult>(
      await this.redis.get(cacheKey),
    );
    if (cached) return cached;

    // L2: pre-aggregated table (updated hourly by aggregate-stats cron)
    const preAggregated = await this.statsRepository.findOne({
      where: { attractionId },
    });

    let result: {
      badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
      last30Days: {
        mae: number;
        mape: number;
        rmse: number;
        comparedPredictions: number;
        totalPredictions: number;
      };
      message?: string;
    };

    if (preAggregated) {
      result = {
        badge: preAggregated.badge,
        last30Days: {
          mae: preAggregated.mae ?? 0,
          mape: 0, // not stored in pre-aggregated table
          rmse: 0, // not stored in pre-aggregated table
          comparedPredictions: preAggregated.comparedPredictions,
          totalPredictions: preAggregated.totalPredictions,
        },
        message: preAggregated.message ?? undefined,
      };
    } else {
      // L3: raw aggregate SQL fallback for rides not yet in attraction_accuracy_stats
      const stats = await this.getAttractionAccuracyStats(attractionId, days);
      const badgeInfo = this.calculateAccuracyBadge(
        stats.averageAbsoluteError,
        stats.comparedPredictions,
      );
      result = {
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

    await this.redis.set(
      cacheKey,
      JSON.stringify(result),
      "EX",
      this.TTL_ACCURACY_BADGE,
    );
    return result;
  }

  /**
   * Get prediction accuracy with badge for multiple attractions in a single query
   * OPTIMIZED: Uses single SQL query with IN clause instead of N individual queries
   *
   * @param attractionIds - Array of attraction IDs
   * @param days - Number of days to look back (default: 30)
   * @returns Map of attractionId -> accuracy badge info
   */
  async getBatchAttractionAccuracy(
    attractionIds: string[],
    days: number = 30,
  ): Promise<
    Map<
      string,
      {
        badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
        last30Days: {
          mae: number;
          comparedPredictions: number;
          totalPredictions: number;
        };
        message?: string;
      }
    >
  > {
    const resultMap = new Map<
      string,
      {
        badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
        last30Days: {
          mae: number;
          comparedPredictions: number;
          totalPredictions: number;
        };
        message?: string;
      }
    >();

    if (attractionIds.length === 0) {
      return resultMap;
    }

    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    try {
      // Single SQL query with aggregation for all attractions
      const results = await this.accuracyRepository.query(
        `
        SELECT 
          attraction_id as "attractionId",
          COUNT(*) as "totalPredictions",
          COUNT(CASE WHEN actual_wait_time >= 5 AND actual_wait_time <= $3 AND "wasUnplannedClosure" = false THEN 1 END) as "comparedPredictions",
          COALESCE(AVG(absolute_error) FILTER (WHERE actual_wait_time >= 5 AND actual_wait_time <= $3 AND "wasUnplannedClosure" = false), 0) as "mae"
        FROM prediction_accuracy
        WHERE attraction_id = ANY($1)
          AND target_time >= $2
        GROUP BY attraction_id
        `,
        [attractionIds, startDate, MAX_PLAUSIBLE_WAIT_TIME],
      );

      // Process results
      for (const row of results) {
        const mae = parseFloat(row.mae) || 0;
        const comparedPredictions = parseInt(row.comparedPredictions) || 0;
        const totalPredictions = parseInt(row.totalPredictions) || 0;

        const badgeInfo = this.calculateAccuracyBadge(mae, comparedPredictions);

        resultMap.set(row.attractionId, {
          badge: badgeInfo.badge,
          last30Days: {
            mae,
            comparedPredictions,
            totalPredictions,
          },
          message: badgeInfo.message,
        });
      }

      // Set insufficient_data for attractions with no records
      for (const id of attractionIds) {
        if (!resultMap.has(id)) {
          resultMap.set(id, {
            badge: "insufficient_data",
            last30Days: {
              mae: 0,
              comparedPredictions: 0,
              totalPredictions: 0,
            },
            message: "Need at least 10 compared predictions (currently 0)",
          });
        }
      }
    } catch (error) {
      this.logger.warn(`Failed to batch fetch accuracy:`, error);
      // Fallback: set insufficient_data for all
      for (const id of attractionIds) {
        resultMap.set(id, {
          badge: "insufficient_data",
          last30Days: {
            mae: 0,
            comparedPredictions: 0,
            totalPredictions: 0,
          },
          message: "Error fetching accuracy data",
        });
      }
    }

    return resultMap;
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

    const featureAnalysis: {
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
      weatherCode?: {
        highError: Record<number, number>;
        lowError: Record<number, number>;
        mostProblematicWeather: Array<{ code: number; errorRate: number }>;
      };
      [key: string]: unknown;
    } = {};
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
  async getSystemAccuracyStats(days: number = 7) {
    return this.cachedAgg(`accuracy:sys:${days}`, this.TTL_ACCURACY_AGG, () =>
      this.computeSystemAccuracyStats(days),
    );
  }

  private async computeSystemAccuracyStats(days: number = 7): Promise<{
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
      HOURLY: TypeBreakdown;
      DAILY: TypeBreakdown;
    };
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    this.logger.debug(
      `Calculating system accuracy stats for last ${days} days (SQL Optimized)`,
    );

    // 1. Get Overall Basic Counts
    const totalPredictions = await this.accuracyRepository.count({
      where: { targetTime: Between(startDate, new Date()) },
    });

    // "Verified coverage" = predictions whose outcome we actually checked against
    // real operating data (comparison_status='COMPLETED'), over all predictions
    // whose target time has passed. This is intentionally SEPARATE from the
    // MAE-eligible `matchedCount` below: the old coverage number reused
    // matchedCount, which excludes rides that were closed at the target time AND
    // every slot whose actual wait was <5 min — so it conflated "did we verify
    // this?" with "was the actual a meaningful non-trivial wait?", reporting ~54%
    // when we had in fact checked ~80%+ of predictions against ground truth. A
    // ride being closed (or quiet) when we looked IS a verification of coverage;
    // it just isn't scored into MAE. Genuine data gaps (no sample found, or
    // status-only parks that never publish a wait → MISSED) correctly stay out.
    const completedPredictions = await this.accuracyRepository.count({
      where: {
        targetTime: Between(startDate, new Date()),
        comparisonStatus: "COMPLETED",
      },
    });

    // 2. Get Aggregated Metrics for Matched Predictions
    // Calculate sums needed for MAE, RMSE, MAPE, R2
    // IMPROVEMENT: We now include wait times >= 0 in coverage calculation
    // BUT only for UNKNOWN or OPERATING schedules (as requested by user).
    // Explicitly CLOSED parks often send "0" by default which would fake verification.
    const statsResult = await this.accuracyRepository
      .createQueryBuilder("pa")
      .innerJoin("attractions", "a", 'a.id = pa."attraction_id"')
      // Thin-data gate: only aggregate ratable parks (typical-day-peak baseline
      // present, ≥ 30 operating days). The INNER JOIN drops thin-park rows so
      // the reported MAE/coverage excludes them — same flag as ML training.
      .innerJoin(
        "park_p50_baselines",
        "pb",
        'pb."parkId" = a."parkId" AND pb."typicalDayPeak" IS NOT NULL',
      )
      .leftJoin(
        "schedule_entries",
        "se",
        'se."parkId" = a."parkId" AND se.date = DATE(pa.target_time) AND se."attractionId" IS NULL',
      )
      .select("COUNT(*)", "matchedCount")
      .addSelect("AVG(pa.absolute_error)", "mae")
      .addSelect(
        "AVG(CASE WHEN pa.actual_wait_time >= 10 THEN pa.percentage_error ELSE NULL END)",
        "mape",
      )
      .addSelect("SUM(POWER(pa.absolute_error, 2))", "sumSqError")
      .addSelect("SUM(pa.actual_wait_time)", "sumActual")
      .addSelect("SUM(POWER(pa.actual_wait_time, 2))", "sumSqActual")
      .where("pa.target_time >= :startDate", { startDate })
      .andWhere("pa.actual_wait_time IS NOT NULL")
      .andWhere(
        '(pa.actual_wait_time >= 5 OR (pa.actual_wait_time >= 0 AND (se."scheduleType" IS NULL OR se."scheduleType" != \'CLOSED\')))',
      )
      .andWhere("pa.actual_wait_time <= :maxWait", {
        maxWait: MAX_PLAUSIBLE_WAIT_TIME,
      })
      .andWhere('pa."wasUnplannedClosure" = false') // Exclude random closures
      .getRawOne();
    const matchedCount = parseInt(statsResult.matchedCount || "0", 10);
    const mae = parseFloat(parseFloat(statsResult.mae || "0").toFixed(1));
    const mape = parseFloat(parseFloat(statsResult.mape || "0").toFixed(1));

    // RMSE Calculation
    const sumSqError = parseFloat(statsResult.sumSqError || "0");
    const rmse =
      matchedCount > 0
        ? parseFloat(Math.sqrt(sumSqError / matchedCount).toFixed(1))
        : 0;

    // R2 Calculation
    // R2 = 1 - (SSres / SStot)
    // SSres = sumSqError
    // SStot = sumSqActual - (sumActual^2 / N)
    let r2Score = 0;
    if (matchedCount > 0) {
      const sumActual = parseFloat(statsResult.sumActual || "0");
      const sumSqActual = parseFloat(statsResult.sumSqActual || "0");
      const ssRes = sumSqError;
      const ssTot = sumSqActual - Math.pow(sumActual, 2) / matchedCount;

      if (ssTot > 0) {
        r2Score = parseFloat((1 - ssRes / ssTot).toFixed(2));
      }
    }

    // Coverage = verified-against-real-data (COMPLETED) / total. NOT matchedCount
    // (MAE-eligible), which under-reports by treating ride closures and sub-5-min
    // waits as "uncovered". See completedPredictions above.
    const coveragePercent =
      totalPredictions > 0
        ? parseFloat(
            ((completedPredictions / totalPredictions) * 100).toFixed(1),
          )
        : 0;

    // 3. Get Unique Counts (Optimized)
    const uniqueAttractionsResult = await this.accuracyRepository
      .createQueryBuilder("pa")
      .select("COUNT(DISTINCT pa.attractionId)", "count")
      .where("pa.targetTime >= :startDate", { startDate })
      .getRawOne();

    const uniqueParksResult = await this.accuracyRepository
      .createQueryBuilder("pa")
      .innerJoin("attractions", "a", "pa.attractionId = a.id")
      .select("COUNT(DISTINCT a.parkId)", "count")
      .where("pa.targetTime >= :startDate", { startDate })
      .getRawOne();

    // 4. Breakdown by Prediction Type
    const typeStatsRaw = await this.accuracyRepository
      .createQueryBuilder("pa")
      .select("pa.predictionType", "type")
      .addSelect("COUNT(*)", "total")
      .addSelect("COUNT(pa.actualWaitTime)", "matched")
      .addSelect(
        "AVG(CASE WHEN pa.actualWaitTime IS NOT NULL THEN pa.absoluteError ELSE NULL END)",
        "mae",
      )
      .where("pa.targetTime >= :startDate", { startDate })
      .groupBy("pa.predictionType")
      .getRawMany();

    // Initialize Default. mae/coverage are null (not 0) when a type isn't
    // tracked against actuals: daily predictions span up to 365 days and are
    // intentionally never compared, so reporting 0%/0min reads as "broken" on
    // the dashboard. `tracked` lets the UI render "n/a" instead.
    const byType: {
      HOURLY: TypeBreakdown;
      DAILY: TypeBreakdown;
    } = {
      HOURLY: {
        mae: null,
        totalPredictions: 0,
        coveragePercent: null,
        tracked: false,
      },
      DAILY: {
        mae: null,
        totalPredictions: 0,
        coveragePercent: null,
        tracked: false,
      },
    };

    typeStatsRaw.forEach((row) => {
      const type = row.type ? row.type.toUpperCase() : "UNKNOWN";
      const total = parseInt(row.total || "0", 10);
      const matched = parseInt(row.matched || "0", 10);

      if (type === "HOURLY" || type === "DAILY") {
        // Tracked only if rows exist AND at least one was matched to an actual.
        const tracked = total > 0 && matched > 0;
        byType[type as "HOURLY" | "DAILY"] = {
          mae: tracked
            ? parseFloat(parseFloat(row.mae || "0").toFixed(1))
            : null,
          totalPredictions: total,
          coveragePercent: tracked
            ? parseFloat(((matched / total) * 100).toFixed(1))
            : null,
          tracked,
        };
      }
    });

    return {
      overall: {
        mae,
        rmse,
        mape,
        r2Score,
        totalPredictions,
        matchedPredictions: matchedCount,
        coveragePercent,
        uniqueAttractions: parseInt(uniqueAttractionsResult.count || "0", 10),
        uniqueParks: parseInt(uniqueParksResult.count || "0", 10),
      },
      byPredictionType: byType,
    };
  }

  /**
   * Served-intraday accuracy from the PCN shadow board (see the
   * {@link ServedIntradayAccuracy} type). n-weighted MAE over the whole-day,
   * all-lead cells (`segment='all'`, `lead_bucket='all'`) — the same matched
   * population the admin PCN verdict uses, so `pcn` and `catboost` share `n`.
   * Degrades to `null` (never throws) so callers can render the CatBoost number
   * alone when PCN isn't serving.
   */
  async getServedIntradayAccuracy(
    days: number = 7,
  ): Promise<ServedIntradayAccuracy | null> {
    // The board lives in the pcn-service's schema; on a fresh DB or before PCN
    // is deployed the table doesn't exist — degrade to null rather than 500.
    const reg = await this.accuracyRepository.query(
      "SELECT to_regclass('public.pcn_intraday_comparisons') AS t",
    );
    if (!reg?.[0]?.t) return null;

    // CURRENT_DATE − days matches the board's park-local `target_date` keying.
    const rows: Array<{ model: string; n: string | null; mae: string | null }> =
      await this.accuracyRepository.query(
        `SELECT model, SUM(n) AS n, SUM(mae * n) / NULLIF(SUM(n), 0) AS mae
           FROM pcn_intraday_comparisons
          WHERE segment = 'all' AND lead_bucket = 'all'
            AND target_date >= (CURRENT_DATE - $1::int)
          GROUP BY model`,
        [days],
      );

    const pcn = rows.find((r) => r.model === "pcn");
    if (!pcn || pcn.mae == null) return null; // no served-PCN evidence in-window

    const cat = rows.find((r) => r.model === "catboost");
    const round1 = (v: number) => Math.round(v * 10) / 10;
    const mae = round1(parseFloat(pcn.mae));
    const catboostMae =
      cat && cat.mae != null ? round1(parseFloat(cat.mae)) : null;

    return {
      servedModel: "pcn",
      mae,
      n: parseInt(pcn.n ?? "0", 10) || 0,
      catboostMae,
      delta: catboostMae != null ? round1(catboostMae - mae) : null,
      days,
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
      // Thin-data gate: aggregate only ratable parks (typical-day-peak present).
      .innerJoin(
        "park_p50_baselines",
        "pb",
        'pb."parkId" = a."parkId" AND pb."typicalDayPeak" IS NOT NULL',
      )
      .select("pa.attractionId", "attractionId")
      .addSelect("a.name", "attractionName")
      .addSelect("p.name", "parkName")
      .addSelect("AVG(pa.absoluteError)", "mae")
      .addSelect("COUNT(*)", "predictionsCount")
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.actualWaitTime >= 5")
      .andWhere("pa.actualWaitTime <= :maxWait", {
        maxWait: MAX_PLAUSIBLE_WAIT_TIME,
      })
      .andWhere('pa."wasUnplannedClosure" = false')
      .groupBy("pa.attractionId")
      .addGroupBy("a.name")
      .addGroupBy("p.name")
      .having("COUNT(*) >= :minPredictions", { minPredictions: 10 })
      .andHaving("AVG(pa.actualWaitTime) >= :minAvgWait", { minAvgWait: 10 })
      // Exclude trivially-flat series (shows, walk-on/kiddie rides, transport)
      // mis-ingested as attractions: their wait never varies (a 4D film "queues"
      // a constant ~15 min = the show interval), so the model predicts the
      // constant perfectly → 0.0 MAE that floods the "best predictions" board
      // with non-rides. Real rides swing widely (stddev 14-29); shows sit at
      // 0-7 (measured live: Hall of Presidents 1.9, Magiezijn 0.0, vs Taron
      // 14.6, Manta 18.2). A stddev floor keeps the board to genuinely
      // predicted rides without deleting any data.
      .andHaving("STDDEV_SAMP(pa.actualWaitTime) >= :minStddev", {
        minStddev: 8,
      })
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
   * Per-attraction best/worst performers for the TFT DAILY model.
   *
   * CatBoost's getTopBottomPerformers scores hourly predictions out of
   * prediction_accuracy; TFT instead forecasts the daily peak (tft_forecasts)
   * and is scored against the realised daily P90 — the same contract the
   * nf-forecast `score-comparison` processor uses. We mirror that scoring here
   * but aggregate PER ATTRACTION, and reuse the same board hygiene as CatBoost:
   * a stddev floor on the realised wait so flat shows/walk-on rides don't flood
   * the "best" list with trivially-perfect 0-MAE non-rides.
   */
  async getTftTopBottomPerformers(
    days: number = 14,
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
    const rows: Array<{
      attractionId: string;
      attractionName: string | null;
      parkName: string | null;
      mae: string;
      predictionsCount: string;
    }> = await this.accuracyRepository.query(
      `
      WITH actuals AS (
        SELECT qd."attractionId" aid,
               DATE(qd.timestamp AT TIME ZONE p.timezone) d,
               PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") p90
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE qd.timestamp >= NOW() - ($1 || ' days')::interval
          AND qd.status = 'OPERATING' AND qd."queueType" = 'STANDBY'
          AND qd."waitTime" >= 5
        GROUP BY 1, 2
        HAVING COUNT(*) >= 3
      ),
      tft AS (
        SELECT DISTINCT ON (f.attraction_id, f.target_date)
          f.attraction_id aid, f.target_date d, f.predicted_peak pred
        FROM tft_forecasts f
        WHERE f.target_date >= (CURRENT_DATE - $1::int)
          AND f.forecast_date < f.target_date
        ORDER BY f.attraction_id, f.target_date, f.forecast_date DESC
      ),
      scored AS (
        SELECT t.aid, act.p90, t.pred
        FROM tft t
        JOIN actuals act ON act.aid = t.aid AND act.d = t.d
        WHERE t.d < CURRENT_DATE
      )
      SELECT s.aid::text "attractionId", a.name "attractionName", p.name "parkName",
             AVG(ABS(s.pred - s.p90)) mae,
             COUNT(*)::int "predictionsCount"
      FROM scored s
      JOIN attractions a ON a.id = s.aid
      JOIN parks p ON p.id = a."parkId"
      GROUP BY s.aid, a.name, p.name
      HAVING COUNT(*) >= 5
         AND AVG(s.p90) >= 10
         AND STDDEV_SAMP(s.p90) >= 8
      ORDER BY mae ASC
      `,
      [days],
    );

    const sorted = rows.map((r) => ({
      attractionId: r.attractionId,
      attractionName: r.attractionName || "Unknown",
      parkName: r.parkName || "Unknown",
      mae: parseFloat(parseFloat(r.mae).toFixed(1)),
      predictionsCount: parseInt(r.predictionsCount as unknown as string, 10),
    }));

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
      // Thin-data gate: aggregate only ratable parks (typical-day-peak present).
      .innerJoin("attractions", "a", "a.id = pa.attractionId")
      .innerJoin(
        "park_p50_baselines",
        "pb",
        'pb."parkId" = a."parkId" AND pb."typicalDayPeak" IS NOT NULL',
      )
      .select("DATE(pa.targetTime AT TIME ZONE 'UTC')", "date")
      .addSelect("AVG(pa.absoluteError)", "mae")
      .addSelect("COUNT(*)", "totalCount")
      .addSelect(
        'COUNT(CASE WHEN pa.actualWaitTime >= 5 AND pa.actualWaitTime <= :maxWait AND pa."wasUnplannedClosure" = false THEN 1 END)',
        "matchedCount",
      )
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.actualWaitTime >= 5")
      .andWhere("pa.actualWaitTime <= :maxWait", {
        maxWait: MAX_PLAUSIBLE_WAIT_TIME,
      })
      .andWhere('pa."wasUnplannedClosure" = false')
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
  async getHourlyAccuracyPatterns(days: number = 30) {
    return this.cachedAgg(
      `accuracy:hourly:${days}`,
      this.TTL_ACCURACY_AGG,
      () => this.computeHourlyAccuracyPatterns(days),
    );
  }

  private async computeHourlyAccuracyPatterns(days: number = 30): Promise<
    Array<{
      hour: number;
      mae: number;
      predictionsCount: number;
    }>
  > {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const results = await this.accuracyRepository
      .createQueryBuilder("pa")
      // Thin-data gate: aggregate only ratable parks (typical-day-peak present).
      .innerJoin("attractions", "a", "a.id = pa.attractionId")
      .innerJoin(
        "park_p50_baselines",
        "pb",
        'pb."parkId" = a."parkId" AND pb."typicalDayPeak" IS NOT NULL',
      )
      .select("EXTRACT(HOUR FROM pa.targetTime AT TIME ZONE 'UTC')", "hour")
      .addSelect("AVG(pa.absoluteError)", "mae")
      .addSelect("COUNT(*)", "predictionsCount")
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.actualWaitTime >= 5")
      .andWhere("pa.actualWaitTime <= :maxWait", {
        maxWait: MAX_PLAUSIBLE_WAIT_TIME,
      })
      .andWhere('pa."wasUnplannedClosure" = false')
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
  async getDayOfWeekAccuracyPatterns(days: number = 30) {
    return this.cachedAgg(`accuracy:dow:${days}`, this.TTL_ACCURACY_AGG, () =>
      this.computeDayOfWeekAccuracyPatterns(days),
    );
  }

  private async computeDayOfWeekAccuracyPatterns(days: number = 30): Promise<
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
      // Thin-data gate: aggregate only ratable parks (typical-day-peak present).
      .innerJoin("attractions", "a", "a.id = pa.attractionId")
      .innerJoin(
        "park_p50_baselines",
        "pb",
        'pb."parkId" = a."parkId" AND pb."typicalDayPeak" IS NOT NULL',
      )
      .select(
        "EXTRACT(DOW FROM pa.targetTime AT TIME ZONE 'UTC')",
        "day_of_week",
      )
      .addSelect("AVG(pa.absoluteError)", "mae")
      .addSelect("COUNT(*)", "predictionsCount")
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.actualWaitTime >= 5")
      .andWhere("pa.actualWaitTime <= :maxWait", {
        maxWait: MAX_PLAUSIBLE_WAIT_TIME,
      })
      .andWhere('pa."wasUnplannedClosure" = false')
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
        actualWaitTime: Between(5, MAX_PLAUSIBLE_WAIT_TIME),
        wasUnplannedClosure: false,
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
      .andWhere("pa.actualWaitTime >= 5")
      .andWhere("pa.actualWaitTime <= :maxWait", {
        maxWait: MAX_PLAUSIBLE_WAIT_TIME,
      })
      .andWhere('pa."wasUnplannedClosure" = false')
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
    metrics: {
      mae: number;
      rmse: number;
      mape: number;
      r2Score: number;
      totalPredictions: number;
      matchedPredictions: number;
      coveragePercent: number;
    } | null;
  }> {
    try {
      const stats = await this.getSystemAccuracyStats(days);

      // Thresholds
      const MAE_THRESHOLD = 8; // minutes — alert when ~2× training MAE (training baseline ~4.6 min)
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

      // Coverage intentionally excluded: low coverage means attractions without
      // OPERATING data are being predicted (pipeline issue), not model degradation.
      // Retraining on poor-coverage data makes the model worse, not better.

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
