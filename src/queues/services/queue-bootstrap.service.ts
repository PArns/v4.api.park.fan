import { Injectable, OnModuleInit, Logger } from "@nestjs/common";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { Park } from "../../parks/entities/park.entity";
import { AttractionP50Baseline } from "../../analytics/entities/attraction-p50-baseline.entity";

/**
 * Queue Bootstrap Service
 *
 * Problem: On first startup, no data exists. Bull queues run every 5-15 minutes,
 * delaying initial data fetch.
 *
 * Solution: Trigger immediate queue jobs on startup (non-blocking).
 *
 * Workflow:
 * 1. OnModuleInit → check if database is empty
 * 2. If empty → trigger park-metadata and attractions-metadata jobs immediately
 * 3. If data exists → trigger wait-times update
 * 4. Regular scheduled jobs continue as normal (cron)
 *
 * Note: This service runs ONCE on app startup. Actual processors will be
 * implemented in Phase 2 when ThemeParks.wiki integration is ready.
 */
@Injectable()
export class QueueBootstrapService implements OnModuleInit {
  private readonly logger = new Logger(QueueBootstrapService.name);

  constructor(
    @InjectQueue("wait-times") private waitTimesQueue: Queue,
    @InjectQueue("park-metadata") private parkMetadataQueue: Queue,
    @InjectQueue("children-metadata") private childrenQueue: Queue, // Phase 6.2: Combined queue
    @InjectQueue("weather") private weatherQueue: Queue,
    @InjectQueue("holidays") private holidaysQueue: Queue,
    @InjectQueue("ml-training") private mlTrainingQueue: Queue,
    @InjectQueue("prediction-accuracy") private predictionAccuracyQueue: Queue,
    @InjectQueue("analytics") private analyticsQueue: Queue,
    @InjectQueue("p50-baseline") private p50BaselineQueue: Queue, // P50 baseline queue
    @InjectQueue("rope-drop") private ropeDropQueue: Queue,
    @InjectQueue("typical-waits") private typicalWaitsQueue: Queue,
    @InjectRepository(Park) private parkRepository: Repository<Park>,
    @InjectRepository(AttractionP50Baseline)
    private p50Repository: Repository<AttractionP50Baseline>,
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
  ) {}

  /**
   * Runs after all modules initialized.
   * Triggers initial data fetch jobs (non-blocking).
   */
  async onModuleInit(): Promise<void> {
    // Skip bootstrap if SKIP_QUEUE_BOOTSTRAP is set (for scripts)
    if (process.env.SKIP_QUEUE_BOOTSTRAP === "true") {
      this.logger.debug("Queue bootstrap skipped (SKIP_QUEUE_BOOTSTRAP=true)");
      return;
    }

    // Non-blocking: fire and forget
    this.bootstrapQueues().catch((err) => {
      this.logger.error("Queue bootstrap failed", err);
    });
  }

  /**
   * Checks database state and triggers appropriate jobs.
   */
  private async bootstrapQueues(): Promise<void> {
    this.logger.log("🚀 Queue bootstrap starting...");

    // Clean up old jobs from all queues
    await this.cleanupQueues();

    // Check if parks exist in database
    const parkCount = await this.parkRepository.count();
    const isEmpty = parkCount === 0;

    this.logger.log(`Database status: ${parkCount} parks found`);

    if (isEmpty) {
      // Priority 1: (moved to end of function to run always)

      this.logger.log("No data found. Triggering initial metadata fetch...");

      // Phase 6.2: Use COMBINED children-metadata job instead of 3 separate jobs
      // This reduces API requests by 67% (105 requests instead of 315)
      const childrenJobActive = await this.isJobActiveOrWaiting(
        this.childrenQueue,
        "fetch-all-children",
      );

      if (!childrenJobActive) {
        // Priority 2: Fetch ALL children (attractions, shows, restaurants) in ONE job
        await this.childrenQueue.add(
          "fetch-all-children",
          {},
          {
            priority: 1,
            jobId: "bootstrap-children", // Prevent duplicates
            removeOnComplete: true,
          },
        );
        this.logger.log(
          "✅ Combined children metadata job queued (Attractions + Shows + Restaurants)",
        );
      } else {
        this.logger.log(
          "⏭️  Combined children metadata job already running, skipping",
        );
      }
    } else {
      this.logger.log("Data exists. Checking data freshness...");

      // Optimization: Check if we have recent queue data (last 5 minutes)
      // If so, skip immediate fetch to prevent "double fetch" with cron job
      const latestData = await this.queueDataRepository.findOne({
        where: {},
        order: { timestamp: "DESC" },
        select: ["timestamp"],
      });

      const isFresh =
        latestData &&
        Date.now() - latestData.timestamp.getTime() < 5 * 60 * 1000;

      if (isFresh) {
        this.logger.debug(
          `✅ Data is fresh (${latestData?.timestamp.toISOString()}). Skipping immediate wait times update.`,
        );
      } else {
        this.logger.log("Data is stale. Triggering wait times update...");

        // Check if wait-times job is already active/waiting
        const waitTimesJobActive = await this.isJobActiveOrWaiting(
          this.waitTimesQueue,
          "fetch-wait-times",
        );

        if (!waitTimesJobActive) {
          // Trigger immediate wait times update
          await this.waitTimesQueue.add(
            "fetch-wait-times",
            {},
            {
              priority: 2,
              jobId: "bootstrap-wait-times", // Prevent duplicates
              removeOnComplete: true,
            },
          );
          this.logger.log("✅ Wait times update job queued");
        } else {
          this.logger.debug("⏭️  Wait times job already running, skipping");
        }
      }
    }

    // 4. Holidays are NOT synced on boot — the monthly cron is the single
    // scheduled source, and a holiday sync is also triggered after a
    // park-metadata sync (park-metadata.processor.ts) so new park countries
    // get covered once their country data exists.

    // Trigger ML-related bootstrap jobs — only when the data is genuinely missing.
    // attraction_accuracy_stats is maintained by the every-15-min prediction-accuracy cron,
    // so on a warm redeploy (table already populated) the boot trigger is pure redundant
    // load; only seed it on a cold start.
    try {
      const accuracyRows = await this.parkRepository.query(
        `SELECT 1 FROM attraction_accuracy_stats LIMIT 1`,
      );
      if (accuracyRows.length === 0) {
        await this.predictionAccuracyQueue.add(
          "aggregate-stats",
          {},
          { removeOnComplete: true },
        );
        this.logger.log(
          "✅ Boot: accuracy-stats aggregation queued (table empty)",
        );
      } else {
        this.logger.debug(
          "⏭️  Boot: accuracy-stats already populated — leaving it to the 15-min cron",
        );
      }

      // P90 pre-aggregation removed — the daily P50 baseline cron now
      // computes P90 alongside P50 (single 548-day scan), so the
      // occupancy-calculation precompute job was pure duplicate work.
    } catch (e) {
      this.logger.warn(`Failed to trigger ML/analytics jobs: ${e}`);
    }

    // 5. Trigger P50 Baseline Calculation (Optimized: Skip if fresh)
    try {
      const latestP50 = await this.p50Repository.findOne({
        where: {},
        order: { updatedAt: "DESC" },
      });

      const isP50Fresh =
        latestP50 &&
        Date.now() - latestP50.updatedAt.getTime() < 12 * 60 * 60 * 1000;

      // Force a recompute (in batches — the job processes parks in groups of
      // 5) when the typical-day-peak baseline hasn't been populated yet, even
      // if P50 is fresh. This handles the rollout of the typical-day-peak
      // column: existing park_p50_baselines rows have it NULL until the first
      // recompute fills it. Raw query avoids injecting the park-P50 repo here.
      const typicalRows = await this.parkRepository.query(
        `SELECT 1 FROM park_p50_baselines WHERE "typicalDayPeak" IS NOT NULL LIMIT 1`,
      );
      const needsTypicalDayPeak = typicalRows.length === 0;

      if (isP50Fresh && !needsTypicalDayPeak) {
        this.logger.log(
          `✅ P50 Baselines are fresh (from ${latestP50.updatedAt.toISOString()}). Skipping initial calculation.`,
        );
      } else {
        if (needsTypicalDayPeak) {
          this.logger.log(
            "🔁 typical-day-peak baseline missing — forcing batched park-baseline recompute (post-deploy backfill).",
          );
        }
        const p50ParkJobActive = await this.isJobActiveOrWaiting(
          this.p50BaselineQueue,
          "calculate-park-baselines",
        );

        if (!p50ParkJobActive) {
          await this.p50BaselineQueue.add(
            "calculate-park-baselines",
            {},
            {
              priority: 4,
              jobId: "bootstrap-p50-parks",
              removeOnComplete: true,
            },
          );
          this.logger.log("✅ Boot: P50 park baselines calculation queued");
        }

        // Trigger attraction P50 calculation (runs after park baselines)
        const p50AttrJobActive = await this.isJobActiveOrWaiting(
          this.p50BaselineQueue,
          "calculate-attraction-baselines",
        );

        if (!p50AttrJobActive) {
          await this.p50BaselineQueue.add(
            "calculate-attraction-baselines",
            {},
            {
              priority: 5,
              jobId: "bootstrap-p50-attractions",
              removeOnComplete: true,
              delay: 300000, // Delay 5min: boot-rush (wait-times sync, schedules, holidays) needs to clear before this 3-6min job runs, otherwise Bull marks it stalled
            },
          );
          this.logger.log(
            "✅ Boot: P50 attraction baselines calculation queued (delayed 5min)",
          );
        }
      }
    } catch (e) {
      this.logger.warn(`Failed to trigger P50 baseline jobs: ${e}`);
    }

    // 6. Seasonal detection: only on a cold start. It's maintained by the daily 02:30
    // detect-seasonal cron, so on a warm redeploy re-running it (incl. the
    // attraction_day_operating rollup work) is pure boot-load. Gate on the rollup being
    // empty — true exactly when the table was just created (first deploy of this feature)
    // or wiped — so the one-time backfill still happens, then never again on redeploys.
    try {
      const rollupRows = await this.parkRepository.query(
        `SELECT 1 FROM attraction_day_operating LIMIT 1`,
      );
      const rollupEmpty = rollupRows.length === 0;
      const seasonalJobActive = await this.isJobActiveOrWaiting(
        this.analyticsQueue,
        "detect-seasonal",
      );

      if (rollupEmpty && !seasonalJobActive) {
        await this.analyticsQueue.add(
          "detect-seasonal",
          {},
          {
            priority: 6,
            jobId: "bootstrap-seasonal-detection",
            removeOnComplete: true,
            delay: 90000, // 90s delay — let P50 baselines run first
          },
        );
        this.logger.log(
          "✅ Boot: Seasonal detection queued (operating-day rollup empty — one-time backfill)",
        );
      } else {
        this.logger.debug(
          "⏭️  Boot: Seasonal detection skipped (rollup populated → daily cron maintains it)",
        );
      }
    } catch (e) {
      this.logger.warn(`Failed to trigger seasonal detection: ${e}`);
    }

    // 7. Rope-drop: one-time force-run when the table is empty (post-deploy
    // backfill). Maintained by the daily 05:15 cron, so on a warm redeploy this
    // is skipped. Gate on the table being empty — true only on the first deploy
    // of this feature (synchronize just created it) or after a wipe. Delayed so
    // the boot-rush and the hourly-history/p50 jobs clear first.
    try {
      const ropeRows = await this.parkRepository.query(
        `SELECT 1 FROM attraction_rope_drop LIMIT 1`,
      );
      const ropeEmpty = ropeRows.length === 0;
      const ropeJobActive = await this.isJobActiveOrWaiting(
        this.ropeDropQueue,
        "calculate-rope-drop",
      );

      if (ropeEmpty && !ropeJobActive) {
        await this.ropeDropQueue.add(
          "calculate-rope-drop",
          {},
          {
            priority: 7,
            jobId: "bootstrap-rope-drop",
            removeOnComplete: true,
            delay: 360000, // 6min — let boot-rush + p50 baselines run first
          },
        );
        this.logger.log(
          "✅ Boot: rope-drop queued (table empty — one-time backfill)",
        );
      } else {
        this.logger.debug(
          "⏭️  Boot: rope-drop skipped (populated → daily cron maintains it)",
        );
      }
    } catch (e) {
      this.logger.warn(`Failed to trigger rope-drop: ${e}`);
    }

    // Typical-waits backfill — same gate as rope-drop: force one run when the
    // table is empty (first deploy of this feature / after a wipe), else the
    // daily 5:30 cron maintains it.
    try {
      const twRows = await this.parkRepository.query(
        `SELECT 1 FROM attraction_typical_waits LIMIT 1`,
      );
      const twEmpty = twRows.length === 0;
      const twJobActive = await this.isJobActiveOrWaiting(
        this.typicalWaitsQueue,
        "calculate-typical-waits",
      );

      if (twEmpty && !twJobActive) {
        await this.typicalWaitsQueue.add(
          "calculate-typical-waits",
          {},
          {
            priority: 7,
            jobId: "bootstrap-typical-waits",
            removeOnComplete: true,
            delay: 420000, // 7min — after the rope-drop backfill
          },
        );
        this.logger.log(
          "✅ Boot: typical-waits queued (table empty — one-time backfill)",
        );
      } else {
        this.logger.debug(
          "⏭️  Boot: typical-waits skipped (populated → daily cron maintains it)",
        );
      }
    } catch (e) {
      this.logger.warn(`Failed to trigger typical-waits: ${e}`);
    }
  }

  /**
   * Check if a job with the given name is already active or waiting.
   */
  private async isJobActiveOrWaiting(
    queue: Queue,
    jobName: string,
  ): Promise<boolean> {
    const [activeJobs, waitingJobs] = await Promise.all([
      queue.getActive(),
      queue.getWaiting(),
    ]);

    const hasActiveJob = activeJobs.some((job) => job.name === jobName);
    const hasWaitingJob = waitingJobs.some((job) => job.name === jobName);

    return hasActiveJob || hasWaitingJob;
  }

  /**
   * Clean up old completed and failed jobs from all queues.
   * This prevents job accumulation and keeps Redis memory usage low.
   */
  private async cleanupQueues(): Promise<void> {
    this.logger.log("🧹 Cleaning up old jobs from queues...");

    const queues = [
      { name: "wait-times", queue: this.waitTimesQueue },
      { name: "park-metadata", queue: this.parkMetadataQueue },
      { name: "children-metadata", queue: this.childrenQueue }, // Phase 6.2: Combined
      { name: "weather", queue: this.weatherQueue },
      { name: "holidays", queue: this.holidaysQueue },
      { name: "ml-training", queue: this.mlTrainingQueue },
    ];

    let totalCleaned = 0;

    for (const { name, queue } of queues) {
      try {
        // More aggressive cleanup: Limit increased to 1000
        const completed = await queue.clean(0, "completed", 1000);
        const failed = await queue.clean(0, "failed", 1000);

        const cleaned = completed.length + failed.length;
        totalCleaned += cleaned;

        if (cleaned > 0) {
          this.logger.debug(
            `  ✓ Queue [${name}]: cleaned ${completed.length} completed, ${failed.length} failed jobs`,
          );
        }
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.warn(`Failed to clean queue [${name}]: ${errorMessage}`);
      }
    }

    if (totalCleaned > 0) {
      this.logger.log(`✅ Cleaned ${totalCleaned} old jobs from queues`);
    } else {
      this.logger.debug("✅ No old jobs to clean");
    }
  }
}
