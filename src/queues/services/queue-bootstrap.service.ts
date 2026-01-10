import { Injectable, OnModuleInit, Logger } from "@nestjs/common";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { Park } from "../../parks/entities/park.entity";

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
    @InjectQueue("attractions-metadata") private attractionsQueue: Queue, // DEPRECATED
    @InjectQueue("shows-metadata") private showsQueue: Queue, // DEPRECATED
    @InjectQueue("restaurants-metadata") private restaurantsQueue: Queue, // DEPRECATED
    @InjectQueue("occupancy-calculation") private occupancyQueue: Queue,
    @InjectQueue("weather") private weatherQueue: Queue,
    @InjectQueue("holidays") private holidaysQueue: Queue,
    @InjectQueue("ml-training") private mlTrainingQueue: Queue,
    @InjectQueue("prediction-accuracy") private predictionAccuracyQueue: Queue,
    @InjectRepository(Park) private parkRepository: Repository<Park>,
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

    // ALWAYS trigger park metadata sync on boot (non-blocking)
    // This ensures code updates (e.g. slug changes, new matching logic) are applied immediately
    // without waiting for the nightly 3AM job.
    const parkJobActive = await this.isJobActiveOrWaiting(
      this.parkMetadataQueue,
      "sync-all-parks",
    );

    if (!parkJobActive) {
      // Remove stale job first (to ensure ID is available)
      const existingJob = await this.parkMetadataQueue.getJob(
        "bootstrap-parks-sync",
      );
      if (existingJob) {
        await existingJob.remove().catch(() => {});
      }

      await this.parkMetadataQueue.add(
        "sync-all-parks",
        {},
        {
          priority: 10, // Highest - runs first
          jobId: "bootstrap-parks-sync", // Unique ID for boot run
          removeOnComplete: true,
        },
      );
      this.logger.log("✅ Boot: Park metadata sync job queued (Force Sync)");
    } else {
      this.logger.debug(
        "⏭️  Boot: Park metadata sync job already running, skipping",
      );
    }

    // 4. Trigger Holiday Sync (Holidays change infrequently, but we need initial data)
    // This fetches both Public (Nager) and School (OpenHolidays) data
    const holidayJobActive = await this.isJobActiveOrWaiting(
      this.holidaysQueue,
      "fetch-holidays",
    );

    if (!holidayJobActive) {
      // Remove stale job first if any (completed/failed)
      const existingJob = await this.holidaysQueue.getJob("bootstrap-holidays");
      if (existingJob) {
        await existingJob.remove().catch(() => {});
      }

      await this.holidaysQueue.add(
        "fetch-holidays",
        {},
        {
          priority: 5,
          jobId: "bootstrap-holidays",
          removeOnComplete: true,
        },
      );
      this.logger.log("✅ Boot: Holiday sync job queued");
    } else {
      this.logger.debug("⏭️  Boot: Holiday sync job already running, skipping");
    }

    // NOTE: Holidays sync is now triggered AFTER park metadata completes
    // (from park-metadata.processor.ts) to ensure all parks have country data

    // Trigger ML training check immediately
    try {
      await this.mlTrainingQueue.add(
        "check-training-needed",
        {},
        {
          removeOnComplete: true,
        },
      );

      // Trigger prediction accuracy aggregation immediately (Phase 2 optimization)
      // This ensures the attraction_accuracy_stats table is populated on startup
      await this.predictionAccuracyQueue.add(
        "aggregate-stats",
        {},
        {
          removeOnComplete: true,
        },
      );

      // Trigger P90 sliding window pre-aggregation
      // This pre-computes P90 values for all parks and attractions
      // Benefits: Faster crowd level calculations, reduced Redis cache misses
      const p90JobActive = await this.isJobActiveOrWaiting(
        this.occupancyQueue,
        "precompute-p90-sliding-window",
      );

      if (!p90JobActive) {
        await this.occupancyQueue.add(
          "precompute-p90-sliding-window",
          {},
          {
            priority: 3,
            jobId: "bootstrap-p90-precompute",
            removeOnComplete: true,
          },
        );
        this.logger.log("✅ Boot: P90 sliding window pre-aggregation queued");
      } else {
        this.logger.debug(
          "⏭️  Boot: P90 pre-aggregation already running, skipping",
        );
      }
    } catch (e) {
      this.logger.warn(`Failed to trigger ML/analytics jobs: ${e}`);
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
      { name: "occupancy-calculation", queue: this.occupancyQueue },
      { name: "weather", queue: this.weatherQueue },
      { name: "holidays", queue: this.holidaysQueue },
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
