import { Injectable, OnModuleInit, Logger } from "@nestjs/common";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";

/**
 * Queue Scheduler Service
 *
 * Registers scheduled (repeating) jobs for all queues.
 * Runs after bootstrap to ensure data exists first.
 *
 * Schedule:
 * - wait-times: Every 5 minutes (frequent updates)
 * - park-metadata: Daily at 3am (metadata changes rarely)
 * - children-metadata: Daily at 4am (Phase 6.2: Combined Attractions + Shows + Restaurants - 67% fewer requests!)
 * - weather: Every 12 hours (0:00 and 12:00)
 * - weather-historical: Daily at 5am (mark past data as historical)
 * - holidays: Monthly on 1st at 2am (holidays change rarely)
 * - ml-predictions: Every 15 minutes (fresh crowd/wait predictions)
 * - ml-training: Daily at 6am (retrain model with new data)
 * - prediction-accuracy: Every hour (compare predictions with actuals)
 * - occupancy-calculation: Every 15 minutes (placeholder for Phase 5)
 * - geoip-update: Every 48 hours (GeoLite2-City for nearby endpoint)
 */
@Injectable()
export class QueueSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(QueueSchedulerService.name);

  constructor(
    @InjectQueue("wait-times") private waitTimesQueue: Queue,
    @InjectQueue("park-metadata") private parkMetadataQueue: Queue,
    @InjectQueue("children-metadata") private childrenQueue: Queue, // Phase 6.2: Combined
    @InjectQueue("attractions-metadata") private attractionsQueue: Queue, // DEPRECATED
    @InjectQueue("shows-metadata") private showsQueue: Queue, // DEPRECATED
    @InjectQueue("restaurants-metadata") private restaurantsQueue: Queue, // DEPRECATED
    @InjectQueue("occupancy-calculation") private occupancyQueue: Queue,
    @InjectQueue("weather") private weatherQueue: Queue,
    @InjectQueue("weather-historical")
    private weatherHistoricalQueue: Queue,
    @InjectQueue("holidays") private holidaysQueue: Queue,
    @InjectQueue("ml-training") private mlTrainingQueue: Queue,
    @InjectQueue("prediction-accuracy")
    private predictionAccuracyQueue: Queue,
    @InjectQueue("predictions") private predictionsQueue: Queue,
    @InjectQueue("analytics") private analyticsQueue: Queue,
    @InjectQueue("wartezeiten-schedule")
    private wartezeitenScheduleQueue: Queue,
    @InjectQueue("ml-monitoring")
    private mlMonitoringQueue: Queue,
    @InjectQueue("stats") private statsQueue: Queue,
    @InjectQueue("p50-baseline") private p50BaselineQueue: Queue, // P50 baseline
    @InjectQueue("geoip-update") private geoipUpdateQueue: Queue,
  ) {}

  async onModuleInit(): Promise<void> {
    // Skip scheduler if SKIP_QUEUE_BOOTSTRAP is set (for scripts)
    if (process.env.SKIP_QUEUE_BOOTSTRAP === "true") {
      this.logger.debug("Queue scheduler skipped (SKIP_QUEUE_BOOTSTRAP=true)");
      return;
    }

    // Wait a bit to let bootstrap complete first
    setTimeout(() => {
      this.registerScheduledJobs().catch((err) => {
        this.logger.error("Failed to register scheduled jobs", err);
      });
    }, 5000); // 5 second delay
  }

  private async registerScheduledJobs(): Promise<void> {
    this.logger.log("📅 Registering scheduled jobs...");

    // Wait Times: Every 5 minutes
    const hasWaitTimesCron = await this.hasRepeatableJob(
      this.waitTimesQueue,
      "wait-times-cron",
    );

    if (!hasWaitTimesCron) {
      await this.waitTimesQueue.add(
        "fetch-wait-times",
        {},
        {
          repeat: {
            cron: "*/5 * * * *", // Every 5 minutes
          },
          jobId: "wait-times-cron", // Prevent duplicates
        },
      );
    }

    // Park Metadata: Daily at 3am
    const hasParkMetadataCron = await this.hasRepeatableJob(
      this.parkMetadataQueue,
      "park-metadata-cron",
    );

    if (!hasParkMetadataCron) {
      await this.parkMetadataQueue.add(
        "sync-all-parks",
        {},
        {
          repeat: {
            cron: "0 3 * * *", // Daily at 3am
          },
          jobId: "park-metadata-cron",
        },
      );
    }

    // Schedule-only sync: Daily at 15:00 so new opening hours (e.g. Efteling March+) appear same day
    const hasSchedulesOnlyCron = await this.hasRepeatableJob(
      this.parkMetadataQueue,
      "park-schedules-only-cron",
    );
    if (!hasSchedulesOnlyCron) {
      await this.parkMetadataQueue.add(
        "sync-schedules-only",
        {},
        {
          repeat: {
            cron: "0 15 * * *", // Daily at 3pm
          },
          jobId: "park-schedules-only-cron",
        },
      );
    }

    // Calendar warmup: once per day (5am) so /calendar is fast without warming every 5 min
    const hasCalendarWarmupCron = await this.hasRepeatableJob(
      this.parkMetadataQueue,
      "warmup-calendar-daily-cron",
    );
    if (!hasCalendarWarmupCron) {
      await this.parkMetadataQueue.add(
        "warmup-calendar-daily",
        {},
        {
          repeat: {
            cron: "0 5 * * *", // Daily at 5am
          },
          jobId: "warmup-calendar-daily-cron",
        },
      );
    }

    // Phase 6.2: Combined Children Metadata (Attractions + Shows + Restaurants)
    // Daily at 4am (replaces 3 separate jobs at 4am, 4:30am, 5am)
    // This reduces API requests by 67% (105 instead of 315)
    const hasChildrenCron = await this.hasRepeatableJob(
      this.childrenQueue,
      "children-metadata-cron",
    );

    if (!hasChildrenCron) {
      await this.childrenQueue.add(
        "fetch-all-children",
        {},
        {
          repeat: {
            cron: "0 4 * * *", // Daily at 4am
          },
          jobId: "children-metadata-cron",
        },
      );
    }

    // Weather full sync: Every 12 hours (forecast + current)
    const hasWeatherFullCron = await this.hasRepeatableJob(
      this.weatherQueue,
      "weather-full-cron",
      "0 */12 * * *",
    );

    if (!hasWeatherFullCron) {
      await this.weatherQueue.add(
        "fetch-weather",
        { currentOnly: false },
        {
          repeat: { cron: "0 */12 * * *" },
          jobId: "weather-full-cron",
        },
      );
    }

    // Weather current-only sync: Every hour (live conditions for today)
    const hasWeatherCurrentCron = await this.hasRepeatableJob(
      this.weatherQueue,
      "weather-current-cron",
      "0 * * * *",
    );

    if (!hasWeatherCurrentCron) {
      await this.weatherQueue.add(
        "fetch-weather",
        { currentOnly: true },
        {
          repeat: { cron: "0 * * * *" },
          jobId: "weather-current-cron",
        },
      );
    }

    // Remove old combined weather cron if still registered
    const oldWeatherJobs = await this.weatherQueue.getRepeatableJobs();
    const oldWeatherCron = oldWeatherJobs.find((j) => j.id === "weather-cron");
    if (oldWeatherCron) {
      await this.weatherQueue.removeRepeatableByKey(oldWeatherCron.key);
      this.logger.log('Removed legacy "weather-cron" job');
    }

    // Weather Historical: Daily at 5am
    const hasWeatherHistoricalCron = await this.hasRepeatableJob(
      this.weatherHistoricalQueue,
      "weather-historical-cron",
    );

    if (!hasWeatherHistoricalCron) {
      await this.weatherHistoricalQueue.add(
        "mark-historical",
        {},
        {
          repeat: {
            cron: "0 5 * * *", // Daily at 5am
          },
          jobId: "weather-historical-cron",
        },
      );
    }

    // Holidays: Monthly on 1st at 2am
    const hasHolidaysCron = await this.hasRepeatableJob(
      this.holidaysQueue,
      "holidays-cron",
    );

    if (!hasHolidaysCron) {
      await this.holidaysQueue.add(
        "fetch-holidays",
        {},
        {
          repeat: {
            cron: "0 2 1 * *", // Monthly on 1st at 2am
          },
          jobId: "holidays-cron",
        },
      );
    }

    // ML Training: Daily at 6am (Phase 5)
    const hasMLTrainingCron = await this.hasRepeatableJob(
      this.mlTrainingQueue,
      "ml-training-cron",
    );

    if (!hasMLTrainingCron) {
      await this.mlTrainingQueue.add(
        "train-model",
        {},
        {
          repeat: {
            cron: "0 6 * * *", // Daily at 6am
          },
          jobId: "ml-training-cron",
        },
      );
    }

    // Model Cleanup: Daily at 7am (delete models older than 3 days)
    const hasModelCleanupCron = await this.hasRepeatableJob(
      this.mlTrainingQueue,
      "ml-model-cleanup-cron",
    );

    if (!hasModelCleanupCron) {
      await this.mlTrainingQueue.add(
        "cleanup-models",
        {},
        {
          repeat: {
            cron: "0 7 * * *", // Daily at 7am
          },
          jobId: "ml-model-cleanup-cron",
        },
      );
    }

    // Prediction Accuracy: Every 15 minutes (aligned with prediction generation cycle)
    const hasPredictionAccuracyCron = await this.hasRepeatableJob(
      this.predictionAccuracyQueue,
      "prediction-accuracy-cron",
      "*/15 * * * *",
    );

    if (!hasPredictionAccuracyCron) {
      await this.predictionAccuracyQueue.add(
        "compare-accuracy",
        {},
        {
          repeat: {
            cron: "*/15 * * * *",
          },
          jobId: "prediction-accuracy-cron",
        },
      );
    }

    // Prediction Accuracy Stats: Daily at 3am (aggregate MAE per attraction)
    const hasAggregateStatsCron = await this.hasRepeatableJob(
      this.predictionAccuracyQueue,
      "aggregate-stats-cron",
    );

    if (!hasAggregateStatsCron) {
      await this.predictionAccuracyQueue.add(
        "aggregate-stats",
        {},
        {
          repeat: {
            cron: "0 3 * * *", // Daily at 3am
          },
          jobId: "aggregate-stats-cron",
        },
      );
    }

    // Prediction Accuracy Cleanup: Daily at 4am (delete old MISSED/PENDING records)
    const hasAccuracyCleanupCron = await this.hasRepeatableJob(
      this.predictionAccuracyQueue,
      "accuracy-cleanup-cron",
    );

    if (!hasAccuracyCleanupCron) {
      await this.predictionAccuracyQueue.add(
        "cleanup-old",
        {},
        {
          repeat: {
            cron: "0 4 * * *", // Daily at 4am
          },
          jobId: "accuracy-cleanup-cron",
        },
      );
    }

    // Predictions: Every 15 minutes (fresh predictions for bestVisitTimes and crowd levels)
    const hasHourlyPredictionsCron = await this.hasRepeatableJob(
      this.predictionsQueue,
      "hourly-predictions-cron",
      "*/15 * * * *",
    );

    if (!hasHourlyPredictionsCron) {
      await this.predictionsQueue.add(
        "generate-hourly",
        {},
        {
          repeat: {
            cron: "*/15 * * * *", // Every 15 minutes
          },
          jobId: "hourly-predictions-cron",
        },
      );
    }

    // Daily Predictions: Once per day at 1am
    const hasDailyPredictionsCron = await this.hasRepeatableJob(
      this.predictionsQueue,
      "daily-predictions-cron",
    );

    if (!hasDailyPredictionsCron) {
      await this.predictionsQueue.add(
        "generate-daily",
        {},
        {
          repeat: {
            cron: "0 1 * * *", // Daily at 1am
          },
          jobId: "daily-predictions-cron",
        },
      );
    }

    // Queue Percentiles: Daily at 2am (after midnight + buffer)
    const hasPercentilesCron = await this.hasRepeatableJob(
      this.analyticsQueue,
      "percentiles-cron",
    );

    if (!hasPercentilesCron) {
      await this.analyticsQueue.add(
        "calculate-percentiles",
        {},
        {
          repeat: {
            cron: "0 2 * * *", // Daily at 2am
          },
          jobId: "percentiles-cron",
        },
      );
    }

    // Seasonal Detection: Daily at 2:30am (after percentiles)
    const hasSeasonalCron = await this.hasRepeatableJob(
      this.analyticsQueue,
      "seasonal-detection-cron",
    );
    if (!hasSeasonalCron) {
      await this.analyticsQueue.add(
        "detect-seasonal",
        {},
        {
          repeat: { cron: "30 2 * * *" },
          jobId: "seasonal-detection-cron",
        },
      );
    }

    // Wartezeiten Opening Times: Daily at 6am
    const hasWartezeitenScheduleCron = await this.hasRepeatableJob(
      this.wartezeitenScheduleQueue,
      "wartezeiten-schedule-cron",
    );

    if (!hasWartezeitenScheduleCron) {
      await this.wartezeitenScheduleQueue.add(
        "fetch-opening-times",
        {},
        {
          repeat: {
            cron: "0 6 * * *", // Daily at 6am
          },
          jobId: "wartezeiten-schedule-cron",
        },
      );
    }

    // ML Monitoring Jobs
    {
      // Feature drift detection (daily at 2am)
      const hasFeatureDriftCron = await this.hasRepeatableJob(
        this.mlMonitoringQueue,
        "feature-drift-cron",
      );
      if (!hasFeatureDriftCron) {
        await this.mlMonitoringQueue.add(
          "detect-feature-drift",
          {},
          {
            repeat: {
              cron: "0 2 * * *", // Daily at 2am
            },
            jobId: "feature-drift-cron",
          },
        );
      }

      // Alert check (hourly)
      const hasAlertCheckCron = await this.hasRepeatableJob(
        this.mlMonitoringQueue,
        "alert-check-cron",
      );
      if (!hasAlertCheckCron) {
        await this.mlMonitoringQueue.add(
          "check-alerts",
          {},
          {
            repeat: {
              cron: "0 * * * *", // Every hour
            },
            jobId: "alert-check-cron",
          },
        );
      }

      // Anomaly detection (daily at 3am)
      const hasAnomalyDetectionCron = await this.hasRepeatableJob(
        this.mlMonitoringQueue,
        "anomaly-detection-cron",
      );
      if (!hasAnomalyDetectionCron) {
        await this.mlMonitoringQueue.add(
          "detect-anomalies",
          {},
          {
            repeat: {
              cron: "0 3 * * *", // Daily at 3am
            },
            jobId: "anomaly-detection-cron",
          },
        );
      }

      // Cleanup (daily at 4am)
      const hasCleanupCron = await this.hasRepeatableJob(
        this.mlMonitoringQueue,
        "cleanup-cron",
      );
      if (!hasCleanupCron) {
        await this.mlMonitoringQueue.add(
          "cleanup",
          {},
          {
            repeat: {
              cron: "0 4 * * *", // Daily at 4am
            },
            jobId: "cleanup-cron",
          },
        );
      }
    }

    // Stats: Update current day (Hourly)
    const hasStatsTodayCron = await this.hasRepeatableJob(
      this.statsQueue,
      "stats-today-cron",
    );
    if (!hasStatsTodayCron) {
      await this.statsQueue.add(
        "update-today-stats",
        {},
        {
          repeat: {
            cron: "0 * * * *", // Hourly at minute 0
          },
          jobId: "stats-today-cron",
        },
      );
    }

    // Stats: Finalize yesterday (Daily at 1am)
    const hasStatsYesterdayCron = await this.hasRepeatableJob(
      this.statsQueue,
      "stats-yesterday-cron",
    );
    if (!hasStatsYesterdayCron) {
      await this.statsQueue.add(
        "finalize-yesterday-stats",
        {},
        {
          repeat: {
            cron: "0 1 * * *", // Daily at 1am
          },
          jobId: "stats-yesterday-cron",
        },
      );
    }

    // P50 Baseline: Daily at 3am (after percentile calculation at 2am)
    const hasP50ParkBaselineCron = await this.hasRepeatableJob(
      this.p50BaselineQueue,
      "p50-park-baseline-cron",
    );
    if (!hasP50ParkBaselineCron) {
      await this.p50BaselineQueue.add(
        "calculate-park-baselines",
        {},
        {
          repeat: {
            cron: "0 3 * * *", // Daily at 3am
          },
          jobId: "p50-park-baseline-cron",
        },
      );
    }

    // P50 Attraction Baseline: Daily at 4am (after park baselines)
    const hasP50AttrBaselineCron = await this.hasRepeatableJob(
      this.p50BaselineQueue,
      "p50-attraction-baseline-cron",
    );
    if (!hasP50AttrBaselineCron) {
      await this.p50BaselineQueue.add(
        "calculate-attraction-baselines",
        {},
        {
          repeat: {
            cron: "0 4 * * *", // Daily at 4am
          },
          jobId: "p50-attraction-baseline-cron",
        },
      );
    }

    // GeoIP (GeoLite2-City): Every 48 hours (0:00 every 2 days)
    const hasGeoipCron = await this.hasRepeatableJob(
      this.geoipUpdateQueue,
      "geoip-update-cron",
    );
    if (!hasGeoipCron) {
      await this.geoipUpdateQueue.add(
        "update-geolite2-city",
        {},
        {
          repeat: {
            cron: "0 0 */2 * *", // Every 2 days at midnight
          },
          jobId: "geoip-update-cron",
        },
      );
    }

    this.logger.log("🎉 All scheduled jobs registered!");
  }

  /**
   * Check if a repeatable job with the given jobId already exists.
   * If the job exists but its next scheduled run is in the past (stalled),
   * it is removed so the caller re-registers it with a fresh schedule.
   */
  private async hasRepeatableJob(
    queue: Queue,
    jobId: string,
    expectedCron?: string,
  ): Promise<boolean> {
    const repeatableJobs = await queue.getRepeatableJobs();
    const existing = repeatableJobs.find((job) => job.id === jobId);

    if (!existing) return false;

    // If the cron expression changed, remove and re-register with the new schedule.
    if (expectedCron && existing.cron !== expectedCron) {
      this.logger.warn(
        `Repeatable job "${jobId}" has outdated cron "${existing.cron}" (expected "${expectedCron}"), re-registering`,
      );
      await queue.removeRepeatableByKey(existing.key);
      return false;
    }

    // If the next run is more than 2 minutes in the past the job is stalled —
    // remove it so it gets re-registered with the current cron schedule.
    const STALL_THRESHOLD_MS = 2 * 60 * 1000;
    if (existing.next && existing.next < Date.now() - STALL_THRESHOLD_MS) {
      this.logger.warn(
        `Repeatable job "${jobId}" is overdue (next: ${new Date(existing.next).toISOString()}), removing stale entry for re-registration`,
      );
      await queue.removeRepeatableByKey(existing.key);
      return false;
    }

    return true;
  }
}
