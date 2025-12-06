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
 * - ml-training: Daily at 6am (retrain model with new data)
 * - prediction-accuracy: Every hour (compare predictions with actuals)
 * - occupancy-calculation: Every 15 minutes (placeholder for Phase 5)
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
  ) {}

  async onModuleInit(): Promise<void> {
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
        "fetch-all-parks",
        {},
        {
          repeat: {
            cron: "0 3 * * *", // Daily at 3am
          },
          jobId: "park-metadata-cron",
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

    // Weather: Every 12 hours (0:00 and 12:00)
    const hasWeatherCron = await this.hasRepeatableJob(
      this.weatherQueue,
      "weather-cron",
    );

    if (!hasWeatherCron) {
      await this.weatherQueue.add(
        "fetch-weather",
        {},
        {
          repeat: {
            cron: "0 */12 * * *", // Every 12 hours (0:00 and 12:00)
          },
          jobId: "weather-cron",
        },
      );
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

    // Prediction Accuracy: Every hour (Phase 5.6)
    const hasPredictionAccuracyCron = await this.hasRepeatableJob(
      this.predictionAccuracyQueue,
      "prediction-accuracy-cron",
    );

    if (!hasPredictionAccuracyCron) {
      await this.predictionAccuracyQueue.add(
        "compare-accuracy",
        {},
        {
          repeat: {
            cron: "0 * * * *", // Every hour
          },
          jobId: "prediction-accuracy-cron",
        },
      );
    }

    // Hourly Predictions: Every hour at :15
    const hasHourlyPredictionsCron = await this.hasRepeatableJob(
      this.predictionsQueue,
      "hourly-predictions-cron",
    );

    if (!hasHourlyPredictionsCron) {
      await this.predictionsQueue.add(
        "generate-hourly",
        {},
        {
          repeat: {
            cron: "15 * * * *", // Every hour at :15 (after wait times sync)
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

    this.logger.log("🎉 All scheduled jobs registered!");
  }

  /**
   * Check if a repeatable job with the given jobId already exists.
   */
  private async hasRepeatableJob(
    queue: Queue,
    jobId: string,
  ): Promise<boolean> {
    const repeatableJobs = await queue.getRepeatableJobs();
    return repeatableJobs.some((job) => job.id === jobId);
  }
}
