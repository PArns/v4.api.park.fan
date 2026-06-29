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
 * - geoip-update: Every 48 hours (GeoLite2-City for nearby endpoint)
 */
@Injectable()
export class QueueSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(QueueSchedulerService.name);

  constructor(
    @InjectQueue("wait-times") private waitTimesQueue: Queue,
    @InjectQueue("park-metadata") private parkMetadataQueue: Queue,
    @InjectQueue("children-metadata") private childrenQueue: Queue, // Phase 6.2: Combined
    @InjectQueue("weather") private weatherQueue: Queue,
    @InjectQueue("weather-warnings")
    private weatherWarningsQueue: Queue,
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
    @InjectQueue("p50-baseline") private p50BaselineQueue: Queue, // P50 + P90 baseline
    @InjectQueue("attraction-hourly-history")
    private attractionHourlyHistoryQueue: Queue,
    @InjectQueue("rope-drop") private ropeDropQueue: Queue,
    @InjectQueue("typical-waits") private typicalWaitsQueue: Queue,
    @InjectQueue("geoip-update") private geoipUpdateQueue: Queue,
    @InjectQueue("nf-training") private nfTrainingQueue: Queue,
    @InjectQueue("pcn-shadow") private pcnShadowQueue: Queue,
    @InjectQueue("shape-shadow") private shapeShadowQueue: Queue,
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

    // Calendar warmup: every 12h (08:00 + 20:00 UTC, both AFTER the nightly CatBoost
    // retrain at 06:00 and TFT at 07:00) so /calendar serves fresh predictions and users
    // never hit the ~15s cold daily-ML path. force=true evicts the daily ML + month caches
    // so each run actually refreshes (weather syncs every 12h too). TTL_DAILY_PREDICTIONS
    // (13h) > 12h interval so the cache never expires between two background refreshes.
    const CALENDAR_WARMUP_CRON = "0 8,20 * * *";
    const hasCalendarWarmupCron = await this.hasRepeatableJob(
      this.parkMetadataQueue,
      "warmup-calendar-daily-cron",
      CALENDAR_WARMUP_CRON,
    );
    if (!hasCalendarWarmupCron) {
      await this.parkMetadataQueue.add(
        "warmup-calendar-daily",
        {},
        {
          repeat: {
            cron: CALENDAR_WARMUP_CRON,
          },
          jobId: "warmup-calendar-daily-cron",
        },
      );
    }

    // Popularity decay: once per day (midnight) so the prewarm ranking tracks
    // recent demand (~1-2 week window) instead of all-time request totals.
    const hasPopularityDecayCron = await this.hasRepeatableJob(
      this.parkMetadataQueue,
      "popularity-decay-cron",
    );
    if (!hasPopularityDecayCron) {
      await this.parkMetadataQueue.add(
        "decay-popularity",
        {},
        {
          repeat: {
            cron: "0 0 * * *", // Daily at midnight
          },
          jobId: "popularity-decay-cron",
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

    // Weather current-only sync: Every 6 hours (live conditions for today).
    // Real-time conditions are served by the nowcast (15-min cache), so the DB
    // "current" record only needs periodic refresh — this respects the quota.
    const hasWeatherCurrentCron = await this.hasRepeatableJob(
      this.weatherQueue,
      "weather-current-cron",
      "0 */6 * * *",
    );

    if (!hasWeatherCurrentCron) {
      await this.weatherQueue.add(
        "fetch-weather",
        { currentOnly: true },
        {
          repeat: { cron: "0 */6 * * *" },
          jobId: "weather-current-cron",
        },
      );
    }

    // Weather warnings: every 15 minutes (severe-weather warnings change fast,
    // unlike the 12h forecast). MeteoGate → DWD/MeteoAlarm, German+EU parks.
    const hasWeatherWarningsCron = await this.hasRepeatableJob(
      this.weatherWarningsQueue,
      "weather-warnings-cron",
      "*/15 * * * *",
    );

    if (!hasWeatherWarningsCron) {
      await this.weatherWarningsQueue.add(
        "sync-warnings",
        {},
        {
          repeat: { cron: "*/15 * * * *" },
          jobId: "weather-warnings-cron",
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
          // Never retry: training is a ~45min CatBoost job. The global default
          // (attempts:3 + backoff) would re-POST /train and re-run the whole
          // job up to 3× on a transient failure — a retry storm. Matches the
          // train-nf / score-comparison guards.
          attempts: 1,
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

    // TFT (NeuralForecast) training: Daily at 03:00 UTC — safely before CatBoost
    // (06:00 UTC). NF takes ~2.5h → finishes ~05:30, leaving a ~30min gap before
    // CatBoost starts. No memory overlap on the shared 28GB host.
    const hasNfTrainingCron = await this.hasRepeatableJob(
      this.nfTrainingQueue,
      "nf-training-cron",
    );

    if (!hasNfTrainingCron) {
      await this.nfTrainingQueue.add(
        "train-nf",
        {},
        {
          repeat: { cron: "0 3 * * *" }, // Daily at 03:00 UTC (before CatBoost at 06:00)
          jobId: "nf-training-cron",
          attempts: 1, // long job + overlap-guarded; never retry-stack a 2nd train
        },
      );
    }

    // Model comparison scoreboard: Daily at 7:00am UTC — after NF (~05:30) and
    // CatBoost (~06:15) have both completed their forward forecasts.
    const hasNfScoreCron = await this.hasRepeatableJob(
      this.nfTrainingQueue,
      "nf-score-comparison-cron",
    );

    if (!hasNfScoreCron) {
      await this.nfTrainingQueue.add(
        "score-comparison",
        {},
        {
          repeat: { cron: "0 7 * * *" }, // Daily at 07:00 UTC
          jobId: "nf-score-comparison-cron",
          attempts: 1, // idempotent upsert by (targetDate, model) — no retry needed
        },
      );
    }

    // PCN intraday shadow (design doc §12). Runs in the shadow — writes pcn_forecasts /
    // pcn_intraday_comparisons only; CatBoost stays the served champion until a gate win.
    // train-pcn at 08:30 UTC: after TFT (starts 03:00, ~2.5h) and CatBoost (starts 06:00,
    // ~45min) have both finished, so the shared-GPU training spikes never overlap.
    const hasPcnTrainCron = await this.hasRepeatableJob(
      this.pcnShadowQueue,
      "pcn-train-cron",
    );
    if (!hasPcnTrainCron) {
      await this.pcnShadowQueue.add(
        "train-pcn",
        {},
        {
          repeat: { cron: "30 8 * * *" }, // Daily 08:30 UTC (after TFT + CatBoost)
          jobId: "pcn-train-cron",
          attempts: 1, // long, overlap-guarded — never retry-stack a 2nd train
        },
      );
    }

    // forecast-pcn every 15 min: re-infer with the current state → durable pcn_forecasts.
    const hasPcnForecastCron = await this.hasRepeatableJob(
      this.pcnShadowQueue,
      "pcn-forecast-cron",
      "*/15 * * * *",
    );
    if (!hasPcnForecastCron) {
      await this.pcnShadowQueue.add(
        "forecast-pcn",
        {},
        {
          repeat: { cron: "*/15 * * * *" },
          jobId: "pcn-forecast-cron",
        },
      );
    }

    // score-pcn hourly: score matured forecasts vs actuals + CatBoost (idempotent upsert).
    const hasPcnScoreCron = await this.hasRepeatableJob(
      this.pcnShadowQueue,
      "pcn-score-cron",
    );
    if (!hasPcnScoreCron) {
      await this.pcnShadowQueue.add(
        "score-pcn",
        {},
        {
          repeat: { cron: "0 * * * *" }, // hourly
          jobId: "pcn-score-cron",
          attempts: 1,
        },
      );
    }

    // Shape day-curve shadow (design §6–8). Runs in the shadow — writes shape_forecasts /
    // shape_comparisons only; CatBoost stays the served champion. Daily horizon, so the
    // crons are daily and ordered AFTER the daily forecast (the level source) + a margin.
    const hasShapeBuildCron = await this.hasRepeatableJob(
      this.shapeShadowQueue,
      "shape-build-cron",
    );
    if (!hasShapeBuildCron) {
      await this.shapeShadowQueue.add(
        "build-shape",
        {},
        {
          repeat: { cron: "0 9 * * *" }, // 09:00 UTC — after the nightly daily forecast
          jobId: "shape-build-cron",
          attempts: 1,
        },
      );
    }
    const hasShapeForecastCron = await this.hasRepeatableJob(
      this.shapeShadowQueue,
      "shape-forecast-cron",
    );
    if (!hasShapeForecastCron) {
      await this.shapeShadowQueue.add(
        "forecast-shape",
        {},
        {
          repeat: { cron: "30 9 * * *" }, // 09:30 UTC — after build
          jobId: "shape-forecast-cron",
          attempts: 1,
        },
      );
    }
    const hasShapeScoreCron = await this.hasRepeatableJob(
      this.shapeShadowQueue,
      "shape-score-cron",
    );
    if (!hasShapeScoreCron) {
      await this.shapeShadowQueue.add(
        "score-shape",
        {},
        {
          repeat: { cron: "0 10 * * *" }, // 10:00 UTC — score matured forecasts
          jobId: "shape-score-cron",
          attempts: 1,
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

    // Prediction Accuracy Stats: Hourly (aggregate MAE per attraction)
    // compare-accuracy runs every 15min → new COMPLETED rows every 15min.
    // Hourly aggregation keeps attraction_accuracy_stats and the Redis badge
    // cache (30min TTL) in sync: worst-case badge staleness = 1.5h.
    const hasAggregateStatsCron = await this.hasRepeatableJob(
      this.predictionAccuracyQueue,
      "aggregate-stats-cron",
      "0 * * * *",
    );

    if (!hasAggregateStatsCron) {
      await this.predictionAccuracyQueue.add(
        "aggregate-stats",
        {},
        {
          repeat: {
            cron: "0 * * * *", // Every hour
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

    // Attraction hourly history: daily at 4:30 AM (after attraction P50/P90
    // baselines finish). Pre-aggregates yesterday's per-attraction 15-min
    // slot breakdown so the history endpoint can serve a 30-day chart
    // with one SELECT instead of 30× PERCENTILE_CONT scans.
    const hasHourlyHistoryCron = await this.hasRepeatableJob(
      this.attractionHourlyHistoryQueue,
      "attraction-hourly-history-cron",
    );
    if (!hasHourlyHistoryCron) {
      await this.attractionHourlyHistoryQueue.add(
        "calculate-yesterday-hourly-history",
        {},
        {
          repeat: {
            cron: "30 4 * * *", // Daily at 4:30am
          },
          jobId: "attraction-hourly-history-cron",
        },
      );
    }

    // Rope-drop recommendations: daily at 5:15 AM (after attraction-hourly-history
    // at 4:30 finishes writing yesterday's slots). Daily recompute on a trailing
    // window = the "window adapts to the current season" requirement.
    const hasRopeDropCron = await this.hasRepeatableJob(
      this.ropeDropQueue,
      "rope-drop-cron",
    );
    if (!hasRopeDropCron) {
      await this.ropeDropQueue.add(
        "calculate-rope-drop",
        {},
        {
          repeat: {
            cron: "15 5 * * *", // Daily at 5:15am
          },
          jobId: "rope-drop-cron",
        },
      );
    }

    // Typical-waits: daily at 5:30 AM (after rope-drop at 5:15 + the hourly-history
    // / percentile rebuild). Precomputes P50/P90 peak-wait stats per headliner so
    // the park response (SSR ride-page shell) serves them without a percentile scan.
    const hasTypicalWaitsCron = await this.hasRepeatableJob(
      this.typicalWaitsQueue,
      "typical-waits-cron",
    );
    if (!hasTypicalWaitsCron) {
      await this.typicalWaitsQueue.add(
        "calculate-typical-waits",
        {},
        {
          repeat: {
            cron: "30 5 * * *", // Daily at 5:30am
          },
          jobId: "typical-waits-cron",
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
