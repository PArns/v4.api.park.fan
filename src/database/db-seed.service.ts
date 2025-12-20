import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { InjectQueue } from "@nestjs/bull";
import { Repository, MoreThan } from "typeorm";
import { Queue } from "bull";
import { Park } from "../parks/entities/park.entity";
import { Holiday } from "../holidays/entities/holiday.entity";
import { WeatherData } from "../parks/entities/weather-data.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { MLModel } from "../ml/entities/ml-model.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";

/**
 * Database Seeding Service
 *
 * Intelligently populates database with ALL missing data on startup.
 * Checks each data type individually and only syncs what's missing.
 *
 * Checks:
 * - Parks: Core entity (must exist)
 * - Holidays: Year-round data (must exist for ML)
 * - Weather: Forecast data (should be recent/future)
 * - Wait Times: Live data (initial trigger)
 *
 * Strategy:
 * 1. Check each data type independently
 * 2. Queue sync jobs for missing/stale data with proper priorities
 * 3. Jobs run asynchronously via Bull queues
 */
@Injectable()
export class DbSeedService implements OnModuleInit {
  private readonly logger = new Logger(DbSeedService.name);

  constructor(
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    @InjectRepository(Holiday)
    private holidayRepository: Repository<Holiday>,
    @InjectRepository(WeatherData)
    private weatherRepository: Repository<WeatherData>,
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
    @InjectRepository(MLModel)
    private mlModelRepository: Repository<MLModel>,
    @InjectRepository(ScheduleEntry)
    private scheduleRepository: Repository<ScheduleEntry>,
    @InjectQueue("park-metadata")
    private parkMetadataQueue: Queue,
    @InjectQueue("children-metadata")
    private childrenMetadataQueue: Queue,
    @InjectQueue("weather")
    private weatherQueue: Queue,
    @InjectQueue("holidays")
    private holidaysQueue: Queue,
    @InjectQueue("wait-times")
    private waitTimesQueue: Queue,
    @InjectQueue("ml-training")
    private mlTrainingQueue: Queue,
  ) {}

  async onModuleInit(): Promise<void> {
    // Delay seeding by 10 minutes to ensure all dependencies (Redis, Postgres, Bull queues)
    // are fully initialized and prevent race conditions during server startup
    const SEED_DELAY_MS = 10 * 60 * 1000; // 10 minutes

    this.logger.log(
      `⏰ Database seeding will start in ${SEED_DELAY_MS / 1000 / 60} minutes to ensure all dependencies are ready`,
    );

    // Run async to not block app startup
    setTimeout(() => {
      this.checkAndSeed().catch((err) => {
        this.logger.error("Failed to auto-seed database", err);
      });
    }, SEED_DELAY_MS);
  }

  /**
   * Intelligently check all data types and seed what's missing
   */
  private async checkAndSeed(): Promise<void> {
    this.logger.log("🔍 Checking database for missing data...");

    const checks = await this.performDataChecks();
    const jobsToQueue: Array<{
      name: string;
      priority: number;
      fn: () => Promise<void>;
    }> = [];

    // 1. Parks (highest priority - dependency for everything else)
    if (checks.needsParks) {
      this.logger.log("❌ Parks: Missing");
      jobsToQueue.push({
        name: "Parks",
        priority: 10,
        fn: async () => {
          await this.parkMetadataQueue.add(
            "sync-all-parks",
            {},
            { priority: 10 },
          );
        },
      });
    } else {
      this.logger.log(`✅ Parks: ${checks.parkCount} found`);
    }

    // 2. Holidays (critical for ML)
    if (checks.needsHolidays) {
      this.logger.log("❌ Holidays: Missing or incomplete");
      jobsToQueue.push({
        name: "Holidays",
        priority: 7,
        fn: async () => {
          await this.holidaysQueue.add("fetch-holidays", {}, { priority: 7 });
        },
      });
    } else {
      this.logger.log(`✅ Holidays: ${checks.holidayCount} found`);
    }

    // 3. Weather (important for predictions)
    if (checks.needsWeather) {
      this.logger.log("❌ Weather: Missing or stale");
      jobsToQueue.push({
        name: "Weather",
        priority: 8,
        fn: async () => {
          await this.weatherQueue.add("fetch-weather", {}, { priority: 8 });
        },
      });
    } else {
      this.logger.log(
        `✅ Weather: ${checks.weatherCount} records (latest: ${checks.latestWeatherDate?.toString().split("T")[0] || "N/A"})`,
      );
    }

    // 4. Wait Times (initial data collection)
    if (checks.needsWaitTimes) {
      this.logger.log("❌ Wait Times: No recent data");
      jobsToQueue.push({
        name: "Wait Times",
        priority: 6,
        fn: async () => {
          await this.waitTimesQueue.add(
            "fetch-wait-times",
            {},
            { priority: 6 },
          );
        },
      });
    } else {
      this.logger.log(
        `✅ Wait Times: ${checks.queueDataCount} records (latest: ${checks.latestQueueTime?.toString() || "N/A"})`,
      );
    }

    // 5. Schedules/Gaps (ensure holidays are applied)
    if (checks.needsSchedules) {
      this.logger.log("❌ Schedules/Gaps: Missing or incomplete");
      jobsToQueue.push({
        name: "Schedule Gaps",
        priority: 5,
        fn: async () => {
          await this.parkMetadataQueue.add(
            "fill-all-gaps",
            {},
            { priority: 5 },
          );
        },
      });
    } else {
      this.logger.log(`✅ Schedules: ${checks.scheduleCount} entries found`);
    }

    // 6. ML Model (trigger training if no active model exists)
    if (checks.needsMLModel) {
      this.logger.log("❌ ML Model: No active model found");
      jobsToQueue.push({
        name: "ML Training",
        priority: 1, // Low priority - run after all data is collected
        fn: async () => {
          await this.mlTrainingQueue.add(
            "train-model",
            {},
            { priority: 1, delay: 60000 }, // 1 minute delay to ensure data is synced
          );
        },
      });
    } else {
      this.logger.log(
        `✅ ML Model: Active model found (version: ${checks.mlModelVersion})`,
      );
    }

    // Queue jobs if any are needed
    if (jobsToQueue.length > 0) {
      this.logger.log(
        `🌱 Seeding ${jobsToQueue.length} missing data type(s)...`,
      );

      // Sort by priority (highest first)
      jobsToQueue.sort((a, b) => b.priority - a.priority);

      for (const job of jobsToQueue) {
        this.logger.log(`📦 Queueing ${job.name} sync...`);
        await job.fn();
      }

      this.logger.log(
        "✅ Auto-seed jobs queued successfully! Database will be populated shortly.",
      );
    } else {
      this.logger.log("✅ All data present - no seeding needed");
    }
  }

  /**
   * Perform all data existence checks
   */
  private async performDataChecks(): Promise<{
    needsParks: boolean;
    parkCount: number;
    needsHolidays: boolean;
    holidayCount: number;
    needsWeather: boolean;
    weatherCount: number;
    latestWeatherDate: Date | null;
    needsWaitTimes: boolean;
    queueDataCount: number;
    latestQueueTime: Date | null;
    needsMLModel: boolean;
    mlModelVersion: string | null;
    needsSchedules: boolean;
    scheduleCount: number;
  }> {
    // Check Parks
    const parkCount = await this.parkRepository.count();
    const needsParks = parkCount === 0;

    // Check Holidays (need current + next year for predictions)
    const currentYear = new Date().getFullYear();
    const holidayCount = await this.holidayRepository.count();
    const futureHolidays = await this.holidayRepository.count({
      where: {
        date: MoreThan(new Date(`${currentYear}-01-01`)),
      },
    });
    const needsHolidays = holidayCount === 0 || futureHolidays < 100; // Arbitrary threshold

    // Check Weather (should have future forecasts)
    const weatherCount = await this.weatherRepository.count();
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);

    const futureWeather = await this.weatherRepository.count({
      where: {
        date: MoreThan(tomorrow),
      },
    });

    const latestWeather = await this.weatherRepository.findOne({
      where: {},
      order: { date: "DESC" },
      select: ["date"],
    });

    const needsWeather = weatherCount === 0 || futureWeather === 0;

    // Check Wait Times (should have data from last 24h)
    const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const recentQueueData = await this.queueDataRepository.count({
      where: {
        timestamp: MoreThan(oneDayAgo),
      },
    });

    const latestQueue = await this.queueDataRepository.findOne({
      where: {},
      order: { timestamp: "DESC" },
      select: ["timestamp"],
    });

    const needsWaitTimes = recentQueueData === 0;

    // Check ML Model (should have at least one active model)
    const activeModel = await this.mlModelRepository.findOne({
      where: { isActive: true },
      select: ["version"],
    });

    const needsMLModel = !activeModel;

    // Check Schedules
    const scheduleCount = await this.scheduleRepository.count();
    // We expect a baseline of schedule entries for operating parks
    const needsSchedules = scheduleCount < parkCount * 7; // At least one week for each park

    return {
      needsParks,
      parkCount,
      needsHolidays,
      holidayCount,
      needsWeather,
      weatherCount,
      latestWeatherDate: latestWeather?.date || null,
      needsWaitTimes,
      queueDataCount: recentQueueData,
      latestQueueTime: latestQueue?.timestamp || null,
      needsMLModel,
      mlModelVersion: activeModel?.version || null,
      needsSchedules,
      scheduleCount,
    };
  }
}
