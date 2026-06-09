import { Module } from "@nestjs/common";
import { BullModule } from "@nestjs/bull";
import { ConfigModule } from "@nestjs/config";
import { TypeOrmModule } from "@nestjs/typeorm";
import { getRedisConfig } from "../config/redis.config";
import { QueueBootstrapService } from "./services/queue-bootstrap.service";
import { QueueSchedulerService } from "./services/queue-scheduler.service";
import { CacheWarmupService } from "./services/cache-warmup.service";
import { ParkMetadataProcessor } from "./processors/park-metadata.processor";
import { ChildrenMetadataProcessor } from "./processors/children-metadata.processor";
import { WaitTimesProcessor } from "./processors/wait-times.processor";
import { WeatherProcessor } from "./processors/weather.processor";
import { HolidaysProcessor } from "./processors/holidays.processor";
import { WeatherHistoricalProcessor } from "./processors/weather-historical.processor";
import { MLTrainingProcessor } from "./processors/ml-training.processor";
import { PredictionAccuracyProcessor } from "./processors/prediction-accuracy.processor";
import { PredictionGeneratorProcessor } from "./processors/prediction-generator.processor";
import { ParkEnrichmentProcessor } from "./processors/park-enrichment.processor";
import { EntityMappingsProcessor } from "./processors/entity-mappings.processor";
import { QueuePercentileProcessor } from "./processors/queue-percentile.processor";
import { WartezeitenScheduleProcessor } from "./processors/wartezeiten-schedule.processor";
import { MLMonitoringProcessor } from "./processors/ml-monitoring.processor";
import { P50BaselineProcessor } from "./processors/p50-baseline.processor";
import { AttractionHourlyHistoryProcessor } from "./processors/attraction-hourly-history.processor";
import { RopeDropProcessor } from "./processors/rope-drop.processor";
import { GeoipUpdateProcessor } from "./processors/geoip-update.processor";
import { NfForecastProcessor } from "./processors/nf-forecast.processor";
import { GeoipModule } from "../geoip/geoip.module";
import { ParksModule } from "../parks/parks.module";
import { DestinationsModule } from "../destinations/destinations.module";
import { AttractionsModule } from "../attractions/attractions.module";
import { ShowsModule } from "../shows/shows.module";
import { RestaurantsModule } from "../restaurants/restaurants.module";
import { QueueDataModule } from "../queue-data/queue-data.module";
import { HolidaysModule } from "../holidays/holidays.module";
import { MLModule } from "../ml/ml.module";
import { ThemeParksModule } from "../external-apis/themeparks/themeparks.module";
import { WeatherModule } from "../external-apis/weather/weather.module";
import { GeocodingModule } from "../external-apis/geocoding/geocoding.module";
import { NagerDateModule } from "../external-apis/nager-date/nager-date.module";
import { DataSourcesModule } from "../external-apis/data-sources/data-sources.module";
import { WartezeitenModule } from "../external-apis/wartezeiten/wartezeiten.module";
import { AnalyticsModule } from "../analytics/analytics.module";
import { OpenHolidaysModule } from "../external-apis/open-holidays/open-holidays.module";
import { DiscoveryModule } from "../discovery/discovery.module";
import { SearchModule } from "../search/search.module";
import { RedisModule } from "../common/redis/redis.module";
import { StatsModule } from "../stats/stats.module";
import { PopularityModule } from "../popularity/popularity.module";
import { StatsProcessor } from "./processors/stats.processor";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Park } from "../parks/entities/park.entity";
import { MLModel } from "../ml/entities/ml-model.entity";
import { ExternalEntityMapping } from "../database/entities/external-entity-mapping.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { QueueDataAggregate } from "../analytics/entities/queue-data-aggregate.entity";
import { AttractionAccuracyStats } from "../ml/entities/attraction-accuracy-stats.entity";
import { PredictionAccuracy } from "../ml/entities/prediction-accuracy.entity";
import { AttractionP50Baseline } from "../analytics/entities/attraction-p50-baseline.entity";
import { AttractionP90Baseline } from "../analytics/entities/attraction-p90-baseline.entity";
import { ModelComparison } from "../ml/entities/model-comparison.entity";

@Module({
  imports: [
    ConfigModule,
    TypeOrmModule.forFeature([
      Park,
      Attraction,
      Show,
      MLModel,
      ExternalEntityMapping,
      QueueData,
      QueueDataAggregate,
      AttractionAccuracyStats,
      PredictionAccuracy,
      AttractionP50Baseline,
      AttractionP90Baseline,
      ModelComparison,
    ]),
    // Register Bull queues with Redis connection
    BullModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: () => {
        const redisConfig = getRedisConfig();
        return {
          redis: {
            host: redisConfig.host,
            port: redisConfig.port,
          },
          prefix: process.env.BULL_PREFIX || "parkfan",
          defaultJobOptions: {
            attempts: 3, // Retry failed jobs 3 times
            backoff: {
              type: "exponential",
              delay: 2000, // Start with 2 seconds
            },
            removeOnComplete: 100, // Keep last 100 completed jobs
            removeOnFail: 500, // Keep last 500 failed jobs
          },
        };
      },
    }),

    // Register individual queues
    BullModule.registerQueue(
      { name: "wait-times" },
      { name: "park-metadata" },
      { name: "children-metadata" }, // Phase 6.2: Combined Attractions + Shows + Restaurants
      { name: "entity-mappings" }, // Phase 6.6.3: Multi-source mappings
      { name: "weather" },
      { name: "weather-historical" },
      { name: "holidays" },
      { name: "ml-training" },
      { name: "prediction-accuracy" },
      { name: "predictions" },
      { name: "park-enrichment" },
      { name: "analytics" },
      { name: "wartezeiten-schedule" },
      { name: "ml-monitoring" },
      { name: "stats" },
      {
        // P50 + P90 baseline calculation. calculate-attraction-baselines
        // does 113 PERCENTILE_CONT scans over 548-day queue_data — a single
        // run takes 3-6 min. Bull's default lockDuration of 30s would
        // wrongly mark the worker stalled mid-run, so bump it to 10 min
        // with proportional renew interval. Same queue handles park-level
        // and backfill jobs which are much shorter; the higher limit is
        // harmless for them.
        name: "p50-baseline",
        settings: {
          lockDuration: 600000, // 10 min
          lockRenewTime: 300000, // renew every 5 min
        },
      },
      {
        // Hourly history backfills can iterate 30+ days × 100+ parks — same
        // stall risk as p50-baseline. Yesterday-only cron is fast (<5s)
        // but the backfill path needs headroom.
        name: "attraction-hourly-history",
        settings: {
          lockDuration: 600000,
          lockRenewTime: 300000,
        },
      },
      { name: "geoip-update" }, // GeoLite2-City every 48h
      { name: "nf-training" }, // TFT train+forecast + TFT-vs-CatBoost scoreboard
      {
        // Rope-drop: one LATERAL query per park over hourly-history slots +
        // pure aggregation. Lighter than p50-baseline (no fresh PERCENTILE
        // scan) but still iterates all parks — give it the same headroom so a
        // batch run is never flagged stalled.
        name: "rope-drop",
        settings: {
          lockDuration: 600000, // 10 min
          lockRenewTime: 300000, // renew every 5 min
        },
      },
    ),

    // Feature modules for processors
    ParksModule,
    DestinationsModule,
    AttractionsModule,
    ShowsModule,
    RestaurantsModule,
    QueueDataModule,
    HolidaysModule,
    MLModule,
    ThemeParksModule,
    WeatherModule,
    DataSourcesModule,
    GeocodingModule,
    NagerDateModule,
    OpenHolidaysModule,
    WartezeitenModule,
    AnalyticsModule,
    DiscoveryModule,
    StatsModule,
    SearchModule,
    PopularityModule,
    RedisModule, // For cache warmup service
    GeoipModule,
  ],
  providers: [
    QueueBootstrapService,
    QueueSchedulerService,
    CacheWarmupService, // Cache warmup service
    ParkMetadataProcessor,
    ChildrenMetadataProcessor, // Phase 6.2: Combined processor
    EntityMappingsProcessor, // Phase 6.6.3: Multi-source mapping processor
    WaitTimesProcessor,
    WeatherProcessor,
    WeatherHistoricalProcessor,
    HolidaysProcessor,
    MLTrainingProcessor,
    PredictionAccuracyProcessor,
    PredictionGeneratorProcessor,
    ParkEnrichmentProcessor,
    QueuePercentileProcessor,
    WartezeitenScheduleProcessor,
    MLMonitoringProcessor,
    StatsProcessor,
    P50BaselineProcessor, // P50 + P90 baseline processor
    AttractionHourlyHistoryProcessor, // Per-day hourly history rollup
    RopeDropProcessor, // Rope-drop recommendations (daily)
    GeoipUpdateProcessor,
    NfForecastProcessor, // TFT train+forecast + TFT-vs-CatBoost scoreboard
  ],
  exports: [BullModule], // Export for use in other modules
})
export class QueuesModule {}
