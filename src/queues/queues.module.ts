import { Module } from "@nestjs/common";
import { BullModule } from "@nestjs/bull";
import { ConfigModule } from "@nestjs/config";
import { TypeOrmModule } from "@nestjs/typeorm";
import { getRedisConfig } from "../config/redis.config";
import { BULL_QUEUE_REGISTRATIONS, bullPrefix } from "./queue-registrations";
import { QueueBootstrapService } from "./services/queue-bootstrap.service";
import { QueueSchedulerService } from "./services/queue-scheduler.service";
import { CacheWarmupService } from "./services/cache-warmup.service";
import { ParkMetadataProcessor } from "./processors/park-metadata.processor";
import { ChildrenMetadataProcessor } from "./processors/children-metadata.processor";
import { SixFlagsHeightsProcessor } from "./processors/six-flags-heights.processor";
import { RideStatsProcessor } from "./processors/ride-stats.processor";
import { CuratedDataProcessor } from "./processors/curated-data.processor";
import { SixFlagsClient } from "../external-apis/six-flags/six-flags.client";
import { WaitTimesProcessor } from "./processors/wait-times.processor";
import { LiveCacheWarmupProcessor } from "./processors/live-cache-warmup.processor";
import { WeatherProcessor } from "./processors/weather.processor";
import { WeatherWarningsProcessor } from "./processors/weather-warnings.processor";
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
import { DowntimeReconstructionProcessor } from "./processors/downtime-reconstruction.processor";
import { PushNotificationProcessor } from "./processors/push-notification.processor";
import { TripsMaintenanceProcessor } from "./processors/trips-maintenance.processor";
import { ForecastArchiveProcessor } from "./processors/forecast-archive.processor";
import { ShowPatternProcessor } from "./processors/show-pattern.processor";
import { RopeDropProcessor } from "./processors/rope-drop.processor";
import { ParkDayOperationProcessor } from "./processors/park-day-operation.processor";
import { TypicalWaitsProcessor } from "./processors/typical-waits.processor";
import { GeoipUpdateProcessor } from "./processors/geoip-update.processor";
import { NfForecastProcessor } from "./processors/nf-forecast.processor";
import { PcnShadowProcessor } from "./processors/pcn-shadow.processor";
import { ShapeShadowProcessor } from "./processors/shape-shadow.processor";
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
import { RevalidationModule } from "../common/revalidation/revalidation.module";
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
import { PushModule } from "../push/push.module";
import { TripsModule } from "../trips/trips.module";
import { RideAlertsModule } from "../ride-alerts/ride-alerts.module";
import { ShowFollowsModule } from "../show-follows/show-follows.module";

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
          prefix: bullPrefix(),
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
    BullModule.registerQueue(...BULL_QUEUE_REGISTRATIONS),

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
    PushModule, // Web-push subscriptions and sending
    TripsModule, // The stored plans the notification job walks
    RideAlertsModule, // Wait-time alerts — checked from WaitTimesProcessor
    ShowFollowsModule, // Followed shows — the push-notifications job's other half
    RedisModule, // For cache warmup service
    RevalidationModule, // Frontend on-demand revalidation (best-days webhook)
    GeoipModule,
  ],
  providers: [
    QueueBootstrapService,
    QueueSchedulerService,
    CacheWarmupService, // Cache warmup service
    ParkMetadataProcessor,
    ChildrenMetadataProcessor, // Phase 6.2: Combined processor
    SixFlagsHeightsProcessor,
    RideStatsProcessor,
    SixFlagsClient,
    CuratedDataProcessor,
    EntityMappingsProcessor, // Phase 6.6.3: Multi-source mapping processor
    WaitTimesProcessor,
    LiveCacheWarmupProcessor, // Cache warmup after each sync (PAR-822)
    WeatherProcessor,
    WeatherWarningsProcessor,
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
    ParkDayOperationProcessor, // Measured-operation verdict per park-day (daily)
    DowntimeReconstructionProcessor, // Outage intervals + exposure + profiles
    PushNotificationProcessor, // The five-minute tick that sends "next up"
    TripsMaintenanceProcessor, // Daily sweep of expired stored plans
    ForecastArchiveProcessor, // Forward archive of served curves + scoring (PAR-831)
    ShowPatternProcessor, // Nightly per-weekday showtime patterns
    RopeDropProcessor, // Rope-drop recommendations (daily)
    TypicalWaitsProcessor, // Typical P50/P90 peak-wait stats (daily)
    GeoipUpdateProcessor,
    NfForecastProcessor, // TFT train+forecast + TFT-vs-CatBoost scoreboard
    PcnShadowProcessor, // PCN intraday shadow: train + forecast + score
    ShapeShadowProcessor, // Shape day-curve shadow: build + forecast + score
  ],
  exports: [BullModule], // Export for use in other modules
})
export class QueuesModule {}
