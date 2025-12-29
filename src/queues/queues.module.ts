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
import { AttractionsMetadataProcessor } from "./processors/attractions-metadata.processor";
import { ShowsMetadataProcessor } from "./processors/shows-metadata.processor";
import { RestaurantsMetadataProcessor } from "./processors/restaurants-metadata.processor";
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
import { RedisModule } from "../common/redis/redis.module";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Park } from "../parks/entities/park.entity";
import { MLModel } from "../ml/entities/ml-model.entity";
import { ExternalEntityMapping } from "../database/entities/external-entity-mapping.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { QueueDataAggregate } from "../analytics/entities/queue-data-aggregate.entity";

@Module({
  imports: [
    ConfigModule,
    TypeOrmModule.forFeature([
      Park,
      Attraction,
      MLModel,
      ExternalEntityMapping,
      QueueData,
      QueueDataAggregate,
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
      { name: "attractions-metadata" }, // DEPRECATED: Use children-metadata instead
      { name: "shows-metadata" }, // DEPRECATED: Use children-metadata instead
      { name: "restaurants-metadata" }, // DEPRECATED: Use children-metadata instead
      { name: "occupancy-calculation" },
      { name: "weather" },
      { name: "weather-historical" },
      { name: "holidays" },
      { name: "ml-training" },
      { name: "prediction-accuracy" },
      { name: "predictions" },
      { name: "park-enrichment" },
      { name: "analytics" },
      { name: "wartezeiten-schedule" },
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
    RedisModule, // For cache warmup service
  ],
  providers: [
    QueueBootstrapService,
    QueueSchedulerService,
    CacheWarmupService, // Cache warmup service
    ParkMetadataProcessor,
    ChildrenMetadataProcessor, // Phase 6.2: Combined processor
    EntityMappingsProcessor, // Phase 6.6.3: Multi-source mapping processor
    AttractionsMetadataProcessor, // DEPRECATED: Keep for backward compatibility
    ShowsMetadataProcessor, // DEPRECATED: Keep for backward compatibility
    RestaurantsMetadataProcessor, // DEPRECATED: Keep for backward compatibility
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
  ],
  exports: [BullModule], // Export for use in other modules
})
export class QueuesModule {}
