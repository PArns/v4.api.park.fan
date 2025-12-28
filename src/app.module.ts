import { Module } from "@nestjs/common";
import { ConfigModule } from "@nestjs/config";
import { TypeOrmModule } from "@nestjs/typeorm";
import { typeOrmConfig } from "./config/typeorm.config";
import { DatabaseModule } from "./database/database.module";
import { QueuesModule } from "./queues/queues.module";
import { HealthModule } from "./health/health.module";
import { DestinationsModule } from "./destinations/destinations.module";
import { ParksModule } from "./parks/parks.module";
import { AttractionsModule } from "./attractions/attractions.module";
import { ShowsModule } from "./shows/shows.module";
import { RestaurantsModule } from "./restaurants/restaurants.module";
import { QueueDataModule } from "./queue-data/queue-data.module";
import { HolidaysModule } from "./holidays/holidays.module";
import { DateFeaturesModule } from "./date-features/date-features.module";
import { AnalyticsModule } from "./analytics/analytics.module";
import { MLModule } from "./ml/ml.module";
import { SearchModule } from "./search/search.module";
import { RedisModule } from "./common/redis/redis.module";
import { DiscoveryModule } from "./discovery/discovery.module";
import { LocationModule } from "./location/location.module";
import { AdminModule } from "./admin/admin.module";
import { AppController } from "./app.controller";
import { AppService } from "./app.service";

@Module({
  imports: [
    // Global config module
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ".env",
      cache: true,
    }),

    // Redis
    RedisModule,

    // TypeORM with async config
    TypeOrmModule.forRootAsync(typeOrmConfig),

    // Database utilities
    DatabaseModule,

    // Core modules
    QueuesModule,
    HealthModule,

    // Feature modules (order matters for route precedence)
    QueueDataModule, // Must come before ParksModule for /parks/:slug/wait-times route
    HolidaysModule,
    DestinationsModule,
    ParksModule,
    AttractionsModule,
    ShowsModule,
    RestaurantsModule,
    DateFeaturesModule,

    // Phase 5: Analytics & ML
    AnalyticsModule,
    MLModule,

    // Phase 6.3: Search & Filtering
    SearchModule,

    // Geographic Discovery
    LocationModule,
    DiscoveryModule,

    // Admin utilities
    AdminModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule { }
