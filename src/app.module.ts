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
import { LocationModule } from "./location/location.module";
import { DiscoveryModule } from "./discovery/discovery.module";
import { SitemapModule } from "./sitemap/sitemap.module";
import { GeoipModule } from "./geoip/geoip.module";
import { FavoritesModule } from "./favorites/favorites.module";
import { AdminModule } from "./admin/admin.module";
import { StatsModule } from "./stats/stats.module";
import { PopularityModule } from "./popularity/popularity.module";
import { AppController } from "./app.controller";
import { AppService } from "./app.service";
import { APP_GUARD, APP_INTERCEPTOR } from "@nestjs/core";
import { PopularityInterceptor } from "./popularity/interceptors/popularity.interceptor";
import { ThrottlerModule } from "@nestjs/throttler";
import {
  getThrottlerOptions,
  isThrottlingEnabled,
} from "./common/throttler/throttler.config";
import { CfThrottlerGuard } from "./common/guards/cf-throttler.guard";

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

    // Origin rate limiting (circuit breaker; tunable/disable-able via env).
    // Evaluated after ConfigModule.forRoot() above has loaded .env into
    // process.env, so getThrottlerOptions() sees env-file values too.
    ThrottlerModule.forRoot(getThrottlerOptions()),

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
    StatsModule,
    PopularityModule,
    // Phase 6.3: Search & Filtering
    SearchModule,

    // Geographic Discovery
    GeoipModule,
    LocationModule,
    DiscoveryModule,
    SitemapModule,

    // Favorites
    FavoritesModule,

    // Admin utilities
    AdminModule,
  ],
  controllers: [AppController],
  providers: [
    AppService,
    {
      provide: APP_INTERCEPTOR,
      useClass: PopularityInterceptor,
    },
    // Only register the throttler guard when a positive limit is set
    // (THROTTLE_LIMIT=0 disables rate limiting entirely).
    ...(isThrottlingEnabled()
      ? [{ provide: APP_GUARD, useClass: CfThrottlerGuard }]
      : []),
  ],
})
export class AppModule {}
