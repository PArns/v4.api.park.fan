import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { DiscoveryController } from "./discovery.controller";
import { DiscoveryService } from "./discovery.service";
import { Park } from "../parks/entities/park.entity";
import { ParkDailyStats } from "../stats/entities/park-daily-stats.entity";
import { RedisModule } from "../common/redis/redis.module";
import { ParksModule } from "../parks/parks.module";
import { AnalyticsModule } from "../analytics/analytics.module";

/**
 * Discovery Module
 *
 * Provides geographic structure discovery for route generation
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([Park, ParkDailyStats]),
    RedisModule,
    ParksModule,
    AnalyticsModule,
  ],
  controllers: [DiscoveryController],
  providers: [DiscoveryService],
  exports: [DiscoveryService],
})
export class DiscoveryModule {}
