import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { LocationController } from "./location.controller";
import { LocationService } from "./location.service";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { AnalyticsModule } from "../analytics/analytics.module";
import { ParksModule } from "../parks/parks.module";
import { QueueDataModule } from "../queue-data/queue-data.module";

/**
 * Location Module
 *
 * Provides location-based discovery functionality for parks and attractions.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([Park, Attraction]),
    AnalyticsModule,
    ParksModule,
    QueueDataModule,
  ],
  controllers: [LocationController],
  providers: [LocationService],
  exports: [LocationService],
})
export class LocationModule {}
