import { Module, forwardRef } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { QueueData } from "./entities/queue-data.entity";
import { ForecastData } from "./entities/forecast-data.entity";
import { QueueDataService } from "./queue-data.service";
import { QueueDataController } from "./queue-data.controller";
import { AttractionsModule } from "../attractions/attractions.module";
import { ParksModule } from "../parks/parks.module";
import { Attraction } from "../attractions/entities/attraction.entity";

/**
 * Queue Data Module
 *
 * Handles wait times and queue data for all 6 queue types:
 * - STANDBY
 * - SINGLE_RIDER
 * - RETURN_TIME
 * - PAID_RETURN_TIME
 * - BOARDING_GROUP
 * - PAID_STANDBY
 *
 * Features:
 * - Delta-based storage (only save when data changes significantly)
 * - Supports all queue types from ThemeParks.wiki
 * - Time-series optimized for analytics
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([QueueData, ForecastData, Attraction]),
    forwardRef(() => AttractionsModule),
    forwardRef(() => ParksModule),
  ],
  controllers: [QueueDataController],
  providers: [QueueDataService],
  exports: [QueueDataService],
})
export class QueueDataModule {}
