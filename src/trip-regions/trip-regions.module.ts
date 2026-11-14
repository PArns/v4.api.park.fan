import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { TicketProductRecord } from "./entities/ticket-product.entity";
import { TicketCatalogService } from "./ticket-catalog.service";
import { ParksModule } from "../parks/parks.module";
import { ParkSeasonsModule } from "../parks/park-seasons.module";
import { MLModule } from "../ml/ml.module";
import { HolidaysModule } from "../holidays/holidays.module";
import { TripRegionsController } from "./trip-regions.controller";
import { TripRegionService } from "./trip-region.service";
import { TripPlanService } from "./trip-plan.service";
import { TripWhenService } from "./trip-when.service";
import { TripAccuracyService } from "./trip-accuracy.service";
import { TripMetricsModule } from "./metrics/trip-metrics.module";

/**
 * Trip regions (`/v1/trip-regions/*`): the curated regions (`regions.ts`),
 * their calendars and the trip optimizer. The calendar warmup that keeps the region
 * parks' months warm lives in `CacheWarmupService`, which reads `regions.ts`
 * directly rather than importing this module. `/plan`, `/when` and
 * `/best-days` record each answer's operational facts (`metrics/`).
 */
@Module({
  imports: [
    ParksModule,
    ParkSeasonsModule,
    MLModule,
    HolidaysModule,
    TypeOrmModule.forFeature([TicketProductRecord]),
    TripMetricsModule,
  ],
  controllers: [TripRegionsController],
  providers: [
    TripRegionService,
    TripPlanService,
    TripWhenService,
    TripAccuracyService,
    TicketCatalogService,
  ],
  exports: [TripRegionService, TicketCatalogService],
})
export class TripRegionsModule {}
