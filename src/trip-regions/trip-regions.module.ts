import { Module } from "@nestjs/common";
import { ParksModule } from "../parks/parks.module";
import { ParkSeasonsModule } from "../parks/park-seasons.module";
import { MLModule } from "../ml/ml.module";
import { TripRegionsController } from "./trip-regions.controller";
import { TripRegionService } from "./trip-region.service";
import { TripPlanService } from "./trip-plan.service";

/**
 * Trip regions (`/v1/trip-regions/*`): the curated Orlando region, its
 * calendar and the trip optimizer. The calendar warmup that keeps the region
 * parks' months warm lives in `CacheWarmupService`, which reads `regions.ts`
 * directly rather than importing this module.
 */
@Module({
  imports: [ParksModule, ParkSeasonsModule, MLModule],
  controllers: [TripRegionsController],
  providers: [TripRegionService, TripPlanService],
  exports: [TripRegionService],
})
export class TripRegionsModule {}
