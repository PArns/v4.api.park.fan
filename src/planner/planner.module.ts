import { Module } from "@nestjs/common";
import { RedisModule } from "../common/redis/redis.module";
import { ParksModule } from "../parks/parks.module";
import { PlanOptimizeController } from "./plan-optimize.controller";
import { PlanOptimizeService } from "./plan-optimize.service";

/**
 * The single-day trip planner's server half: the day optimizer park.fan's planner and Parky both
 * call. The engine itself (`day-optimizer/`) is plain TypeScript with no Nest in it, so it runs in
 * worker threads and in specs alike.
 */
@Module({
  imports: [ParksModule, RedisModule],
  controllers: [PlanOptimizeController],
  providers: [PlanOptimizeService],
})
export class PlannerModule {}
