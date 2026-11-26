import { Module } from "@nestjs/common";
import { RedisModule } from "../common/redis/redis.module";
import { ParksModule } from "../parks/parks.module";
import { AnalyticsModule } from "../analytics/analytics.module";
import { RankingsController } from "./rankings.controller";
import { RankingsService } from "./rankings.service";

/** Year rankings of rides across parks (`/v1/rankings`). */
@Module({
  imports: [RedisModule, ParksModule, AnalyticsModule],
  controllers: [RankingsController],
  providers: [RankingsService],
})
export class RankingsModule {}
