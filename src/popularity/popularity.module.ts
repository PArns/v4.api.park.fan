import { Module } from "@nestjs/common";
import { PopularityService } from "./popularity.service";
import { RedisModule } from "../common/redis/redis.module";

@Module({
  imports: [RedisModule],
  providers: [PopularityService],
  exports: [PopularityService],
})
export class PopularityModule {}
