import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { TripsController } from "./trips.controller";
import { TripsService } from "./trips.service";
import { TripWriteRateLimitService } from "./trip-write-rate-limit.service";
import { Trip } from "./entities/trip.entity";
import { RedisModule } from "../common/redis/redis.module";

/**
 * Stored plans — see `TripsController` for why this module owns its own rate
 * limiter instead of using the global throttler.
 */
@Module({
  imports: [TypeOrmModule.forFeature([Trip]), RedisModule],
  controllers: [TripsController],
  providers: [TripsService, TripWriteRateLimitService],
  exports: [TripsService],
})
export class TripsModule {}
