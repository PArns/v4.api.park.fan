import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { TripsController } from "./trips.controller";
import { TripsService } from "./trips.service";
import { TripWriteRateLimitService } from "./trip-write-rate-limit.service";
import { Trip } from "./entities/trip.entity";
import { PushSubscription } from "../push/entities/push-subscription.entity";
import { RedisModule } from "../common/redis/redis.module";

/**
 * Stored plans — see `TripsController` for why this module owns its own rate
 * limiter instead of using the global throttler.
 *
 * `PushSubscription` is registered here rather than reached through
 * `PushModule`, which imports THIS module for `TripsService` and would make the
 * pair circular. Deleting a trip has to clear the `tripId` pointing at it in the
 * same transaction (see `TripsService.remove`), so the repository has to be
 * available on this side.
 */
@Module({
  imports: [TypeOrmModule.forFeature([Trip, PushSubscription]), RedisModule],
  controllers: [TripsController],
  providers: [TripsService, TripWriteRateLimitService],
  exports: [TripsService],
})
export class TripsModule {}
