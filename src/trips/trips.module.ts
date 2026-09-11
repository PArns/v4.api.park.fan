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
 *
 * `PushSubscription` is deliberately NOT registered here, though
 * `TripsService` writes to it. `forFeature` provides a repository to inject,
 * and nothing here injects one: the deletes go through the shared
 * `EntityManager`, which resolves `PushSubscription` from the connection's
 * entity glob (`config/typeorm.config.ts`). Importing `PushModule` for it is
 * not an option either — that module imports THIS one for `TripsService`.
 */
@Module({
  imports: [TypeOrmModule.forFeature([Trip]), RedisModule],
  controllers: [TripsController],
  providers: [TripsService, TripWriteRateLimitService],
  exports: [TripsService],
})
export class TripsModule {}
